import asyncio
from abc import ABC, abstractmethod
from collections.abc import Sequence
from itertools import islice
from typing import Any

from sqlalchemy import case
from sqlalchemy.dialects.postgresql import (
    Insert as PostgresInsert,
    insert,
)
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.processing import BaseJSONSchema
from src.database.models import BaseModel
from src.logger import logger


def chunked(iterable: list[dict[str, Any]], size: int):
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


class BaseLoader(ABC):
    def __init__(self) -> None:
        self.rowcount: int = 0

    @abstractmethod
    async def load(self, data: Any) -> None:
        pass


class SQLAlchemyLoader(BaseLoader):
    def __init__(self, session: AsyncSession, alchemy_model: type[BaseModel]) -> None:
        super().__init__()
        self.model = alchemy_model
        self.session = session

    @abstractmethod
    async def load(self, data: Any) -> None:
        pass


class ConflictStrategy(ABC):
    @abstractmethod
    def apply(self, stmt: PostgresInsert) -> PostgresInsert: ...


class DoNothingOnConflict(ConflictStrategy):
    def __init__(self, index_elements: Sequence[str]):
        self.index_elements = index_elements

    def apply(self, stmt: PostgresInsert) -> PostgresInsert:
        return stmt.on_conflict_do_nothing(index_elements=self.index_elements)


class UpdateNonNullFields(ConflictStrategy):
    def __init__(self, index_elements: Sequence[str], fields_to_update: Sequence[str]):
        self.index_elements = index_elements
        self.fields_to_update = fields_to_update

    def apply(self, stmt: PostgresInsert) -> PostgresInsert:

        update_dict = {
            field: case((stmt.excluded[field].isnot(None), stmt.excluded[field]), else_=getattr(stmt.table.c, field))
            for field in self.fields_to_update
        }
        return stmt.on_conflict_do_update(index_elements=self.index_elements, set_=update_dict)


class PydanticLoader:
    def __init__(
        self,
        session: AsyncSession,
        alchemy_model: type[BaseModel],
        conflict_strategy: ConflictStrategy | None = None,
        max_concurrent_batches: int = 5,
    ):
        self.session = session
        self.model = alchemy_model
        self.conflict_strategy = conflict_strategy
        self.max_concurrent_batches = max_concurrent_batches
        self.rowcount: int = 0

    async def _insert_batch(self, batch: list[dict[str, Any]]) -> int:
        stmt = insert(self.model).values(batch)
        if self.conflict_strategy:
            stmt = self.conflict_strategy.apply(stmt)
        result = await self.session.execute(stmt)
        return result.rowcount or 0  # type: ignore

    async def load(self, data: list[BaseJSONSchema]) -> None:
        if not data:
            logger.info("No data to load to '%s'", self.model.__tablename__)
            return

        records = [item.model_dump() for item in data]
        columns_count = len(records[0])
        max_params = 32767
        batch_size = max(1, max_params // columns_count)
        batches = list(chunked(records, batch_size))
        logger.info(
            "Loader %s received %s items, using batch size %s (%s batches)",
            self.__class__.__name__,
            len(records),
            batch_size,
            len(batches),
        )

        semaphore = asyncio.Semaphore(self.max_concurrent_batches)

        async def worker(batch: list[dict[str, Any]]):
            async with semaphore:
                return await self._insert_batch(batch)

        results = await asyncio.gather(*(worker(batch) for batch in batches))
        self.rowcount = sum(results)
        await self.session.commit()
        logger.info(
            "Loader %s inserted %s rows to '%s'", self.__class__.__name__, self.rowcount, self.model.__tablename__
        )
