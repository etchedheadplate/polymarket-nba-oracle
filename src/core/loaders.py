import asyncio
from abc import ABC, abstractmethod
from itertools import islice
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.conflicts import ConflictStrategy
from src.core.parser import BaseJsonSchema
from src.database.models import BaseModel
from src.logger import logger


class BaseLoader(ABC):
    def __init__(
        self, session: AsyncSession, alchemy_model: type[BaseModel], conflict_strategy: ConflictStrategy | None = None
    ) -> None:
        self._rowcount: int = 0
        self._model = alchemy_model
        self._session = session
        self._conflict_strategy = conflict_strategy

    async def _insert_batch(self, batch: list[dict[str, Any]]) -> int:
        stmt = insert(self._model).values(batch)
        if self._conflict_strategy:
            stmt = self._conflict_strategy.apply(stmt)
        result = await self._session.execute(stmt)
        return result.rowcount or 0  # type: ignore

    @staticmethod
    def _chunk(iterable: list[dict[str, Any]], size: int):
        it = iter(iterable)
        while chunk := list(islice(it, size)):
            yield chunk

    @abstractmethod
    async def load(self, data: Any) -> None: ...


class PydanticLoader(BaseLoader):
    def __init__(
        self,
        session: AsyncSession,
        alchemy_model: type[BaseModel],
        batch_size: int,
        conflict_strategy: ConflictStrategy | None = None,
        max_concurrent_batches: int = 5,
    ):
        super().__init__(session, alchemy_model, conflict_strategy)
        self._max_concurrent_batches = max_concurrent_batches
        self._batch_size = batch_size

    async def load(self, data: list[BaseJsonSchema]) -> None:
        if not data:
            logger.info("%s: no data to load", self._model.__tablename__)
            return

        records = [item.model_dump() for item in data]
        batches = list(self._chunk(records, self._batch_size))

        semaphore = asyncio.Semaphore(self._max_concurrent_batches)

        async def worker(batch: list[dict[str, Any]]) -> int:
            async with semaphore:
                return await self._insert_batch(batch)

        try:
            results = await asyncio.gather(*(worker(batch) for batch in batches))
            await self._session.commit()
            self._rowcount = sum(results)
        except Exception:
            await self._session.rollback()
            logger.error("Failed to commit data to '%s'", self._model.__tablename__)
            raise

        logger.info("%s: %s rows inserted", self._model.__tablename__, self._rowcount)
