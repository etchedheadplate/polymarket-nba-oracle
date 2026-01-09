from abc import ABC, abstractmethod
from typing import Any, cast

import pandas as pd
from sqlalchemy import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import BaseModel


class BaseLoader(ABC):
    def __init__(self) -> None:
        self.rowcount: int = 0

    @abstractmethod
    async def load(self, data: Any) -> None:
        pass


class SQLAlchemyLoader(BaseLoader):
    def __init__(self, alchemy_model: type[BaseModel], session: AsyncSession) -> None:
        super().__init__()
        self.model = alchemy_model
        self.session = session

    @abstractmethod
    async def load(self, data: Any) -> None:
        pass


class DataFrameLoader(SQLAlchemyLoader):
    async def load(self, data: pd.DataFrame) -> None:
        df = data
        df = df.convert_dtypes()  # convert data to optimal nullable Pandas types
        df = df.where(pd.notna(df), None)  # replace nullable Pandas types with Python None
        records = df.to_dict(orient="records")

        if not records:
            return

        stmt = insert(self.model).values(records)
        result = await self.session.execute(stmt)
        cursor_result = cast(CursorResult[Any], result)
        self.rowcount = cursor_result.rowcount or 0
