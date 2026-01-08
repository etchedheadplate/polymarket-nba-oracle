from typing import Any, cast

import pandas as pd
from sqlalchemy import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import BaseModel


class BaseLoader:
    def __init__(self, alchemy_model: type[BaseModel], session: AsyncSession) -> None:
        self.model = alchemy_model
        self.session = session
        self.rowcount = 0

    async def load_df_to_db(self, df: pd.DataFrame) -> None:
        df = df.convert_dtypes()  # convert data to optimal nullable Pandas types
        df = df.where(pd.notna(df), None)  # replace nullable Pandas types with Python None
        records = df.to_dict(orient="records")

        if not records:
            return

        stmt = insert(self.model).values(records)
        result = await self.session.execute(stmt)
        cursor_result = cast(CursorResult[Any], result)
        self.rowcount = cursor_result.rowcount or 0
