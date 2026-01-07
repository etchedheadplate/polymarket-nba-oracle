import pandas as pd
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import BaseModel


class BaseNBADataLoader:
    def __init__(self, alchemy_model: type[BaseModel], session: AsyncSession) -> None:
        self.model = alchemy_model
        self.session = session

    async def load_df_to_db(self, df: pd.DataFrame) -> None:
        df = df.convert_dtypes()  # convert data to optimal nullable Pandas types
        df = df.where(pd.notna(df), None)  # replace nullable Pandas types with Python None
        records = df.to_dict(orient="records")
        print(len(records))

        if not records:
            return

        stmt = insert(self.model).values(records)
        await self.session.execute(stmt)
