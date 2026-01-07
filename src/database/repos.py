from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import NBAMarketGameModel


class NBAMarketGameRepo:
    async def get_latest_game_start_date(self, session: AsyncSession) -> datetime | None:
        stmt = select(func.max(NBAMarketGameModel.game_start_date))
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
