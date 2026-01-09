from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import NBAGamesModel


class NBAGamesRepo:
    async def get_market_id(self, session: AsyncSession, event_id: int) -> int | None:
        stmt = select(NBAGamesModel.id).where(NBAGamesModel.event_id == event_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_latest_game_date(self, session: AsyncSession) -> date | None:
        stmt = select(func.max(NBAGamesModel.game_date))
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_event_ids_without_markets(self, session: AsyncSession) -> list[int]:
        """
        TODO: make query which checks if row in NBAGamesModel contains NBAGamesModel.markets
        Return List[NBAGamesModel.event_id] from rows where NBAGamesModel.markets is empty
        """
        events = []
        return events
