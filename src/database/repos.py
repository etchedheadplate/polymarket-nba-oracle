from collections.abc import Sequence
from datetime import date

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import NBAGamesModel, NBAMarketsModel


class NBAGamesRepo:
    async def get_market_id(self, session: AsyncSession, event_id: int) -> int | None:
        stmt = select(NBAGamesModel.id).where(NBAGamesModel.event_id == event_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_latest_game_date(self, session: AsyncSession) -> date | None:
        stmt = select(func.max(NBAGamesModel.game_date))
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_event_ids_without_markets(self, session: AsyncSession) -> Sequence[int]:
        stmt = select(NBAGamesModel.event_id).where(~exists().where(NBAMarketsModel.event_id == NBAGamesModel.id))

        result = await session.execute(stmt)
        return result.scalars().all()
