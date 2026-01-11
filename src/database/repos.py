from collections.abc import Sequence
from datetime import date, datetime

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import NBAGamesModel, NBAMarketsModel


class NBAGamesRepo:
    async def get_latest_game_date(self, session: AsyncSession) -> date | None:
        stmt = select(func.max(NBAGamesModel.game_date))
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_event_ids_without_markets(self, session: AsyncSession) -> Sequence[int]:
        stmt = select(NBAGamesModel.event_id).where(~exists().where(NBAMarketsModel.event_id == NBAGamesModel.id))
        result = await session.execute(stmt)
        return result.scalars().all()


class NBAMarketsRepo:
    async def get_market_ids_without_prices(self, session: AsyncSession) -> Sequence[int]:
        stmt = select(NBAMarketsModel.id).where(~NBAMarketsModel.prices.any())
        result = await session.execute(stmt)
        return result.scalars().all()

    async def get_ended_market_time_window(self, session: AsyncSession, market_id: int) -> tuple[datetime, datetime]:
        stmt = select(
            NBAMarketsModel.market_start,
            NBAMarketsModel.market_end,
        ).where(
            NBAMarketsModel.market_id == market_id,
            NBAMarketsModel.market_start.is_not(None),
            NBAMarketsModel.market_end.is_not(None),
        )

        result = await session.execute(stmt)
        row = result.one_or_none()

        if row is None:
            raise ValueError(f"Market {market_id} not found or has incomplete time window")

        market_start, market_end = row
        return market_start, market_end

    async def get_market_tokens(self, session: AsyncSession, market_id: int) -> tuple[str, str]:
        stmt = select(
            NBAMarketsModel.token_id_guest,
            NBAMarketsModel.token_id_host,
        ).where(NBAMarketsModel.market_id == market_id)

        result = await session.execute(stmt)
        row = result.one_or_none()

        if row is None:
            raise ValueError(f"Market {market_id} not found")

        token_id_guest, token_id_host = row
        return token_id_guest, token_id_host
