from collections.abc import Sequence
from datetime import date, datetime

from sqlalchemy import Row, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import NBAGamesModel, NBAMarketsModel


class NBAGamesRepo:
    async def get_latest_game_date(self, session: AsyncSession) -> date | None:
        stmt = select(func.max(NBAGamesModel.game_date))
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_event_ids_without_markets(self, session: AsyncSession) -> Sequence[int]:
        today = datetime.now().date()
        stmt = (
            select(NBAGamesModel.id)
            .where(~exists().where(NBAMarketsModel.event_id == NBAGamesModel.id))
            .where(NBAGamesModel.game_date <= today)
        )
        result = await session.execute(stmt)
        return result.scalars().all()


class NBAMarketsRepo:
    async def get_markets_without_prices(self, session: AsyncSession) -> Sequence[Row[tuple[int, int, int, str, str]]]:
        from sqlalchemy import func

        stmt = select(
            NBAMarketsModel.id,
            func.extract("epoch", NBAMarketsModel.market_start).label("market_start_ts"),
            func.extract("epoch", NBAMarketsModel.market_end).label("market_end_ts"),
            NBAMarketsModel.token_id_guest,
            NBAMarketsModel.token_id_host,
        ).where(~NBAMarketsModel.prices.any())

        result = await session.execute(stmt)
        return result.all()
