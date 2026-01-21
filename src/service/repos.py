from collections.abc import Sequence
from datetime import date, datetime

from sqlalchemy import Row, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import NBAGamesModel, NBAMarketsModel
from src.service.domain import GameStatus


class NBAGamesRepo:
    async def get_latest_game_date(self, session: AsyncSession) -> date | None:
        not_started_status = GameStatus.NOT_STARTED
        stmt = select(func.max(NBAGamesModel.game_date)).where(NBAGamesModel.game_status != not_started_status)
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

    async def get_event_ids_with_open_markets(self, session: AsyncSession) -> Sequence[int]:
        today = datetime.now().date()
        stmt = (
            select(NBAGamesModel.id)
            .where(
                exists().where((NBAMarketsModel.event_id == NBAGamesModel.id) & (NBAMarketsModel.market_end.is_(None)))
            )
            .where(NBAGamesModel.game_date <= today)
        )

        result = await session.execute(stmt)
        return result.scalars().all()


class NBAMarketsRepo:
    async def get_markets_without_prices(self, session: AsyncSession) -> Sequence[Row[tuple[int, int, int, str, str]]]:
        stmt = select(
            NBAMarketsModel.id,
            func.extract("epoch", NBAMarketsModel.market_start).label("market_start_ts"),
            func.extract("epoch", NBAMarketsModel.market_end).label("market_end_ts"),
            NBAMarketsModel.token_id_guest,
            NBAMarketsModel.token_id_host,
        ).where(~NBAMarketsModel.prices.any())

        result = await session.execute(stmt)
        return result.all()
