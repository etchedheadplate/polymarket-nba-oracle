from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

from sqlalchemy import Row, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import NBAGamesModel, NBAMarketsModel
from src.service.domain.games import GameStatus


class NBAGamesRepo:
    async def get_latest_ended_game_date(self, session: AsyncSession) -> date | None:
        stmt = select(func.max(NBAGamesModel.game_date)).where(NBAGamesModel.game_status != GameStatus.NOT_STARTED)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_earliest_live_game_date(self, session: AsyncSession) -> date | None:
        stmt = select(func.min(NBAGamesModel.game_date)).where(NBAGamesModel.game_status == GameStatus.LIVE)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_future_games(self, session: AsyncSession, end_date: date | None) -> Sequence[Any]:
        today = datetime.now().date()
        stmt = (
            select(NBAGamesModel.id, NBAGamesModel.game_date, NBAGamesModel.guest_team, NBAGamesModel.host_team)
            .where(NBAGamesModel.game_status == GameStatus.NOT_STARTED)
            .where(NBAGamesModel.game_date >= today)
        )

        if end_date:
            stmt = stmt.where(NBAGamesModel.game_date <= end_date)

        result = await session.execute(stmt)
        return result.all()

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
