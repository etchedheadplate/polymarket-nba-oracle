import asyncio
from datetime import UTC, date, datetime, timedelta

from src.core.loading import DataFrameLoader
from src.core.updating import BaseDataUpdater
from src.database.connection import async_session_maker
from src.database.models import NBAGamesModel
from src.database.repos import NBAGamesRepo
from src.service.games.clients import NBAGamesBySeriesIdClient, NBAGamesBySlugClient
from src.service.games.parsers import NBAGamesBySeriesIdParser, NBAGamesBySlugParser


async def get_dates(repo: NBAGamesRepo) -> tuple[date, date]:
    async with async_session_maker() as session:
        latest = await repo.get_latest_game_date(session)
        if latest is None:
            dates = (  # dates of archive 2024/2025 NBA season games
                date(year=2024, month=10, day=21),
                date(year=2025, month=7, day=1),
            )
        else:
            dates = (latest, datetime.now(tz=UTC).date() + timedelta(weeks=2))  # future games for announced games
    return dates


async def update_games():
    """
    Older games parsed by slug should be updated first to
    prevent overlapping with newer games parsed by series
    """
    start_date, end_date = await get_dates(NBAGamesRepo())

    games_by_slug = BaseDataUpdater(
        client=NBAGamesBySlugClient(),
        parser=NBAGamesBySlugParser(start_date, end_date),
        loader_cls=DataFrameLoader,
        model_cls=NBAGamesModel,
    )
    await games_by_slug.update()

    start_date, end_date = await get_dates(NBAGamesRepo())

    games_by_series = BaseDataUpdater(
        client=NBAGamesBySeriesIdClient(),
        parser=NBAGamesBySeriesIdParser(start_date, end_date),
        loader_cls=DataFrameLoader,
        model_cls=NBAGamesModel,
    )
    await games_by_series.update()


if __name__ == "__main__":
    asyncio.run(update_games())
