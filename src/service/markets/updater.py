import asyncio

from src.core.loading import DataFrameLoader
from src.core.updating import BaseDataUpdater
from src.database.connection import async_session_maker
from src.database.models import NBAMarketsModel
from src.database.repos import NBAGamesRepo
from src.logger import logger
from src.service.markets.clients import NBAMarketsClient
from src.service.markets.parsers import NBAMarketsParser


async def get_event_ids(repo: NBAGamesRepo) -> list[int]:
    async with async_session_maker() as session:
        events = await repo.get_event_ids_without_markets(session)
        logger.info("Found '%s' events without markets", len(events))
    return events


async def update():
    events = await get_event_ids(NBAGamesRepo())
    if len(events) > 0:
        for event in events:
            markets = BaseDataUpdater(
                client=NBAMarketsClient(event_id=event),
                parser=NBAMarketsParser(event_id=event),
                loader_cls=DataFrameLoader,
                model_cls=NBAMarketsModel,
            )
            await markets.update()


if __name__ == "__main__":
    asyncio.run(update())
