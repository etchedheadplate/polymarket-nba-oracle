from src.core.conflicts import UpdateNonNullFields
from src.core.updater import BaseUpdater
from src.database.connection import async_session_maker
from src.database.models import NBAMarketsModel
from src.service.etl.markets.client import NBAMarketsClient
from src.service.etl.markets.parser import NBAMarketsParser
from src.service.repos import NBAGamesRepo


class MarketsUpdater(BaseUpdater):
    _client_cls = NBAMarketsClient
    _parser_cls = NBAMarketsParser
    _alchemy_model = NBAMarketsModel
    _conflict_strategy = UpdateNonNullFields(
        index_elements=["event_id", "market_question"], fields_to_update=["market_end"]
    )


async def construct_market_endpoints() -> list[str]:
    async with async_session_maker() as session:
        events_without_markets = await NBAGamesRepo().get_event_ids_without_markets(session)
        events_with_open_markets = await NBAGamesRepo().get_event_ids_with_open_markets(session)
    events = set(events_without_markets) | set(events_with_open_markets)
    endpoints = [str(event) for event in events] if events else []
    return endpoints


async def update_markets() -> int:
    endpoints = await construct_market_endpoints()
    rowcount = await MarketsUpdater().run(client_kwargs={"endpoints": endpoints}, parser_kwargs=None)
    return rowcount


if __name__ == "__main__":
    import asyncio

    asyncio.run(update_markets())
