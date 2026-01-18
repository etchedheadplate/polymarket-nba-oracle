import aiohttp

from src.core.conflicts import DoNothingOnConflict, UpdateNonNullFields
from src.core.load import PydanticLoader
from src.database.connection import async_session_maker
from src.database.models import NBAGamesModel, NBAMarketsModel, NBAPricesModel
from src.service.dumping.clients import NBAGamesClient, NBAMarketsClient, NBAPricesClient
from src.service.loading.tasks import construct_game_dates, construct_market_endpoints, construct_prices_payload
from src.service.parsing.parsers import NBAGamesParser, NBAMarketsParser, NBAPricesParser


async def update_games():
    strategy = UpdateNonNullFields(
        index_elements=["event_slug"], fields_to_update=["game_status", "guest_score", "host_score"]
    )

    slug_flags = (True, False)
    for flag in slug_flags:
        async with aiohttp.ClientSession() as session:
            client = NBAGamesClient(session=session, by_slug=flag)
            files = await client.dump()

        start, end = await construct_game_dates()
        parser = NBAGamesParser(start_date=start, end_date=end, by_slug=flag)
        parser.ingest(json_files=files)
        items = await parser.parse()

        async with async_session_maker() as session:
            loader = PydanticLoader(
                session=session, alchemy_model=NBAGamesModel, batch_size=500, conflict_strategy=strategy
            )
            await loader.load(data=items)


async def update_markets():
    strategy = DoNothingOnConflict(index_elements=["id"])

    endpoints = await construct_market_endpoints()
    if not endpoints:
        return

    async with aiohttp.ClientSession() as session:
        client = NBAMarketsClient(session=session, endpoints=endpoints)
        files = await client.dump()

    parser = NBAMarketsParser()
    parser.ingest(json_files=files)
    items = await parser.parse()

    async with async_session_maker() as session:
        loader = PydanticLoader(
            session=session, alchemy_model=NBAMarketsModel, batch_size=1000, conflict_strategy=strategy
        )
        await loader.load(data=items)


async def update_prices():
    """
    Market is represented by teams token id, so it is mandatory
    to make separate calls for guest and host team token prices
    """
    strategy = UpdateNonNullFields(
        index_elements=["market_id", "timestamp"], fields_to_update=["price_guest", "price_host"]
    )

    payload_guest, payload_host, token_market_map = await construct_prices_payload()

    price_config = ((True, payload_guest), (False, payload_host))
    for config in price_config:
        flag, params = config

        async with aiohttp.ClientSession() as session:
            client = NBAPricesClient(session=session, params=params)
            files = await client.dump()

        parser = NBAPricesParser(token_market_map=token_market_map, is_guest=flag)
        parser.ingest(json_files=files)
        items = await parser.parse()

        async with async_session_maker() as session:
            loader = PydanticLoader(
                session=session, alchemy_model=NBAPricesModel, batch_size=3000, conflict_strategy=strategy
            )
            await loader.load(data=items)
