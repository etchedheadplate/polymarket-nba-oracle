from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

import aiohttp
from sqlalchemy import Row

from src.core.loading import DoNothingOnConflict, PydanticLoader, UpdateNonNullFields
from src.database.connection import async_session_maker
from src.database.models import NBAGamesModel, NBAMarketsModel, NBAPricesModel
from src.logger import logger
from src.service.clients import NBAGamesClient, NBAMarketsClient, NBAPricesClient
from src.service.parsers import NBAGamesParser, NBAMarketsParser, NBAPricesParser
from src.service.repos import NBAGamesRepo, NBAMarketsRepo


async def get_dates(repo: NBAGamesRepo) -> tuple[date | None, date | None]:
    async with async_session_maker() as session:
        latest = await repo.get_latest_game_date(session)
        if latest is None:
            dates = (None, None)  # dates of archive 2024/2025 NBA season games
        else:
            dates = (
                latest + timedelta(days=1),
                datetime.now(tz=UTC).date() + timedelta(weeks=2),
            )  # future dates for announced games
    return dates


async def get_event_ids(repo: NBAGamesRepo) -> Sequence[int]:
    async with async_session_maker() as session:
        events = await repo.get_event_ids_without_markets(session)
        logger.info("Found %s events without markets", len(events))
    return events


async def prepare_prices_data(repo: NBAMarketsRepo) -> Sequence[Row[tuple[int, int, int, str, str]]]:
    async with async_session_maker() as session:
        return await repo.get_markets_without_prices(session)


async def update_games():
    strategy = DoNothingOnConflict(index_elements=["id"])

    slug_flags = (True, False)
    for flag in slug_flags:
        async with aiohttp.ClientSession() as session:
            client = NBAGamesClient(session=session, by_slug=flag)
            files = await client.dump()

        start, end = await get_dates(repo=NBAGamesRepo())
        parser = NBAGamesParser(start_date=start, end_date=end, by_slug=flag)
        parser.load(json_files=files)
        items = await parser.parse()

        async with async_session_maker() as session:
            loader = PydanticLoader(session=session, alchemy_model=NBAGamesModel, conflict_strategy=strategy)
            await loader.load(data=items)


async def update_markets():
    strategy = DoNothingOnConflict(index_elements=["id"])

    events = await get_event_ids(NBAGamesRepo())
    if not events:
        return

    endpoints = [str(event) for event in events]

    async with aiohttp.ClientSession() as session:
        client = NBAMarketsClient(session=session, endpoints=endpoints)
        files = await client.dump()

    parser = NBAMarketsParser()
    parser.load(json_files=files)
    items = await parser.parse()

    async with async_session_maker() as session:
        loader = PydanticLoader(session=session, alchemy_model=NBAMarketsModel, conflict_strategy=strategy)
        await loader.load(data=items)


async def update_prices():
    """
    Market is represented by teams token id, so it is mandatory
    to make separate calls for guest and host team token prices
    """
    strategy = UpdateNonNullFields(
        index_elements=["market_id", "timestamp"], fields_to_update=["price_guest", "price_host"]
    )

    markets_repo = NBAMarketsRepo()
    markets = await prepare_prices_data(markets_repo)

    token_market_map: dict[int, int] = {}
    payload_guest: list[dict[str, Any]] = []
    payload_host: list[dict[str, Any]] = []

    for market_id, start, end, guest, host in markets:
        if not end:
            continue

        params_guest = {"market": guest, "startTs": int(start), "endTs": int(end)}
        params_host = {"market": host, "startTs": int(start), "endTs": int(end)}

        payload_guest.append(params_guest)
        payload_host.append(params_host)

        token_market_map[int(guest)] = market_id
        token_market_map[int(host)] = market_id

    guest_config = ((True, payload_guest), (False, payload_host))
    for config in guest_config:
        flag, params = config

        async with aiohttp.ClientSession() as session:
            client = NBAPricesClient(session=session, params=params)
            files = await client.dump()

        parser = NBAPricesParser(token_market_map=token_market_map, is_guest=flag)
        parser.load(json_files=files)
        items = await parser.parse()

        async with async_session_maker() as session:
            loader = PydanticLoader(session=session, alchemy_model=NBAPricesModel, conflict_strategy=strategy)
            await loader.load(data=items)
