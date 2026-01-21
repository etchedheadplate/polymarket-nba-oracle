from datetime import UTC, date, datetime, timedelta
from typing import Any

from src.database.connection import async_session_maker
from src.service.repos import NBAGamesRepo, NBAMarketsRepo


async def construct_game_dates() -> tuple[date | None, date | None]:
    async with async_session_maker() as session:
        latest = await NBAGamesRepo().get_latest_game_date(session)

    if latest:
        now = datetime.now(tz=UTC).date()
        latest = latest if latest < now else now
        return latest, now + timedelta(weeks=2)  # future dates for announced games

    return None, None


async def construct_market_endpoints() -> list[str]:
    async with async_session_maker() as session:
        events_without_markets = await NBAGamesRepo().get_event_ids_without_markets(session)
        events_with_open_markets = await NBAGamesRepo().get_event_ids_with_open_markets(session)
    events = set(events_without_markets) | set(events_with_open_markets)
    endpoints = [str(event) for event in events] if events else []
    return endpoints


async def construct_prices_payload() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[int, int]]:
    async with async_session_maker() as session:
        markets = await NBAMarketsRepo().get_markets_without_prices(session)

    payload_guest: list[dict[str, Any]] = []
    payload_host: list[dict[str, Any]] = []
    token_market_map: dict[int, int] = {}

    for market_id, start, end, guest_token, host_token in markets:
        if not end:
            continue

        params_guest = {"market": guest_token, "startTs": int(start), "endTs": int(end)}
        params_host = {"market": host_token, "startTs": int(start), "endTs": int(end)}

        payload_guest.append(params_guest)
        payload_host.append(params_host)

        token_market_map[int(guest_token)] = market_id
        token_market_map[int(host_token)] = market_id

    return payload_guest, payload_host, token_market_map
