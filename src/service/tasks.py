from datetime import UTC, date, datetime, timedelta
from typing import Any

from src.database.connection import async_session_maker
from src.service.domain import ScheduleRange
from src.service.repos import NBAGamesRepo, NBAMarketsRepo


async def construct_game_dates() -> tuple[date | None, date | None]:
    async with async_session_maker() as session:
        last_ended = await NBAGamesRepo().get_latest_ended_game_date(session)
        first_live = await NBAGamesRepo().get_earliest_live_game_date(session)
        now = datetime.now(tz=UTC).date()

    if last_ended or first_live:
        start = min(d for d in (last_ended, first_live, now) if d is not None)
        end = now + timedelta(weeks=2)  # future dates for announced games
        return start, end

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


async def construct_game_schedule(schedule_range: ScheduleRange) -> dict[str, Any]:
    today = date.today()
    end_date = None

    if schedule_range == ScheduleRange.TODAY:
        end_date = today
    elif schedule_range == ScheduleRange.WEEK:
        end_date = today + timedelta(weeks=1)

    async with async_session_maker() as session:
        future_games = await NBAGamesRepo().get_future_games(session, end_date)

    games_dict: dict[str, Any] = {}
    for game_id, game_date, guest_team, host_team in future_games:
        games_dict[game_id] = {"date": game_date.isoformat(), "guest": guest_team, "host": host_team}

    return games_dict
