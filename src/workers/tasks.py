from datetime import date, datetime, timedelta
from typing import Any

from src.database.connection import async_session_maker
from src.service.domain.games import ScheduleRange
from src.service.etl.update import update_database
from src.service.repos import NBAGamesRepo, NBAMarketsRepo


async def construct_game_schedule(payload: dict[str, Any]) -> dict[str, Any]:
    schedule_range = ScheduleRange(payload["schedule"])
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
        date_key = game_date.isoformat()
        if date_key not in games_dict:
            games_dict[date_key] = {}
        games_dict[date_key][game_id] = {"guest": guest_team, "host": host_team}

    return games_dict


async def run_database_update() -> dict[str, Any]:
    async with async_session_maker() as session:
        future_games = await NBAGamesRepo().get_future_games(session)
        markets_before_update = await NBAMarketsRepo().get_markets_count(session)

    start_ts = datetime.timestamp(datetime.now())
    stats = await update_database()
    end_ts = datetime.timestamp(datetime.now())

    async with async_session_maker() as session:
        markets_after_update = await NBAMarketsRepo().get_markets_count(session)

    return {
        "time": end_ts - start_ts,
        "tables": {
            "games": {"new": stats["games"] - len(future_games), "updated": stats["games"]},
            "markets": {"new": markets_after_update - markets_before_update, "updated": stats["markets"]},
            "prices": {"new": stats["prices"], "updated": 0},
        },
    }


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_database_update())
