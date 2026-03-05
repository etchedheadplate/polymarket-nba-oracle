from datetime import datetime
from typing import Any

from src.database.connection import async_session_maker
from src.service.etl.update import update_database
from src.service.repos import NBAGamesRepo, NBAMarketsRepo


async def run_database_update(payload: None = None) -> dict[str, Any]:
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
