from datetime import UTC, date, datetime, timedelta

from src.core.conflicts import UpdateNonNullFields
from src.core.updater import BaseUpdater
from src.database.connection import async_session_maker
from src.database.models import NBAGamesModel
from src.service.etl.games.client import NBAGamesClient
from src.service.etl.games.parser import NBAGamesParser
from src.service.repos import NBAGamesRepo


class GamesUpdater(BaseUpdater):
    _client_cls = NBAGamesClient
    _parser_cls = NBAGamesParser
    _alchemy_model = NBAGamesModel
    _conflict_strategy = UpdateNonNullFields(
        index_elements=["event_slug"], fields_to_update=["guest_score", "host_score", "game_period", "game_status"]
    )


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


async def update_games() -> int:
    start_date, end_date = await construct_game_dates()
    rowcount = 0
    for by_slug in (True, False):
        rowcount += await GamesUpdater().run(
            client_kwargs={"by_slug": by_slug},
            parser_kwargs={"start_date": start_date, "end_date": end_date, "by_slug": by_slug},
        )
    return rowcount


if __name__ == "__main__":
    import asyncio

    asyncio.run(update_games())
