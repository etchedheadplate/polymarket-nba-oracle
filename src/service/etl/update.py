import asyncio

from src.service.etl.games import update_games
from src.service.etl.markets import update_markets
from src.service.etl.prices import update_prices


async def update_database() -> dict[str, int]:
    """Returns updated row counts for each table"""

    games, markets, prices = await asyncio.gather(
        update_games(),
        update_markets(),
        update_prices(),
    )

    return {
        "games": games,
        "markets": markets,
        "prices": prices,
    }


if __name__ == "__main__":
    asyncio.run(update_database())
