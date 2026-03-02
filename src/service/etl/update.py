import asyncio

from src.service.etl.games import update_games
from src.service.etl.markets import update_markets
from src.service.etl.prices import update_prices


async def main():
    await asyncio.gather(
        update_games(),
        update_markets(),
        update_prices(),
    )


if __name__ == "__main__":
    asyncio.run(main())
