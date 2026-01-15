import asyncio

from src.service.update import update_games, update_markets, update_prices


async def main():
    await update_games()
    await update_markets()
    await update_prices()


if __name__ == "__main__":
    asyncio.run(main())
