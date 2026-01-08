import asyncio

from src.service.updater import update_markets

if __name__ == "__main__":
    asyncio.run(update_markets())
