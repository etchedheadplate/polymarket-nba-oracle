import asyncio

from src.service.update import update_database


async def main():
    await update_database()


if __name__ == "__main__":
    asyncio.run(main())
