import asyncio

from src.service.update import run_update

if __name__ == "__main__":
    asyncio.run(run_update())
