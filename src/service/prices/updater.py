import asyncio
from collections.abc import Sequence

from src.core.loading import DataFrameLoader
from src.core.updating import BaseDataUpdater
from src.database.connection import async_session_maker
from src.database.models import NBAPricesModel
from src.database.repos import NBAMarketsRepo
from src.logger import logger
from src.service.prices.clients import NBAPricesClient
from src.service.prices.parsers import NBAPricesParser


async def get_markets_ids(repo: NBAMarketsRepo) -> Sequence[int]:
    async with async_session_maker() as session:
        markets = await repo.get_market_ids_without_prices(session)
        logger.info("Found '%s' markets without prices", len(markets))
    return markets


async def get_market_timestamps(repo: NBAMarketsRepo, market_id: int) -> tuple[int, int]:
    async with async_session_maker() as session:
        start_dt, end_dt = await repo.get_ended_market_time_window(session, market_id)
        start_ts, end_ts = start_dt.timestamp(), end_dt.timestamp()
    return int(start_ts), int(end_ts)


async def get_clob_id_tokens(repo: NBAMarketsRepo, market_id: int) -> tuple[str, str]:
    async with async_session_maker() as session:
        tokens = await repo.get_market_tokens(session, market_id)
    return tokens


async def update_market_prices():
    """
    Market is represented by teams token ids, so it is mandatory
    to make separate calls for guest and host team token prices
    """

    markets_repo = NBAMarketsRepo()
    markets = await get_markets_ids(markets_repo)

    for market in markets:
        guest, host = await get_clob_id_tokens(markets_repo, market)
        start, end = await get_market_timestamps(markets_repo, market)

        guest_payload = {"market": guest, "startTs": start, "endTs": end}

        guest_prices = BaseDataUpdater(
            client=NBAPricesClient(params=guest_payload),
            parser=NBAPricesParser(market_id=market, is_guest=True),
            loader_cls=DataFrameLoader,
            model_cls=NBAPricesModel,
        )
        await guest_prices.update()

        host_payload = {"market": host, "startTs": start, "endTs": end}

        host_prices = BaseDataUpdater(
            client=NBAPricesClient(params=host_payload),
            parser=NBAPricesParser(market_id=market, is_guest=False),
            loader_cls=DataFrameLoader,
            model_cls=NBAPricesModel,
        )
        await host_prices.update()


if __name__ == "__main__":
    asyncio.run(update_market_prices())
