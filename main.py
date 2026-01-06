import asyncio
from pathlib import Path

import aiohttp
import pandas as pd

from src.logger import logger
from src.markets.clients import ArchiveNBAMarketsClient, BasePolymarketGammaAPIClient, CurrentNBAMarketsClient
from src.markets.errors import MarketParserError, PolymarketClientError
from src.markets.parsers import ArchiveNBAMarketsParser, BaseNBAMarketParser, CurrentNBAMarketsParser


async def export_markets(client: BasePolymarketGammaAPIClient, session: aiohttp.ClientSession) -> Path:
    try:
        await client.export_markets_to_json(session)
        logger.info("Exported markets to '%s'", client.path)
    except PolymarketClientError:
        logger.exception("Failed to export narkets from '%s'", client.url)
        raise
    return client.path


async def parse_markets(parser: BaseNBAMarketParser, markets_file: Path) -> pd.DataFrame:
    try:
        await parser.export_games_to_df(markets_file)
        logger.info("Successfully parsed '%s'", markets_file)
    except MarketParserError:
        logger.exception("Failed to parse '%s'", markets_file)
        raise
    return parser.games


async def main():
    async with aiohttp.ClientSession() as session:
        archive_client = ArchiveNBAMarketsClient()
        current_client = CurrentNBAMarketsClient()

        archive_json, current_json = await asyncio.gather(
            export_markets(archive_client, session), export_markets(current_client, session)
        )

        archive_parser = ArchiveNBAMarketsParser()
        current_parser = CurrentNBAMarketsParser()

        archive_df, current_df = await asyncio.gather(
            parse_markets(archive_parser, archive_json), parse_markets(current_parser, current_json)
        )

        print(archive_df)
        print(current_df)


if __name__ == "__main__":
    asyncio.run(main())
