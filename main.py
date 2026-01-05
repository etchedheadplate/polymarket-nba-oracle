from pathlib import Path

import pandas as pd

from src.logger import logger
from src.markets.clients import BasePolymarketGammaAPIClient, CurrentNBAMarketsClient, LegacyNBAMarketsClient
from src.markets.errors import MarketParserError, PolymarketClientError
from src.markets.parsers import BaseNBAMarketParser, CurrentNBAMarketsParser, LegacyNBAMarketsParser


def export_markets(client: BasePolymarketGammaAPIClient) -> Path:
    try:
        client.export_markets_to_json()
        logger.info("Exported markets to '%s'", client.path)
    except PolymarketClientError:
        logger.exception("Failed to export '%s' Polymarket NBA markets", client.url)
        raise
    return client.path


def parse_markets(parser: BaseNBAMarketParser, markets_file: Path) -> pd.DataFrame:
    try:
        parser.export_games_to_df(markets_file)
        logger.info("Successfully parsed '%s'", markets_file)
    except MarketParserError:
        logger.exception("Failed to parse '%s'", markets_file)
        raise
    return parser.games


if __name__ == "__main__":
    json_legacy = export_markets(LegacyNBAMarketsClient())
    json_current = export_markets(CurrentNBAMarketsClient())
    df_legacy = parse_markets(LegacyNBAMarketsParser(), json_legacy)
    df_current = parse_markets(CurrentNBAMarketsParser(), json_current)
