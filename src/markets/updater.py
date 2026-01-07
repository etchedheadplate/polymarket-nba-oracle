import asyncio
from datetime import UTC, datetime

import aiohttp

from src.database.connection import async_session_maker
from src.database.models import NBAMarketGameModel
from src.database.repos import NBAMarketGameRepo
from src.logger import logger
from src.markets.clients import ArchiveNBAMarketsClient, BasePolymarketGammaAPIClient, CurrentNBAMarketsClient
from src.markets.errors import MarketLoaderError, MarketParserError, PolymarketClientError
from src.markets.loaders import BaseNBADataLoader
from src.markets.parsers import ArchiveNBAMarketsParser, BaseNBAMarketParser, CurrentNBAMarketsParser


class NBAMarketsUpdater:
    def __init__(
        self,
        client: BasePolymarketGammaAPIClient,
        parser: BaseNBAMarketParser,
        model: type = NBAMarketGameModel,
    ) -> None:
        self.client = client
        self.parser = parser
        self.model = model

    async def _export_markets(self, session: aiohttp.ClientSession) -> None:
        try:
            await self.client.export_markets_to_json(session)
            logger.info("Exported markets to '%s'", self.client.path)
        except PolymarketClientError:
            logger.exception("Failed to export markets from '%s'", self.client.url)
            raise

    async def _get_dates(self) -> None:
        async with async_session_maker() as session:
            repo = NBAMarketGameRepo()
            latest = await repo.get_latest_game_start_date(session)
            if latest is None:
                dates = (  # dates of 2024/2025 NBA season markets
                    datetime(year=2024, month=10, day=21, tzinfo=UTC),
                    datetime(year=2025, month=7, day=1, tzinfo=UTC),
                )
            else:
                dates = (latest, datetime.now(tz=UTC))  # dates of 2025/2026 and later season markets
            self.dates = dates

    async def _parse_markets(self) -> None:
        try:
            self.parser.set_dates(*self.dates)
            await self.parser.export_games_to_df(self.client.path)
            if len(self.parser.games) > 0:
                logger.info("Parsed %s markets from '%s'", len(self.parser.games), self.client.path)
            else:
                logger.info("No markets to parse from '%s'", self.client.path)
        except MarketParserError:
            logger.exception("Failed to parse markets from '%s'", self.client.path)
            raise

    async def _load_markets(self) -> None:
        try:
            async with async_session_maker() as session:
                loader = BaseNBADataLoader(self.model, session)
                await loader.load_df_to_db(self.parser.games)
                await session.commit()
                if loader.rowcount > 0:
                    logger.info("Loaded %s markets from '%s'", loader.rowcount, self.client.path)
                else:
                    logger.info("No markets to load from '%s'", self.client.path)
        except MarketLoaderError:
            logger.exception("Failed to load markets from '%s'", self.client.path)
            raise

    async def update(self):
        async with aiohttp.ClientSession() as session:
            await self._export_markets(session)
        await self._get_dates()
        await self._parse_markets()
        await self._load_markets()


async def update_markets():
    """Archive markets should be updated first to escape overlapping with current markets"""

    archive_markets = NBAMarketsUpdater(client=ArchiveNBAMarketsClient(), parser=ArchiveNBAMarketsParser())
    await archive_markets.update()

    current_markets = NBAMarketsUpdater(client=CurrentNBAMarketsClient(), parser=CurrentNBAMarketsParser())
    await current_markets.update()


if __name__ == "__main__":
    asyncio.run(update_markets())
