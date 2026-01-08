import asyncio
from datetime import UTC, datetime, timedelta

import aiohttp

from src.core.api import BasePolymarketGammaAPIClient
from src.core.errors import BaseClientError, BaseLoaderError, BaseParserError
from src.core.loading import BaseLoader
from src.database.connection import async_session_maker
from src.database.models import NBAMarketGameModel
from src.database.repos import NBAMarketGameRepo
from src.logger import logger
from src.service.markets.clients import ArchiveNBAMarketsClient, CurrentNBAMarketsClient
from src.service.markets.parsers import ArchiveNBAMarketsParser, CurrentNBAMarketsParser, NBAMarketsParser


class NBAMarketsUpdater:
    def __init__(
        self,
        client: BasePolymarketGammaAPIClient,
        parser: NBAMarketsParser,
        loader_cls: type[BaseLoader],
    ) -> None:
        self.client = client
        self.parser = parser
        self.loader_cls = loader_cls
        self.model = NBAMarketGameModel
        self.repo = NBAMarketGameRepo()

    async def _get_dates(self) -> tuple[datetime, datetime]:
        async with async_session_maker() as session:
            latest = await self.repo.get_latest_game_start_date(session)
            if latest is None:
                dates = (  # dates of archive 2024/2025 NBA season markets
                    datetime(year=2024, month=10, day=21, tzinfo=UTC),
                    datetime(year=2025, month=7, day=1, tzinfo=UTC),
                )
            else:
                dates = (latest, datetime.now(tz=UTC) + timedelta(weeks=2))  # future markets for announced games
        return dates

    async def _dump_markets(self, session: aiohttp.ClientSession) -> None:
        try:
            await self.client.dump(session)
            logger.info("Exported markets to '%s'", self.client.path)
        except BaseClientError:
            logger.exception("Failed to export markets from '%s'", self.client.url)
            raise

    async def _parse_markets(self, dates: tuple[datetime, datetime]) -> None:
        try:
            self.parser.set_dates(*dates)
            await self.parser.parse(self.client.path)
            if len(self.parser.df) > 0:
                logger.info("Parsed %s markets from '%s'", len(self.parser.df), self.client.path)
            else:
                logger.info("No markets to parse from '%s'", self.client.path)
        except BaseParserError:
            logger.exception("Failed to parse markets from '%s'", self.client.path)
            raise

    async def _load_markets(self) -> None:
        try:
            async with async_session_maker() as session:
                loader = self.loader_cls(self.model, session)
                await loader.load_df_to_db(self.parser.df)
                await session.commit()
                if loader.rowcount > 0:
                    logger.info("Loaded %s markets from '%s'", loader.rowcount, self.client.path)
                else:
                    logger.info("No markets to load from '%s'", self.client.path)
        except BaseLoaderError:
            logger.exception("Failed to load markets from '%s'", self.client.path)
            raise

    async def update(self):
        async with aiohttp.ClientSession() as session:
            await self._dump_markets(session)
        dates = await self._get_dates()
        await self._parse_markets(dates)
        await self._load_markets()


async def update_markets():
    """Archive markets should be updated first to escape overlapping with current markets"""

    archive_markets = NBAMarketsUpdater(
        client=ArchiveNBAMarketsClient(),
        parser=ArchiveNBAMarketsParser(),
        loader_cls=BaseLoader,
    )
    await archive_markets.update()

    current_markets = NBAMarketsUpdater(
        client=CurrentNBAMarketsClient(),
        parser=CurrentNBAMarketsParser(),
        loader_cls=BaseLoader,
    )
    await current_markets.update()


if __name__ == "__main__":
    asyncio.run(update_markets())
