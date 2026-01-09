import aiohttp

from src.core.api import BasePolymarketAPIClient
from src.core.errors import BaseClientError, BaseLoaderError, BaseParserError
from src.core.loading import SQLAlchemyLoader
from src.core.parsing import DataFrameParser
from src.database.connection import async_session_maker
from src.database.models import BaseModel
from src.logger import logger


class BaseDataUpdater:
    def __init__(
        self,
        client: BasePolymarketAPIClient,
        parser: DataFrameParser,
        loader_cls: type[SQLAlchemyLoader],
        model_cls: type[BaseModel],
    ) -> None:
        self.client = client
        self.parser = parser
        self.loader_cls = loader_cls
        self.model_cls = model_cls

    async def _dump(self, session: aiohttp.ClientSession) -> None:
        try:
            await self.client.dump(session)
            logger.info("Exported data to '%s'", self.client.path)
        except BaseClientError:
            logger.exception("Failed to export data from '%s'", self.client.url)
            raise

    async def _parse(self) -> None:
        try:
            await self.parser.parse(self.client.path)
            if len(self.parser.df) > 0:
                logger.info("Parsed %s items from '%s'", len(self.parser.df), self.client.path)
            else:
                logger.info("No items to parse from '%s'", self.client.path)
        except BaseParserError:
            logger.exception("Failed to parse items from '%s'", self.client.path)
            raise

    async def _load(self) -> None:
        try:
            async with async_session_maker() as session:
                loader = self.loader_cls(self.model_cls, session)
                await loader.load(self.parser.df)
                await session.commit()
                if loader.rowcount > 0:
                    logger.info("Loaded %s items from '%s'", loader.rowcount, self.client.path)
                else:
                    logger.info("No items to load from '%s'", self.client.path)
        except BaseLoaderError:
            logger.exception("Failed to load items from '%s'", self.client.path)
            raise

    async def update(self):
        async with aiohttp.ClientSession() as session:
            await self._dump(session)
        await self._parse()
        await self._load()
