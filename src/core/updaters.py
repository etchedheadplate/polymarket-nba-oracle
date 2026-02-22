from pathlib import Path
from typing import Any

import aiohttp

from src.core.clients import BasePolymarketOracleAPIClient
from src.core.conflicts import UpdateNonNullFields
from src.core.loaders import PydanticLoader
from src.core.parsers import JsonParser
from src.database.connection import async_session_maker
from src.database.models import BaseModel


class BaseUpdater:
    _client_cls: type[BasePolymarketOracleAPIClient]
    _parser_cls: type[JsonParser]
    _alchemy_model: type[BaseModel]
    _conflict_strategy: UpdateNonNullFields
    _batch_size: int = 1000

    async def _fetch(self, **client_kwargs: Any) -> dict[str, Path]:
        async with aiohttp.ClientSession() as session:
            client = self._client_cls(session=session, **client_kwargs)
            return await client.dump()

    async def _parse(self, files: dict[str, Path], **parser_kwargs: dict[str, Any]):
        parser = self._parser_cls(**parser_kwargs)
        parser.ingest(json_files=files)
        return await parser.parse()

    async def _load(self, items: list[Any]):
        async with async_session_maker() as session:
            loader = PydanticLoader(
                session=session,
                alchemy_model=self._alchemy_model,
                batch_size=self._batch_size,
                conflict_strategy=self._conflict_strategy,
            )
            await loader.load(data=items)

    async def run(self, client_kwargs: dict[str, Any] | None = None, parser_kwargs: dict[str, Any] | None = None):
        client_kwargs = client_kwargs or {}
        parser_kwargs = parser_kwargs or {}

        files = await self._fetch(**client_kwargs)
        items = await self._parse(files, **parser_kwargs)
        await self._load(items)
