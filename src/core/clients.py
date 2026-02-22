import asyncio
import json
from abc import ABC
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiofiles
import aiohttp
from aiolimiter import AsyncLimiter

from src.logger import logger


class BasePolymarketOracleAPIClient(ABC):
    _base: str
    _dirname = "dumps"
    _subdirname: str
    _file_prefix: str
    _file_rewrite: bool
    _timeout = aiohttp.ClientTimeout(total=30)
    _rate_limit: int
    _call_period: float
    _max_concurrent_requests = 10

    def __init__(
        self,
        session: aiohttp.ClientSession,
        endpoints: list[str] | None = None,
        params: list[dict[str, Any]] | None = None,
    ):
        self._session = session
        self._endpoints = [""] if endpoints is None else endpoints
        self._params: list[dict[str, Any]] = [{}] if params is None else params
        self._path = self._build_path()
        self._limiter = AsyncLimiter(self._rate_limit, self._call_period)
        self._url_count = 0
        self._dump_count = 0
        self.dumped_files: dict[str, Path] = {}  # URL -> Path

    def _build_url(self, endpoint: str, params: dict[str, Any]) -> str:
        query = urlencode(params)
        base = f"{self._base.rstrip('/')}/{endpoint.lstrip('/')}" if endpoint else self._base.rstrip("/")
        self._url_count += 1
        return base + (f"?{query}" if query else "")

    def _build_path(self) -> Path:
        return Path(self._dirname, self._subdirname)

    def _build_filename(self, endpoint: str, params: dict[str, Any]) -> str:
        parts = [self._subdirname, self._file_prefix]
        if endpoint:
            parts.append(endpoint.strip("/").replace("/", "_"))
        if params:
            parts.extend(str(v) for v in params.values())
        return "_".join(parts) + ".json"

    async def _make_request(self, url: str, retries: int = 3) -> Any:
        for attempt in range(retries):
            try:
                async with self._limiter, self._session.get(url, timeout=self._timeout) as response:
                    response.raise_for_status()
                    return await response.json()
            except (TimeoutError, aiohttp.ClientError) as e:
                logger.debug("Request failed (%s), attempt %s/%s: %s", url, attempt + 1, retries, e)
                await asyncio.sleep(2**attempt)
        logger.debug("Failed to fetch data after %s attempts: %s", retries, url)
        return None

    async def _save_file(self, path: Path, data: Any) -> Path | None:
        if data is None:
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(path, "w") as f:
                await f.write(json.dumps(data, indent=4))
            return path
        except OSError as e:
            logger.error("Failed to save file %s: %s", path, e)
            return None

    async def _dump_one_file(self, endpoint: str, params: dict[str, Any], semaphore: asyncio.Semaphore):
        url = self._build_url(endpoint, params)
        name = self._build_filename(endpoint, params)
        path = self._path / name

        if path.exists() and not self._file_rewrite:
            self.dumped_files[url] = path
            return

        async with semaphore:
            data = await self._make_request(url)
            file = await self._save_file(path, data)
            if file:
                self.dumped_files[url] = file
                self._dump_count += 1

    async def dump(self) -> dict[str, Path]:
        self._path.mkdir(parents=True, exist_ok=True)
        semaphore = asyncio.Semaphore(self._max_concurrent_requests)

        tasks = [self._dump_one_file(e, p, semaphore) for e in self._endpoints for p in self._params]
        await asyncio.gather(*tasks)

        return self.dumped_files


class PolymarketOracleGammaAPIClient(BasePolymarketOracleAPIClient):
    _base = "https://gamma-api.polymarket.com/"
    _call_period = 10.0


class PolymarketOracleClobAPIClient(BasePolymarketOracleAPIClient):
    _base = "https://clob.polymarket.com/"
    _call_period = 10.0
