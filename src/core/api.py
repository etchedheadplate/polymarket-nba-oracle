import asyncio
import json
import random
from abc import ABC
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiofiles
import aiohttp
from aiolimiter import AsyncLimiter

from src.core.errors import BaseClientError

MAX_RETRIES = 5


async def backoff_sleep(
    attempt: int,
    base: float = 0.5,
    cap: float = 10.0,
) -> None:
    delay = min(base * (2**attempt), cap)
    delay += random.uniform(0, delay * 0.1)  # nosec B311
    await asyncio.sleep(delay)


class BasePolymarketAPIClient(ABC):
    url_base: str
    dirname = Path("temp")
    timeout = aiohttp.ClientTimeout(total=10)
    limit_requests: int  # https://docs.polymarket.com/quickstart/introduction/rate-limits
    limit_interval: int
    _limiter: AsyncLimiter

    def __init__(
        self,
        endpoint: str,
        filename: str,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._filename = filename
        self._params = dict(params) if params else {}
        self._limiter = AsyncLimiter(
            self.limit_requests,
            self.limit_interval,
        )

    @property
    def params(self) -> dict[str, Any]:
        return self._params

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def url(self) -> str:
        if self.params:
            return f"{self.url_base}{self.endpoint}?{urlencode(self.params)}"
        return f"{self.url_base}{self.endpoint}"

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def path(self) -> Path:
        return self.dirname / Path(self.filename)

    async def _fetch_json(self, session: aiohttp.ClientSession) -> Any:
        for attempt in range(MAX_RETRIES):
            try:
                response = await self._request_once(session)

                async with response:
                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            await asyncio.sleep(float(retry_after))
                        else:
                            await backoff_sleep(attempt)
                        continue

                    response.raise_for_status()
                    return await response.json()

            except aiohttp.ClientResponseError as e:
                if e.status and 500 <= e.status < 600 and attempt < MAX_RETRIES - 1:
                    await backoff_sleep(attempt)
                    continue
                raise BaseClientError(f"Request failed for {self.url}") from e

            except aiohttp.ClientError as e:
                raise BaseClientError(f"Request failed for {self.url}") from e

            except json.JSONDecodeError as e:
                raise BaseClientError(f"Invalid JSON received from {self.url}") from e

        raise BaseClientError(f"Max retries exceeded for {self.url}")

    async def _request_once(self, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        async with self._limiter:
            return await session.get(
                url=self.url,
                timeout=self.timeout,
            )

    async def _write_file(self, data: Any) -> None:
        try:
            async with aiofiles.open(self.path, "w") as f:
                await f.write(json.dumps(data, indent=4))
        except OSError as e:
            raise BaseClientError(f"Failed to write file {self.path}") from e

    async def dump(self, session: aiohttp.ClientSession) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = await self._fetch_json(session)
        await self._write_file(data)


class PolymarketGammaAPIClient(BasePolymarketAPIClient):
    url_base = "https://gamma-api.polymarket.com/"
    limit_requests = 4000  # https://docs.polymarket.com/quickstart/introduction/rate-limits#gamma-api-rate-limits
    limit_interval = 10


class PolymarketClobAPIClient(BasePolymarketAPIClient):
    url_base = "https://clob.polymarket.com/"
    limit_requests = 9000  # https://docs.polymarket.com/quickstart/introduction/rate-limits#general-clob-endpoints
    limit_interval = 10
