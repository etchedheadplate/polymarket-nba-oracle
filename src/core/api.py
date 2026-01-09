import json
from abc import ABC
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiofiles
import aiohttp

from src.core.errors import BaseClientError


class BasePolymarketAPIClient(ABC):
    base: str
    dirname = Path("temp")
    timeout = aiohttp.ClientTimeout(total=10)

    def __init__(
        self,
        endpoint: str,
        filename: str,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._filename = filename
        self._params = dict(params) if params else {}

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def params(self) -> dict[str, Any]:
        return self._params

    @property
    def url(self) -> str:
        if self.params:
            return f"{self.base}{self.endpoint}?{urlencode(self.params)}"
        return f"{self.base}{self.endpoint}"

    @property
    def path(self) -> Path:
        return self.dirname / Path(self.filename)

    async def dump(self, session: aiohttp.ClientSession) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with session.get(url=self.url, timeout=self.timeout) as response:
                response.raise_for_status()
                data = await response.json()
        except aiohttp.ClientError as e:
            raise BaseClientError(f"Request failed for {self.url}") from e
        except json.JSONDecodeError as e:
            raise BaseClientError(f"Invalid JSON received from {self.url}") from e

        try:
            async with aiofiles.open(self.path, "w") as f:
                await f.write(json.dumps(data, indent=4))
        except OSError as e:
            raise BaseClientError(f"Failed to write file {self.path}") from e


class PolymarketGammaAPIClient(BasePolymarketAPIClient):
    base = "https://gamma-api.polymarket.com/"


class PolymarketClobAPIClient(BasePolymarketAPIClient):
    base = "https://clob.polymarket.com/"
