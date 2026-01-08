import json
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

import aiofiles
import aiohttp

from src.core.errors import BaseClientError


class BasePolymarketGammaAPIClient(ABC):
    base = "https://gamma-api.polymarket.com/"
    dirname = Path("temp")

    @property
    @abstractmethod
    def endpoint(self) -> str:
        pass

    @property
    @abstractmethod
    def filename(self) -> str:
        pass

    @property
    def url(self) -> str:
        return self.base + self.endpoint

    @property
    def path(self) -> Path:
        today = date.today().isoformat()
        return self.dirname / f"{today}_{self.filename}"

    async def dump(self, session: aiohttp.ClientSession) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        try:
            async with session.get(self.url, timeout=aiohttp.ClientTimeout(total=10)) as response:
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
