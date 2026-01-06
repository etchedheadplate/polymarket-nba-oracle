import json
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

import requests

from src.markets.errors import PolymarketClientError


class BasePolymarketGammaAPIClient(ABC):
    base = "https://gamma-api.polymarket.com/"
    dirname = "temp"

    def __init__(self) -> None:
        self.url = self._build_url()
        self.path = self._build_path()

    @property
    @abstractmethod
    def endpoint(self) -> str:
        pass

    @property
    @abstractmethod
    def filename(self) -> str:
        pass

    def _build_url(self) -> str:
        return self.base + self.endpoint

    def _build_path(self) -> Path:
        today = date.today().isoformat()
        return Path(self.dirname, f"{today}_{self.filename}")

    def export_markets_to_json(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise PolymarketClientError(f"Request failed for {self.url}") from e
        except json.JSONDecodeError as e:
            raise PolymarketClientError(f"Invalid JSON received from {self.url}") from e

        try:
            with open(self.path, "w") as f:
                json.dump(data, f, indent=4)
        except OSError as e:
            raise PolymarketClientError(f"Failed to write file {self.path}") from e


class ArchiveNBAMarketsClient(BasePolymarketGammaAPIClient):
    """Client for markets before 2025/2026 NBA season (search by market slug)"""

    @property
    def endpoint(self) -> str:
        return "series?slug=nba"

    @property
    def filename(self) -> str:
        return "archive_markets_dump.json"


class CurrentNBAMarketsClient(BasePolymarketGammaAPIClient):
    """Client for 2025/2026 and later NBA seasons markets (search by series id)"""

    @property
    def endpoint(self) -> str:
        return "series/10345"

    @property
    def filename(self) -> str:
        return "current_markets_dump.json"
