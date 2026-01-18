from typing import Any
from urllib.parse import urljoin

import aiohttp

from src.core.dump import PolymarketClobAPIClient, PolymarketGammaAPIClient


class NBAGamesClient(PolymarketGammaAPIClient):
    _subdirname = "games"
    _file_prefix = "series"
    _file_rewrite = True
    _rate_limit = 349  # https://docs.polymarket.com/quickstart/introduction/rate-limits#gamma-api-rate-limits

    def __init__(self, session: aiohttp.ClientSession, by_slug: bool = False) -> None:
        if by_slug:  # legacy pre-2025/26 seasons
            super().__init__(session=session, params=[{"slug": "nba"}])
        else:  # seasons 2025/26 and later
            super().__init__(session=session, endpoints=["10345"])
        self._base = urljoin(self._base, "series/")


class NBAMarketsClient(PolymarketGammaAPIClient):
    _subdirname = "markets"
    _file_prefix = "event"
    _file_rewrite = False
    _rate_limit = 499  # https://docs.polymarket.com/quickstart/introduction/rate-limits#gamma-api-rate-limits

    def __init__(self, session: aiohttp.ClientSession, endpoints: list[str]) -> None:
        super().__init__(session=session, endpoints=endpoints)
        self._base = urljoin(self._base, "events/")


class NBAPricesClient(PolymarketClobAPIClient):
    _subdirname = "prices"
    _file_prefix = "token"
    _file_rewrite = False
    _rate_limit = 999  # https://docs.polymarket.com/quickstart/introduction/rate-limits#clob-markets-&-pricing

    def __init__(self, session: aiohttp.ClientSession, params: list[dict[str, Any]] | None = None) -> None:
        if params is None:
            params = [{}]
        super().__init__(session=session, params=params)
        self._base = urljoin(self._base, "prices-history/")
