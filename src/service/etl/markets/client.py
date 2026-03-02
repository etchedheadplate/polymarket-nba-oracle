from urllib.parse import urljoin

import aiohttp

from src.core.clients import PolymarketOracleGammaAPIClient


class NBAMarketsClient(PolymarketOracleGammaAPIClient):
    _subdirname = "markets"
    _file_prefix = "event"
    _file_rewrite = True
    _rate_limit = 499  # https://docs.polymarket.com/quickstart/introduction/rate-limits#gamma-api-rate-limits

    def __init__(self, session: aiohttp.ClientSession, endpoints: list[str]) -> None:
        super().__init__(session=session, endpoints=endpoints)
        self._base = urljoin(self._base, "events/")
