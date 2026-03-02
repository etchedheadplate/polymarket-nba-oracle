from urllib.parse import urljoin

import aiohttp

from src.core.clients import PolymarketOracleGammaAPIClient


class NBAGamesClient(PolymarketOracleGammaAPIClient):
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
