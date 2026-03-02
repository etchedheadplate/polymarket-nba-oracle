from typing import Any
from urllib.parse import urljoin

import aiohttp

from src.core.clients import PolymarketOracleClobAPIClient


class NBAPricesClient(PolymarketOracleClobAPIClient):
    _subdirname = "prices"
    _file_prefix = "token"
    _file_rewrite = False
    _rate_limit = 999  # https://docs.polymarket.com/quickstart/introduction/rate-limits#clob-markets-&-pricing

    def __init__(self, session: aiohttp.ClientSession, params: list[dict[str, Any]] | None = None) -> None:
        if params is None:
            params = [{}]
        super().__init__(session=session, params=params)
        self._base = urljoin(self._base, "prices-history/")
