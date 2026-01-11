from collections.abc import Mapping
from typing import Any

from src.core.api import PolymarketClobAPIClient


class NBAPricesClient(PolymarketClobAPIClient):
    limit_requests = 1000  # https://docs.polymarket.com/quickstart/introduction/rate-limits#clob-markets-&-pricing

    def __init__(
        self,
        params: Mapping[str, Any],
        endpoint: str = "prices-history",
        filename: str | None = None,
    ) -> None:
        self.clob_token_id = params["clob_token_id"]
        if filename is None:
            filename = f"prices/prices_{self.clob_token_id}.json"
        super().__init__(endpoint=endpoint, filename=filename, params=params)
