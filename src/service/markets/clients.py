from collections.abc import Mapping
from typing import Any

from src.core.api import PolymarketGammaAPIClient


class NBAMarketsClient(PolymarketGammaAPIClient):
    def __init__(
        self,
        event_id: int,
        filename: str | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        endpoint = f"events/{event_id}"
        if filename is None:
            filename = f"markets/markets_event_{event_id}.json"
        super().__init__(endpoint=endpoint, filename=filename, params=params)
