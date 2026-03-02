from typing import Any

from src.core.parser import JsonParser
from src.service.etl.markets.schema import NBAMarketSchema


class NBAMarketsParser(JsonParser):
    def __init__(self) -> None:
        super().__init__()

    def _extract(self) -> None:
        self._raw_markets: list[Any] = []
        if self._raw_json:
            self._event_id = self._raw_json.get("id", None)
            self._raw_markets = self._raw_json.get("markets", [])

    def _validate(self) -> None:
        if self._event_id is not None:
            for raw_market in self._raw_markets:
                try:
                    market = NBAMarketSchema.model_validate(raw_market, extra="ignore")
                    market.event_id = int(self._event_id)
                    self.parsed_items.append(market)
                except (KeyError, ValueError):
                    continue
