from typing import Any

from pydantic import ValidationError

from src.core.parsing import DataFrameParser
from src.service.markets.schemas import NBAMarketSchema


class NBAMarketsParser(DataFrameParser):
    def __init__(self, event_id: int) -> None:
        super().__init__()
        self.event_id = event_id

    def _extract_items(self, raw_json: Any) -> list[dict[str, Any]]:
        return raw_json["markets"]

    def _filter_items(self, raw_items: list[dict[str, Any]]) -> list[NBAMarketSchema]:
        filtered_markets: list[NBAMarketSchema] = []
        for raw_market in raw_items:
            try:
                market = NBAMarketSchema.model_validate(raw_market)
                market.event_id = self.event_id
                filtered_markets.append(market)
            except (KeyError, ValidationError, ValueError):
                continue

        return filtered_markets
