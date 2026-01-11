from typing import Any

from pydantic import ValidationError

from src.core.parsing import DataFrameParser
from src.service.prices.schemas import NBAPriceSchema


class NBAPricesParser(DataFrameParser):
    def __init__(self, market_id: int, is_guest: bool) -> None:
        super().__init__()
        self.market_id = market_id
        self.is_guest = is_guest

    def _extract_items(self, raw_json: Any) -> list[dict[str, Any]]:
        return raw_json["history"]

    def _filter_items(self, raw_items: list[dict[str, Any]]) -> list[NBAPriceSchema]:
        filtered_prices: list[NBAPriceSchema] = []
        for raw_price in raw_items:
            price_guest = raw_price["p"] if self.is_guest else None
            price_host = raw_price["p"] if not self.is_guest else None
            try:
                price = NBAPriceSchema.model_validate(
                    {
                        "market_id": self.market_id,
                        "timestamp": raw_price["t"],
                        "price_guest": price_guest,
                        "price_host": price_host,
                    }
                )
                filtered_prices.append(price)
            except (KeyError, ValidationError, ValueError):
                continue

        return filtered_prices
