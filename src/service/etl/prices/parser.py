from typing import Any
from urllib.parse import parse_qs, urlparse

from src.core.parser import JsonParser
from src.service.etl.prices.schema import NBAPriceSchema


class NBAPricesParser(JsonParser):
    _min_game_time = 100 * 60  # assumed min time for NBA game
    _min_waste_time = 30 * 60  # window when price doesn't change
    _lost_price = 0.01
    _won_price = 0.99

    def __init__(self, token_market_map: dict[int, int], is_guest: bool) -> None:
        super().__init__()
        self._token_market_map = token_market_map
        self._is_guest = is_guest

    def _extract(self) -> None:
        parsed_url = urlparse(self._current_url)
        query = parse_qs(parsed_url.query)
        values = query.get("market")
        self._token_id = int(values[0]) if values else None

        self._raw_prices: list[Any] = []
        if self._raw_json:
            self._raw_prices = self._raw_json.get("history", [])

    def _validate(self) -> None:
        if self._token_id is None:
            return

        market_id = self._token_market_map.get(self._token_id)
        if market_id is None:
            return

        parsed: list[NBAPriceSchema] = []
        first_ts = None
        window_start_ts = None
        window_start_price = None

        for raw in self._raw_prices:
            price = raw.get("p")
            ts = raw.get("t")
            if price is None or ts is None:
                continue

            if first_ts is None:
                first_ts = ts

            parsed.append(
                NBAPriceSchema.model_validate(
                    {
                        "market_id": market_id,
                        "timestamp": ts,
                        "price_guest_buy": price if self._is_guest else None,
                        "price_host_buy": price if not self._is_guest else None,
                    }
                )
            )

            if ts - first_ts < self._min_game_time:
                continue

            if price < self._lost_price or price > self._won_price:
                if window_start_ts is None:
                    window_start_ts = ts
                    window_start_price = price
                elif ts - window_start_ts >= self._min_waste_time:
                    parsed = [p for p in parsed if p.timestamp < window_start_ts]
                    break
            else:
                if window_start_ts is None or abs(price - window_start_price) > 1e-8:
                    window_start_ts = ts
                    window_start_price = price
                elif ts - window_start_ts >= self._min_waste_time:
                    parsed = [p for p in parsed if p.timestamp < window_start_ts]
                    break

        self.parsed_items.extend(parsed)
