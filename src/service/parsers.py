from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

from src.core.parsers import JsonParser
from src.service.schemas import NBAGameSchema, NBAMarketSchema, NBAPriceSchema


class NBAGamesParser(JsonParser):
    def __init__(self, start_date: date | None = None, end_date: date | None = None, by_slug: bool = False) -> None:
        super().__init__()
        if not start_date or not end_date:
            if by_slug:
                self._start_date, self._end_date = (
                    date(year=2024, month=10, day=21),
                    date(year=2025, month=7, day=1),
                )  # dates of legacy pre-2025/26 NBA season games
            else:
                self._start_date, self._end_date = (
                    date(year=2025, month=7, day=2),
                    datetime.now(tz=UTC).date() + timedelta(weeks=2),
                )  # 2025/26 NBA season and future announced games
        else:
            self._start_date, self._end_date = start_date, end_date
        self.by_slug = by_slug

    def _extract(self) -> None:
        self._raw_games: list[Any] = []
        if self._raw_json:
            if self.by_slug:  # legacy pre-2025/26 seasons
                first_item = self._raw_json[0]
                self._raw_games = first_item.get("events", [])
            else:  # seasons 2025/26 and later
                self._raw_games = self._raw_json.get("events", [])

    def _validate(self) -> None:
        for raw_game in self._raw_games:
            try:
                game = NBAGameSchema.model_validate(raw_game, extra="ignore")
                if self._start_date <= game.game_date <= self._end_date:
                    self.parsed_items.append(game)
            except (KeyError, ValueError):
                continue


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
