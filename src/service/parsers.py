from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

from pydantic import ValidationError

from src.core.processing import JsonParser
from src.service.schemas import NBAGameSchema, NBAMarketSchema, NBAPriceSchema


class NBAGamesParser(JsonParser):
    def __init__(self, start_date: date | None = None, end_date: date | None = None, by_slug: bool = False) -> None:
        super().__init__()
        if not start_date or not end_date:
            if by_slug:
                self.start_date, self.end_date = (
                    date(year=2024, month=10, day=21),
                    date(year=2025, month=7, day=1),
                )  # dates of legacy pre-2025/26 NBA season games
            else:
                self.start_date, self.end_date = (
                    date(year=2025, month=7, day=2),
                    datetime.now(tz=UTC).date() + timedelta(weeks=2),
                )  # 2025/26 NBA season and future announced games
        else:
            self.start_date, self.end_date = start_date, end_date
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
                if self.start_date < game.game_date <= self.end_date:
                    self.parsed_items.append(game)
            except (KeyError, ValidationError, ValueError):
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
                except (KeyError, ValidationError, ValueError):
                    continue


class NBAPricesParser(JsonParser):
    def __init__(self, token_market_map: dict[int, int], is_guest: bool) -> None:
        super().__init__()
        self.token_market_map = token_market_map
        self.is_guest = is_guest
        self._last_price = None
        self.same_price_timeout = 600

    def _extract(self) -> None:
        parsed = urlparse(self._current_url)
        query = parse_qs(parsed.query)
        values = query.get("market")
        self._token_id = int(values[0]) if values else None

        self._raw_prices: list[Any] = []
        if self._raw_json:
            self._raw_prices = self._raw_json.get("history", [])

    def _validate(self) -> None:
        if self._token_id is not None:
            market_id = self.token_market_map.get(self._token_id, None)
            if market_id is not None:
                for raw_price in self._raw_prices:
                    price = raw_price["p"]
                    ts = raw_price["t"]

                    if self._last_price is None:
                        self._last_price = price
                        self._last_price_ts = ts
                    elif price == self._last_price:
                        if ts - self._last_price_ts >= self.same_price_timeout:
                            break
                    else:
                        self._last_price = price
                        self._last_price_ts = ts

                    try:
                        price = NBAPriceSchema.model_validate(
                            {
                                "market_id": market_id,
                                "timestamp": ts,
                                "price_guest": price if self.is_guest else None,
                                "price_host": price if not self.is_guest else None,
                            }
                        )
                        self.parsed_items.append(price)
                    except (KeyError, ValidationError, ValueError):
                        continue
