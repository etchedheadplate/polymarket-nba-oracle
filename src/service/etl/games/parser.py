from datetime import UTC, date, datetime, timedelta
from typing import Any

from src.core.parser import JsonParser
from src.service.etl.games.schema import NBAGameSchema


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
