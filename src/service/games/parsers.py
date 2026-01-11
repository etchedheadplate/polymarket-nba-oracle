from abc import abstractmethod
from datetime import date
from typing import Any

from pydantic import ValidationError

from src.core.parsing import DataFrameParser
from src.service.games.schemas import NBAGameSchema


class NBAGamesParser(DataFrameParser):
    def __init__(self, start_date: date, end_date: date) -> None:
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    @abstractmethod
    def _extract_items(self, raw_json: Any) -> list[dict[str, Any]]:
        pass

    def _filter_items(self, raw_items: list[dict[str, Any]]) -> list[NBAGameSchema]:
        filtered_games: list[NBAGameSchema] = []
        for raw_game in raw_items:
            try:
                game = NBAGameSchema.model_validate(raw_game)

                if game.game_date is None:
                    continue

                if self.start_date < game.game_date <= self.end_date:
                    filtered_games.append(game)

            except (KeyError, ValidationError, ValueError):
                continue

        return filtered_games


class NBAGamesBySlugParser(NBAGamesParser):
    def _extract_items(self, raw_json: Any) -> list[dict[str, Any]]:
        return raw_json[0]["events"]


class NBAGamesBySeriesIdParser(NBAGamesParser):
    def _extract_items(self, raw_json: Any) -> list[dict[str, Any]]:
        return raw_json["events"]
