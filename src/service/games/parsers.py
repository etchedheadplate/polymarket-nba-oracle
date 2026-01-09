import json
from abc import abstractmethod
from datetime import date
from pathlib import Path
from typing import Any

import aiofiles
import pandas as pd
from pydantic import ValidationError

from src.core.errors import BaseParserError
from src.core.parsing import DataFrameParser
from src.service.games.schemas import NBAGameSchema


class NBAGamesParser(DataFrameParser):
    def __init__(self, start_date: date, end_date: date) -> None:
        super().__init__()
        self.start_date = start_date
        self.end_date = start_date

    @abstractmethod
    def _extract_events(self, raw_json: Any) -> list[dict[str, Any]]:
        pass

    def _filter_games(self, raw_events: list[dict[str, Any]]) -> list[NBAGameSchema]:
        filtered_games: list[NBAGameSchema] = []
        for game in raw_events:
            try:
                market = NBAGameSchema.model_validate(game)

                if market.game_date is None:
                    continue

                if self.start_date < market.game_date <= self.end_date:
                    filtered_games.append(market)

            except (KeyError, ValidationError, ValueError):
                continue

        return filtered_games

    def _create_df(self, games: list[NBAGameSchema]) -> pd.DataFrame:
        return pd.DataFrame([game.model_dump() for game in games])

    async def parse(self, file: Path) -> None:
        try:
            async with aiofiles.open(file) as f:
                raw_json = json.loads(await f.read())
                raw_events = self._extract_events(raw_json)
                filtered_games = self._filter_games(raw_events)
                self.df = self._create_df(filtered_games)
        except (OSError, json.JSONDecodeError) as e:
            raise BaseParserError("Failed to read file") from e


class NBAGamesBySlugParser(NBAGamesParser):
    def _extract_events(self, raw_json: Any) -> list[dict[str, Any]]:
        return raw_json[0]["events"]


class NBAGamesBySeriesIdParser(NBAGamesParser):
    def _extract_events(self, raw_json: Any) -> list[dict[str, Any]]:
        return raw_json["events"]
