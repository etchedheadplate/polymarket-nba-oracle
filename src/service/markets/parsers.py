import json
from abc import abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles
import pandas as pd
from pydantic import ValidationError

from src.core.errors import BaseParserError
from src.core.parsing import BaseParser
from src.service.markets.schemas import NBAMarketGameSchema


class NBAMarketsParser(BaseParser):
    def __init__(self) -> None:
        super().__init__()
        self.start_date: datetime | None = None
        self.end_date: datetime | None = None

    @abstractmethod
    def _extract_games(self, raw_markets: Any) -> list[dict[str, Any]]:
        pass

    def _filter_games(self, raw_games: list[dict[str, Any]]) -> list[NBAMarketGameSchema]:
        if self.start_date is None or self.end_date is None:
            raise BaseParserError("Parser dates are not initialized")

        filtered_games: list[NBAMarketGameSchema] = []
        for game in raw_games:
            try:
                market = NBAMarketGameSchema.model_validate(game)

                if market.game_start_date is None:
                    continue

                if self.start_date < market.game_start_date <= self.end_date:
                    filtered_games.append(market)

            except (KeyError, ValidationError, ValueError):
                continue

        return filtered_games

    def _create_df(self, games: list[NBAMarketGameSchema]) -> pd.DataFrame:
        return pd.DataFrame([game.model_dump() for game in games])

    async def parse(self, file: Path) -> None:
        try:
            async with aiofiles.open(file) as f:
                raw_markets = json.loads(await f.read())
                raw_games = self._extract_games(raw_markets)
                filtered_games = self._filter_games(raw_games)
                self.df = self._create_df(filtered_games)
        except (OSError, json.JSONDecodeError) as e:
            raise BaseParserError("Failed to read file") from e

    def set_dates(self, start_date: datetime, end_date: datetime) -> None:
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)
        self.start_date = start_date
        self.end_date = end_date


class ArchiveNBAMarketsParser(NBAMarketsParser):
    def _extract_games(self, raw_markets: Any) -> list[dict[str, Any]]:
        return raw_markets[0]["events"]


class CurrentNBAMarketsParser(NBAMarketsParser):
    def _extract_games(self, raw_markets: Any) -> list[dict[str, Any]]:
        return raw_markets["events"]
