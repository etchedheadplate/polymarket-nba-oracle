import json
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles
import pandas as pd
from pydantic import ValidationError

from src.markets.errors import MarketParserError
from src.markets.schemas import NBAMarketGameSchema


class BaseNBAMarketParser(ABC):
    def __init__(self) -> None:
        self.games: pd.DataFrame = pd.DataFrame()
        self.start_date: datetime | None = None
        self.end_date: datetime | None = None

    @abstractmethod
    async def export_games_to_df(self, markets_file: Path) -> None:
        pass

    def set_dates(self, start_date: datetime, end_date: datetime) -> None:
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)
        self.start_date = start_date
        self.end_date = end_date

    def _filter_games(self, raw_games: list[dict[str, Any]]) -> list[NBAMarketGameSchema]:
        if self.start_date is None or self.end_date is None:
            raise ValueError("Dates not set for parser")

        filtered: list[NBAMarketGameSchema] = []
        for game in raw_games:
            try:
                market = NBAMarketGameSchema.model_validate(game)

                if market.game_start_date is None:
                    continue

                if self.start_date < market.game_start_date <= self.end_date:
                    filtered.append(market)

            except (KeyError, ValidationError, ValueError):
                continue

        return filtered

    def _create_df(self, games: list[NBAMarketGameSchema]) -> pd.DataFrame:
        return pd.DataFrame([game.model_dump() for game in games])


class ArchiveNBAMarketsParser(BaseNBAMarketParser):
    async def export_games_to_df(self, markets_file: Path) -> None:
        if self.start_date is None or self.end_date is None:
            raise ValueError("Dates not set")

        try:
            async with aiofiles.open(markets_file) as f:
                raw_markets = json.loads(await f.read())
                raw_games = raw_markets[0]["events"]
                filtered_games = self._filter_games(raw_games)
                self.games = self._create_df(filtered_games)
        except OSError as e:
            raise MarketParserError("Failed to read file markets_file") from e


class CurrentNBAMarketsParser(BaseNBAMarketParser):
    async def export_games_to_df(self, markets_file: Path) -> None:
        if self.start_date is None or self.end_date is None:
            raise ValueError("Dates not set")

        try:
            async with aiofiles.open(markets_file) as f:
                raw_markets = json.loads(await f.read())
                raw_games = raw_markets["events"]
                filtered_games = self._filter_games(raw_games)
                self.games = self._create_df(filtered_games)
        except OSError as e:
            raise MarketParserError("Failed to read file markets_file") from e
