import json
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles
import pandas as pd
from pydantic import ValidationError

from src.markets.errors import MarketParserError
from src.markets.schemas import NBAMarket


class BaseNBAMarketParser(ABC):
    def __init__(self) -> None:
        self.start_date, self.end_date = self._build_dates_range()
        self.games: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    def _build_dates_range(self) -> tuple[datetime, datetime]:
        pass

    @abstractmethod
    async def export_games_to_df(self, markets_file: Path) -> None:
        pass

    def _filter_games(self, raw_games: list[dict[str, Any]]) -> list[NBAMarket]:
        filtered: list[NBAMarket] = []

        for game in raw_games:
            try:
                market = NBAMarket.model_validate(game)

                if market.game_start_date is None:
                    continue

                if self.start_date <= market.game_start_date <= self.end_date:
                    filtered.append(market)

            except (KeyError, ValidationError, ValueError) as e:
                raise MarketParserError("Failed to parse market") from e

        return filtered

    def _create_df(self, games: list[NBAMarket]) -> pd.DataFrame:
        return pd.DataFrame([game.model_dump() for game in games])


class ArchiveNBAMarketsParser(BaseNBAMarketParser):
    def _build_dates_range(self) -> tuple[datetime, datetime]:
        """Dates of 2024/2025 NBA season markets"""
        start = datetime(year=2024, month=10, day=21, tzinfo=UTC)
        end = datetime(year=2025, month=7, day=1, tzinfo=UTC)
        return start, end

    async def export_games_to_df(self, markets_file: Path) -> None:
        try:
            async with aiofiles.open(markets_file) as f:
                raw_markets = json.loads(await f.read())
                raw_games = raw_markets[0]["events"]
                filtered_games = self._filter_games(raw_games)
                self.games = self._create_df(filtered_games)
        except OSError as e:
            raise MarketParserError("Failed to read file markets_file") from e


class CurrentNBAMarketsParser(BaseNBAMarketParser):
    def _build_dates_range(self) -> tuple[datetime, datetime]:
        """Dates of 2025/2026 and later season markets"""
        start = datetime(year=2025, month=10, day=21, tzinfo=UTC)
        end = datetime.now(tz=UTC)
        return start, end

    async def export_games_to_df(self, markets_file: Path) -> None:
        try:
            async with aiofiles.open(markets_file) as f:
                raw_markets = json.loads(await f.read())
                raw_games = raw_markets["events"]
                filtered_games = self._filter_games(raw_games)
                self.games = self._create_df(filtered_games)
        except OSError as e:
            raise MarketParserError("Failed to read file markets_file") from e
