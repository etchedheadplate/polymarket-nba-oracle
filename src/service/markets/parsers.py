import json
from pathlib import Path
from typing import Any

import aiofiles
import pandas as pd
from pydantic import ValidationError

from src.core.errors import BaseParserError
from src.core.parsing import DataFrameParser
from src.service.markets.schemas import NBAMarketSchema


class NBAMarketsParser(DataFrameParser):
    def __init__(self, event_id: int) -> None:
        super().__init__()
        self.event_id = event_id

    def _extract_markets(self, raw_json: Any) -> list[dict[str, Any]]:
        return raw_json["markets"]

    def _filter_markets(self, raw_markets: list[dict[str, Any]]) -> list[NBAMarketSchema]:
        filtered_markets: list[NBAMarketSchema] = []
        for raw_market in raw_markets:
            try:
                market = NBAMarketSchema.model_validate(raw_market)
                market.event_id = self.event_id
                filtered_markets.append(market)
            except (KeyError, ValidationError, ValueError):
                continue

        return filtered_markets

    def _create_df(self, markets: list[NBAMarketSchema]) -> pd.DataFrame:
        return pd.DataFrame([market.model_dump() for market in markets])

    async def parse(self, file: Path) -> None:
        try:
            async with aiofiles.open(file) as f:
                raw_json = json.loads(await f.read())
                raw_markets = self._extract_markets(raw_json)
                filtered_markets = self._filter_markets(raw_markets)
                self.df = self._create_df(filtered_markets)
        except (OSError, json.JSONDecodeError) as e:
            raise BaseParserError("Failed to read file") from e
