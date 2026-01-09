import json
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import aiofiles
import pandas as pd

from src.core.errors import BaseParserError
from src.core.validation import BaseJSONSchema


class BaseParser(ABC):
    def __init__(self) -> None:
        self.file: Any = None

    @abstractmethod
    async def parse(self, file: Path) -> None:
        pass


class DataFrameParser(BaseParser):
    def __init__(self) -> None:
        super().__init__()
        self.df: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    def _extract_items(self, raw_json: Any) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def _filter_items(self, raw_items: list[dict[str, Any]]) -> Sequence[BaseJSONSchema]:
        pass

    def _create_df(self, schemas: Sequence[BaseJSONSchema]) -> pd.DataFrame:
        return pd.DataFrame([schema.model_dump() for schema in schemas])

    async def parse(self, file: Path) -> None:
        try:
            async with aiofiles.open(file) as f:
                raw_json = json.loads(await f.read())
                raw_items = self._extract_items(raw_json)
                filtered_items = self._filter_items(raw_items)
                self.df = self._create_df(filtered_items)
        except (OSError, json.JSONDecodeError) as e:
            raise BaseParserError("Failed to read file") from e
