from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd


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
    async def parse(self, file: Path) -> None:
        pass
