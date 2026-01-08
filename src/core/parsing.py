from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class BaseParser(ABC):
    def __init__(self) -> None:
        self.df: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    async def parse(self, file: Path) -> None:
        pass
