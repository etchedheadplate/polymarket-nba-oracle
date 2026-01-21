import json
from abc import ABC, abstractmethod
from pathlib import Path

import aiofiles
from pydantic import BaseModel, ValidationError

from src.logger import logger


class BaseJsonSchema(BaseModel):
    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }


class JsonParser(ABC):
    def __init__(self) -> None:
        self._ingested_data: dict[str, Path] = {}  # URL: file path
        self.parsed_items: list[BaseJsonSchema] = []

    def ingest(self, json_files: dict[str, Path]) -> None:
        self._ingested_data = json_files

    @abstractmethod
    def _extract(self) -> None: ...

    @abstractmethod
    def _validate(self) -> None: ...

    async def parse(self) -> list[BaseJsonSchema]:
        for url, file in self._ingested_data.items():
            self._raw_json = None
            self._current_url = url
            self._current_file = file
            try:
                async with aiofiles.open(file) as f:
                    self._raw_json = json.loads(await f.read())
                    self._extract()
                    self._validate()
            except ValidationError as e:
                logger.debug("Validation skipped: %s", e)
            except Exception:
                logger.exception("Unexpected error")
                continue

        logger.info("Parsed %s items from %s files", len(self.parsed_items), len(self._ingested_data))
        return self.parsed_items
