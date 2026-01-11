import contextlib
import json
from datetime import datetime
from typing import Any

from pydantic import Field, model_validator

from src.core.validation import BaseJSONSchema


class NBAMarketSchema(BaseJSONSchema):
    """Maps JSON fields to 'nba_markets' model fields"""

    event_id: int | None = None

    market_id: int = Field(alias="id")
    market_type: str | None = Field(default="moneyline", alias="sportsMarketType")
    market_start: datetime = Field(alias="gameStartTime")
    market_end: datetime = Field(alias="closedTime")

    order_min_price: float | None = Field(default=None, alias="orderPriceMinTickSize")
    order_min_size: float | None = Field(default=None, alias="orderMinSize")

    token_id_guest: str
    token_id_host: str

    @model_validator(mode="before")
    def parse_dates(cls, values: dict[str, Any]):
        date_field_names = ["gameStartTime", "closedTime"]

        for field in date_field_names:
            val = values.get(field)
            if isinstance(val, str):
                if val.endswith("+00"):
                    val = val.replace("+00", "+00:00")
                try:
                    values[field] = datetime.fromisoformat(val)
                except ValueError:
                    contextlib.suppress(ValueError)

        return values

    @model_validator(mode="before")
    def parse_tokens(cls, values: Any):
        raw_tokens = values.get("clobTokenIds")
        if raw_tokens:
            try:
                tokens = json.loads(raw_tokens)
                if len(tokens) == 2:
                    values["token_id_guest"] = tokens[0]
                    values["token_id_host"] = tokens[1]
            except json.JSONDecodeError:
                values["token_id_guest"] = None
                values["token_id_host"] = None
        return values
