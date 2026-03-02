import contextlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field, model_validator

from src.core.parser import BaseJsonSchema
from src.service.domain.markets import MarketType


class NBAMarketSchema(BaseJsonSchema):
    """Maps JSON fields to 'event_markets' model fields"""

    id: int | None = None
    event_id: int | None = None

    market_question: str = Field(alias="question")
    market_type: MarketType = Field(default=MarketType.moneyline, alias="sportsMarketType")
    market_start: datetime = Field(alias="gameStartTime")
    market_end: datetime | None = Field(default=None, alias="closedTime")

    order_min_price: Decimal = Field(alias="orderPriceMinTickSize")
    order_min_size: float = Field(alias="orderMinSize")

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
