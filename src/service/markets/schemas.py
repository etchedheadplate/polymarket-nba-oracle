import json
from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class NBAMarketSchema(BaseModel):
    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }

    id: int | None = None
    event_id: int | None = None

    market_id: int = Field(alias="id")
    market_type: str | None = Field(alias="sportsMarketType")
    market_start: datetime = Field(alias="gameStartTime")
    market_end: datetime | None = Field(default=None, alias="closedTime")

    order_min_price: float | None = Field(default=None, alias="orderPriceMinTickSize")
    order_min_size: float | None = Field(default=None, alias="orderMinSize")

    token_id_guest: str
    token_id_host: str

    @model_validator(mode="after")
    def parse_tokens(self):
        raw_tokens = getattr(self, "clobTokenIds", None)
        if not raw_tokens:
            return self

        tokens = json.loads(raw_tokens)
        if len(tokens) < 2:
            raise ValueError("Cannot parse CLOB token ids")

        self.token_id_guest = tokens[0]
        self.token_id_host = tokens[1]
        return self
