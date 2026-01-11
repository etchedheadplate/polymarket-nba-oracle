from pydantic import Field

from src.core.validation import BaseJSONSchema


class NBAPriceSchema(BaseJSONSchema):
    """Maps JSON fields to 'nba_prices' model fields"""

    market_id: int | None = None

    timestamp: int = Field(alias="timestamp")
    price_guest: float | None = Field(default=None, alias="price_guest")
    price_host: float | None = Field(default=None, alias="price_host")
