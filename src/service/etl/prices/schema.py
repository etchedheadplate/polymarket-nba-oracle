from decimal import Decimal

from pydantic import Field

from src.core.parser import BaseJsonSchema


class NBAPriceSchema(BaseJsonSchema):
    """Maps JSON fields to 'market_prices' model fields"""

    market_id: int | None = None

    timestamp: int = Field(alias="timestamp")
    price_guest_buy: Decimal | None = Field(default=None, alias="price_guest_buy")
    price_host_buy: Decimal | None = Field(default=None, alias="price_host_buy")
