from enum import StrEnum


class MarketType(StrEnum):  # adjusted for needed types
    # assists = "assists"
    # first_half_moneyline = "first_half_moneyline"
    # first_half_spreads = "first_half_spreads"
    # first_half_totals = "first_half_totals"
    moneyline = "moneyline"
    # points = "points"
    # rebounds = "rebounds"
    # spreads = "spreads"
    # totals = "totals"


class OrderStatus(StrEnum):
    OPEN = "open"
    PARTIAL = "partial"
    FILLED = "filled"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
