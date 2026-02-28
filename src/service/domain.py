from enum import StrEnum


class NBATeam(StrEnum):
    PHI = "76ers"
    MIL = "Bucks"
    CHI = "Bulls"
    CLE = "Cavaliers"
    BOS = "Celtics"
    LAC = "Clippers"
    MEM = "Grizzlies"
    ATL = "Hawks"
    MIA = "Heat"
    CHA = "Hornets"
    UTA = "Jazz"
    SAC = "Kings"
    NYK = "Knicks"
    LAL = "Lakers"
    ORL = "Magic"
    DAL = "Mavericks"
    BKN = "Nets"
    DEN = "Nuggets"
    IND = "Pacers"
    NOP = "Pelicans"
    DET = "Pistons"
    TOR = "Raptors"
    HOU = "Rockets"
    SAS = "Spurs"
    PHX = "Suns"
    OKC = "Thunder"
    MIN = "Timberwolves"
    POR = "Blazers"
    GSW = "Warriors"
    WAS = "Wizards"


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


class GameStatus(StrEnum):
    NOT_STARTED = "not_started"
    LIVE = "live"
    FINISHED = "finished"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"


GAME_STATUS_NORMALIZATION_MAP = {
    "NS": GameStatus.NOT_STARTED,
    "SCHED": GameStatus.NOT_STARTED,
    "Q1": GameStatus.LIVE,
    "Q2": GameStatus.LIVE,
    "Q3": GameStatus.LIVE,
    "Q4": GameStatus.LIVE,
    "HT": GameStatus.LIVE,
    "OT": GameStatus.LIVE,
    "OT1": GameStatus.LIVE,
    "OT2": GameStatus.LIVE,
    "FT": GameStatus.FINISHED,
    "FT OT": GameStatus.FINISHED,
    "FT OT1": GameStatus.FINISHED,
    "FT OT2": GameStatus.FINISHED,
    "VFT": GameStatus.FINISHED,
    "CAN": GameStatus.CANCELLED,
    "POST": GameStatus.CANCELLED,
    "SUSP": GameStatus.SUSPENDED,
}


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


class ScheduleRange(StrEnum):
    TODAY = "today"
    WEEK = "week"
    ALL = "all"
