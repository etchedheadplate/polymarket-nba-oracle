import contextlib
import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import Field, model_validator

from src.core.parsers import BaseJsonSchema
from src.service.domain import GAME_STATUS_NORMALIZATION_MAP, GameStatus, MarketType, NBATeam


class NBAGameSchema(BaseJsonSchema):
    """Maps JSON fields to 'game_events' model fields"""

    id: int | None = None
    event_slug: str = Field(alias="slug")
    event_title: str = Field(alias="title")

    game_id: int | None = Field(default=None, alias="gameId")
    game_date: date = Field(alias="eventDate")
    game_period: str = Field(alias="period")
    game_status: GameStatus

    guest_team: str | None = None
    host_team: str | None = None

    guest_score: int | None = None
    host_score: int | None = None

    @model_validator(mode="before")
    def create_game_status(cls, values: dict[str, Any]):
        raw_period = values.get("period", "")
        values["game_status"] = GAME_STATUS_NORMALIZATION_MAP.get(raw_period, GameStatus.UNKNOWN)
        return values

    @model_validator(mode="before")
    def parse_score(cls, values: dict[str, Any]):
        raw_score = values.get("score")
        if raw_score and "-" in raw_score:
            guest, host = raw_score.split("-", 1)
            values["guest_score"] = int(guest.strip())
            values["host_score"] = int(host.strip())
        return values

    @model_validator(mode="after")
    def parse_teams(self):
        if not self.event_title:
            raise ValueError("Empty event title")

        teams = re.split(r"\s+vs\.?\s+", self.event_title, flags=re.IGNORECASE)
        if len(teams) != 2:
            raise ValueError(f"Cannot split teams from title: '{self.event_title}'")

        guest_raw = teams[0].strip().lower()
        host_raw = teams[1].strip().lower()

        self.guest_team = None
        self.host_team = None

        def match_team(raw_name: str):
            raw_name_clean = re.sub(r"[^\w\s]", "", raw_name).strip().lower()
            for team in NBATeam:
                team_full = team.value.lower()
                team_words = team_full.split()
                if all(word in raw_name_clean for word in team_words):
                    return team.name
            return None

        self.guest_team = match_team(guest_raw)
        self.host_team = match_team(host_raw)

        if not self.guest_team or not self.host_team:
            raise ValueError(f"Failed to parse teams from title: '{self.event_title}'")

        return self


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


class NBAPriceSchema(BaseJsonSchema):
    """Maps JSON fields to 'market_prices' model fields"""

    market_id: int | None = None

    timestamp: int = Field(alias="timestamp")
    price_guest_buy: Decimal | None = Field(default=None, alias="price_guest_buy")
    price_host_buy: Decimal | None = Field(default=None, alias="price_host_buy")
