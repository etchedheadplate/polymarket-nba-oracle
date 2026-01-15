import contextlib
import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import Field, model_validator

from src.core.processing import BaseJSONSchema

NBA_TEAMS = {
    "PHI": "76ers",
    "MIL": "Bucks",
    "CHI": "Bulls",
    "CLE": "Cavaliers",
    "BOS": "Celtics",
    "LAC": "Clippers",
    "MEM": "Grizzlies",
    "ATL": "Hawks",
    "MIA": "Heat",
    "CHA": "Hornets",
    "UTA": "Jazz",
    "SAC": "Kings",
    "NYK": "Knicks",
    "LAL": "Lakers",
    "ORL": "Magic",
    "DAL": "Mavericks",
    "BKN": "Nets",
    "DEN": "Nuggets",
    "IND": "Pacers",
    "NOP": "Pelicans",
    "DET": "Pistons",
    "TOR": "Raptors",
    "HOU": "Rockets",
    "SAS": "Spurs",
    "PHX": "Suns",
    "OKC": "Thunder",
    "MIN": "Timberwolves",
    "POR": "Blazers",
    "GSW": "Warriors",
    "WAS": "Wizards",
}

NBA_TEAMS_BY_NAME = {v.lower(): k for k, v in NBA_TEAMS.items()}


class NBAGameSchema(BaseJSONSchema):
    """Maps JSON fields to 'nba_games' model fields"""

    id: int | None = None
    event_slug: str = Field(alias="slug")
    event_title: str = Field(alias="title")

    game_id: int | None = Field(default=None, alias="gameId")
    game_date: date = Field(alias="eventDate")
    game_status: str = Field(alias="period")
    game_is_live: bool = Field(default=False, alias="live")

    guest_team: str | None = None
    guest_score: int | None = None
    host_team: str | None = None
    host_score: int | None = None

    @model_validator(mode="after")
    def parse_teams(self):
        if not self.event_title:
            raise ValueError("Empty event title")

        teams = re.split(r"\s+vs\.?\s+", self.event_title, flags=re.IGNORECASE)
        if len(teams) != 2:
            raise ValueError("Cannot split teams from title")

        guest_raw = teams[0].strip().lower()
        host_raw = teams[1].strip().lower()

        self.guest_team = None
        self.host_team = None

        for name, code in NBA_TEAMS_BY_NAME.items():
            if name in guest_raw:
                self.guest_team = code
            if name in host_raw:
                self.host_team = code

        if not self.guest_team or not self.host_team:
            raise ValueError(f"Failed to parse teams from title: '{self.event_title}'")

        return self

    @model_validator(mode="before")
    def parse_score(cls, values: dict[str, Any]):
        raw_score = values.get("score")
        if raw_score and "-" in raw_score:
            guest, host = raw_score.split("-", 1)
            values["guest_score"] = int(guest.strip())
            values["host_score"] = int(host.strip())
        return values


class NBAMarketSchema(BaseJSONSchema):
    """Maps JSON fields to 'nba_markets' model fields"""

    id: int | None = None
    event_id: int | None = None

    market_question: str = Field(alias="question")
    market_type: str = Field(default="moneyline", alias="sportsMarketType")
    market_start: datetime = Field(alias="gameStartTime")
    market_end: datetime = Field(alias="closedTime")

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


class NBAPriceSchema(BaseJSONSchema):
    """Maps JSON fields to 'nba_prices' model fields"""

    market_id: int | None = None

    timestamp: int = Field(alias="timestamp")
    price_guest: Decimal | None = Field(default=None, alias="price_guest")
    price_host: Decimal | None = Field(default=None, alias="price_host")
