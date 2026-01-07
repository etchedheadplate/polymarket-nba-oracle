import re
from datetime import UTC, datetime

from pydantic import BaseModel, Field, model_validator

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


class NBAMarketGameSchema(BaseModel):
    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }

    market_id: int = Field(alias="id")
    market_slug: str = Field(alias="slug")
    market_title: str = Field(alias="title")

    game_id: int | None = Field(default=None, alias="gameId")
    game_start_date: datetime | None = Field(default=None, alias="startTime")
    game_elapsed_time: str | None = Field(default=None, alias="elapsed")
    game_score: str | None = Field(default=None, alias="score")
    game_period: str | None = Field(default=None, alias="period")
    game_is_live: bool = Field(default=False, alias="live")

    guest_team: str | None = None
    guest_score: int | None = None
    host_team: str | None = None
    host_score: int | None = None

    @model_validator(mode="after")
    def parse_teams(self):
        if not self.market_title:
            raise ValueError("Empty market title")

        teams = re.split(r"\s+vs\.?\s+", self.market_title, flags=re.IGNORECASE)
        if len(teams) != 2:
            raise ValueError("Cannot parse teams from title")

        host_raw = teams[0].strip().lower()
        guest_raw = teams[1].strip().lower()

        try:
            self.host_team = NBA_TEAMS_BY_NAME[host_raw]
            self.guest_team = NBA_TEAMS_BY_NAME[guest_raw]
        except KeyError:
            raise ValueError("Unknown NBA team")

        return self

    @model_validator(mode="after")
    def parse_score(self):
        if not self.game_score:
            return self

        if "-" not in self.game_score:
            return self

        guest, host = self.game_score.split("-", 1)
        try:
            self.guest_score = int(guest.strip())
            self.host_score = int(host.strip())
        except ValueError:
            pass

        return self

    @model_validator(mode="after")
    def make_game_start_date_aware(self):
        if self.game_start_date and self.game_start_date.tzinfo is None:
            self.game_start_date = self.game_start_date.replace(tzinfo=UTC)
        return self
