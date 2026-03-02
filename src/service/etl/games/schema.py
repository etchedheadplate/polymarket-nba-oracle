import re
from datetime import date
from typing import Any

from pydantic import Field, model_validator

from src.core.parser import BaseJsonSchema
from src.service.domain.games import GAME_STATUS_NORMALIZATION_MAP, GameStatus, NBATeam


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
