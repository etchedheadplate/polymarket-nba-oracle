import re
from datetime import date
from typing import Any

from pydantic import BaseModel, Field, model_validator

from src.service.teams import NBA_TEAMS_BY_NAME


class NBAGameSchema(BaseModel):
    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }

    id: int | None = None

    event_id: int = Field(alias="id")
    event_slug: str = Field(alias="slug")
    event_title: str = Field(alias="title")

    game_id: int | None = Field(default=None, alias="gameId")
    game_date: date | None = Field(default=None, alias="eventDate")
    game_status: str | None = Field(default=None, alias="status")
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
            raise ValueError("Cannot parse teams from title")

        guest_raw = teams[0].strip().lower()
        host_raw = teams[1].strip().lower()

        try:
            self.guest_team = NBA_TEAMS_BY_NAME[guest_raw]
            self.host_team = NBA_TEAMS_BY_NAME[host_raw]
        except KeyError:
            raise ValueError("Unknown NBA team")

        return self

    @model_validator(mode="after")
    def parse_score(self):
        raw_score: Any = getattr(self, "score", None)
        if not raw_score:
            return self

        if "-" not in raw_score:
            return self

        guest, host = raw_score.split("-", 1)
        try:
            self.guest_score = int(guest.strip())
            self.host_score = int(host.strip())
        except ValueError:
            raise ValueError("Cannot parse team scores")

        return self
