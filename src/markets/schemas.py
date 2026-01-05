import re
from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class NBAMarket(BaseModel):
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
            return self

        parts = re.split(r"\s+vs\.?\s+", self.market_title, flags=re.IGNORECASE)
        if len(parts) != 2:
            return self

        self.host_team = parts[0].strip()
        self.guest_team = parts[1].strip()
        return self

    @model_validator(mode="after")
    def parse_score(self):
        if not self.game_score:
            return self

        if "-" not in self.game_score:
            return self

        left, right = self.game_score.split("-", 1)
        try:
            self.guest_score = int(left.strip())
            self.host_score = int(right.strip())
        except ValueError:
            pass

        return self
