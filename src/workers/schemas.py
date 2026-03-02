from typing import Any
from uuid import UUID

from pydantic import BaseModel, field_validator


class Message(BaseModel):
    id: str
    payload: dict[Any, Any] = {}

    @field_validator("id", mode="before")
    @classmethod
    def validate_uuid4(cls, v: str) -> str:
        try:
            u = UUID(v, version=4)
        except ValueError:
            raise ValueError(f"id {v} is not a valid UUID4")
        if str(u) != v:
            raise ValueError(f"id {v} is not a canonical UUID4 string")
        return v


class Request(Message):
    name: str


class Response(Message):
    done: bool
