from pydantic import BaseModel


class BaseJSONSchema(BaseModel):
    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }

    id: int | None = None
