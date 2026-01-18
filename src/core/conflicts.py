from abc import ABC, abstractmethod
from collections.abc import Sequence

from sqlalchemy import case
from sqlalchemy.dialects.postgresql import Insert as PostgresInsert


class ConflictStrategy(ABC):
    @abstractmethod
    def apply(self, stmt: PostgresInsert) -> PostgresInsert: ...


class DoNothingOnConflict(ConflictStrategy):
    def __init__(self, index_elements: Sequence[str]):
        self.index_elements = index_elements

    def apply(self, stmt: PostgresInsert) -> PostgresInsert:
        return stmt.on_conflict_do_nothing(index_elements=self.index_elements)


class UpdateNonNullFields(ConflictStrategy):
    def __init__(self, index_elements: Sequence[str], fields_to_update: Sequence[str]):
        self.index_elements = index_elements
        self.fields_to_update = fields_to_update

    def apply(self, stmt: PostgresInsert) -> PostgresInsert:

        update_dict = {
            field: case((stmt.excluded[field].isnot(None), stmt.excluded[field]), else_=getattr(stmt.table.c, field))
            for field in self.fields_to_update
        }
        return stmt.on_conflict_do_update(index_elements=self.index_elements, set_=update_dict)
