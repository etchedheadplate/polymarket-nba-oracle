"""Make game scores nullable

Revision ID: c3b2aee0ea54
Revises: b960f4152f1b
Create Date: 2026-01-15 21:41:22.069461

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "c3b2aee0ea54"
down_revision: str | Sequence[str] | None = "b960f4152f1b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
