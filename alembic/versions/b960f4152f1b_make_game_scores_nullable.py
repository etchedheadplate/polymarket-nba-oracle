"""Make game scores nullable

Revision ID: b960f4152f1b
Revises: ba94f168b95a
Create Date: 2026-01-15 21:40:05.247412

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "b960f4152f1b"
down_revision: str | Sequence[str] | None = "ba94f168b95a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
