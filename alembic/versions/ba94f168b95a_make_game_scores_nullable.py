"""Make game scores nullable

Revision ID: ba94f168b95a
Revises: 39a32dd3cad3
Create Date: 2026-01-15 21:38:06.269232

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "ba94f168b95a"
down_revision: str | Sequence[str] | None = "39a32dd3cad3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
