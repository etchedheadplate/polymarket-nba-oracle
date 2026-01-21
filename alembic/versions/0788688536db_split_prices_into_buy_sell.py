"""split prices into buy/sell

Revision ID: 0788688536db
Revises: abb6767430d2
Create Date: 2026-01-21 17:14:51.323095

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0788688536db"
down_revision: str | Sequence[str] | None = "abb6767430d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE nba_prices RENAME COLUMN price_guest TO price_guest_buy")
    op.execute("ALTER TABLE nba_prices RENAME COLUMN price_host TO price_host_buy")

    op.add_column(
        "nba_prices",
        sa.Column("price_guest_sell", sa.Numeric(10, 6), nullable=True),
    )
    op.add_column(
        "nba_prices",
        sa.Column("price_host_sell", sa.Numeric(10, 6), nullable=True),
    )


def downgrade():
    op.drop_column("nba_prices", "price_guest_sell")
    op.drop_column("nba_prices", "price_host_sell")

    op.execute("ALTER TABLE nba_prices RENAME COLUMN price_guest_buy TO price_guest")
    op.execute("ALTER TABLE nba_prices RENAME COLUMN price_host_buy TO price_host")
