"""rename constraints to new naming

Revision ID: aaab30fefff7
Revises: 6205f03e481c
Create Date: 2026-01-23 12:47:09.279172

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "aaab30fefff7"
down_revision: str | Sequence[str] | None = "6205f03e481c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    op.execute("ALTER INDEX nba_prices_pkey RENAME TO market_prices_pkey;")
    op.execute("ALTER TABLE market_prices RENAME CONSTRAINT nba_prices_market_id_fkey TO market_prices_market_id_fkey;")

    op.execute("ALTER INDEX nba_markets_pkey RENAME TO event_markets_pkey;")
    op.execute("ALTER TABLE event_markets RENAME CONSTRAINT nba_markets_event_id_fkey TO event_markets_event_id_fkey;")

    op.execute("ALTER INDEX nba_games_pkey RENAME TO game_events_pkey;")


def downgrade():
    op.execute("ALTER INDEX market_prices_pkey RENAME TO nba_prices_pkey;")
    op.execute("ALTER TABLE market_prices RENAME CONSTRAINT market_prices_market_id_fkey TO nba_prices_market_id_fkey;")

    op.execute("ALTER INDEX event_markets_pkey RENAME TO nba_markets_pkey;")
    op.execute("ALTER TABLE event_markets RENAME CONSTRAINT event_markets_event_id_fkey TO nba_markets_event_id_fkey;")

    op.execute("ALTER INDEX game_events_pkey RENAME TO nba_games_pkey;")
