"""rename nba tables to generic domain names

Revision ID: 6205f03e481c
Revises: 0788688536db
Create Date: 2026-01-23 12:18:53.229846

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6205f03e481c"
down_revision: str | Sequence[str] | None = "0788688536db"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    op.rename_table("nba_prices", "market_prices")
    op.rename_table("nba_markets", "event_markets")
    op.rename_table("nba_games", "game_events")

    op.execute("ALTER TABLE game_events RENAME CONSTRAINT uq_nba_games_event_slug TO uq_game_events_event_slug")
    op.execute(
        "ALTER TABLE event_markets RENAME CONSTRAINT uq_nba_markets_event_question TO uq_event_markets_event_question"
    )
    op.execute("ALTER TABLE market_prices RENAME CONSTRAINT uq_market_price_market_ts TO uq_market_prices_market_ts")


def downgrade():
    op.rename_table("market_prices", "nba_prices")
    op.rename_table("event_markets", "nba_markets")
    op.rename_table("game_events", "nba_games")

    op.execute("ALTER TABLE game_events RENAME CONSTRAINT uq_game_events_event_slug TO uq_nba_games_event_slug")
    op.execute(
        "ALTER TABLE event_markets RENAME CONSTRAINT uq_event_markets_event_question TO uq_nba_markets_event_question"
    )
    op.execute("ALTER TABLE market_prices RENAME CONSTRAINT uq_market_prices_market_ts TO uq_market_price_market_ts")
