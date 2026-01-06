from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, event
from sqlalchemy.engine import Connection
from sqlalchemy.orm import DeclarativeBase, Mapped, Mapper, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class NBAMarketGameModel(Base):
    __tablename__ = "nba_market_games"

    market_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    market_title: Mapped[str] = mapped_column(String(255), nullable=False)

    game_id: Mapped[int | None] = mapped_column(Integer)
    game_start_date: Mapped[datetime | None] = mapped_column(DateTime)
    game_elapsed_time: Mapped[str | None] = mapped_column(String(50))
    game_score: Mapped[str | None] = mapped_column(String(20))
    game_period: Mapped[str | None] = mapped_column(String(50))
    game_is_live: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    guest_team: Mapped[str | None] = mapped_column(String(100))
    guest_score: Mapped[int | None] = mapped_column(Integer)
    host_team: Mapped[str | None] = mapped_column(String(100))
    host_score: Mapped[int | None] = mapped_column(Integer)

    prices: Mapped[list["NBAMarketPriceModel"]] = relationship(
        back_populates="market",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NBAMarketPriceModel(Base):
    __tablename__ = "nba_market_prices"

    __table_args__ = (
        UniqueConstraint(
            "market_id",
            "recorded_at",
            name="uq_nba_market_price_market_id_recorded_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("nba_market_games.market_id", ondelete="CASCADE"), nullable=False)

    recorded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    guest_price: Mapped[float] = mapped_column(Float, nullable=False)
    host_price: Mapped[float] = mapped_column(Float, nullable=False)

    market: Mapped["NBAMarketGameModel"] = relationship(back_populates="prices")


@event.listens_for(NBAMarketPriceModel, "before_update")
def prevent_price_update(
    mapper: Mapper["NBAMarketPriceModel"], connection: Connection, target: "NBAMarketPriceModel"
) -> None:
    raise RuntimeError("NBAMarketPriceModel rows are immutable")
