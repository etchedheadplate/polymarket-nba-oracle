from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, Numeric, String, UniqueConstraint, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class BaseModel(DeclarativeBase): ...


class NBAGamesModel(BaseModel):
    __tablename__ = "nba_games"
    __table_args__ = (UniqueConstraint("event_slug", name="uq_nba_games_event_slug"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    event_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    event_title: Mapped[str] = mapped_column(String(255), nullable=False)

    game_id: Mapped[int | None] = mapped_column(Integer)
    game_date: Mapped[date] = mapped_column(Date, nullable=False)
    game_status: Mapped[str] = mapped_column(String(50), nullable=False)
    game_is_live: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    guest_team: Mapped[str] = mapped_column(String(100), nullable=False)
    guest_score: Mapped[int | None] = mapped_column(Integer, default=None, nullable=True)
    host_team: Mapped[str] = mapped_column(String(100), nullable=False)
    host_score: Mapped[int | None] = mapped_column(Integer, default=None, nullable=True)

    markets: Mapped[list["NBAMarketsModel"]] = relationship(
        back_populates="event",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NBAMarketsModel(BaseModel):
    __tablename__ = "nba_markets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("nba_games.id", ondelete="CASCADE"), nullable=False)

    market_question: Mapped[str | None] = mapped_column(String(255), nullable=False)
    market_type: Mapped[str] = mapped_column(String(50), nullable=False)
    market_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    market_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    order_min_price: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    order_min_size: Mapped[float] = mapped_column(Float, nullable=False)

    token_id_guest: Mapped[str] = mapped_column(String(100), nullable=False)
    token_id_host: Mapped[str] = mapped_column(String(100), nullable=False)

    event: Mapped["NBAGamesModel"] = relationship(back_populates="markets")
    prices: Mapped[list["NBAPricesModel"]] = relationship(
        back_populates="market",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NBAPricesModel(BaseModel):
    __tablename__ = "nba_prices"

    __table_args__ = (UniqueConstraint("market_id", "timestamp", name="uq_market_price_market_ts"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("nba_markets.id", ondelete="CASCADE"), nullable=False)

    timestamp: Mapped[int] = mapped_column(Integer, nullable=False, doc="Unix timestamp (seconds, UTC)")
    price_guest: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=True)
    price_host: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=True)

    market: Mapped["NBAMarketsModel"] = relationship(back_populates="prices")
