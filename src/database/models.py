from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, Float, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class BaseModel(DeclarativeBase): ...


class NBAGamesModel(BaseModel):
    __tablename__ = "game_events"
    __table_args__ = (UniqueConstraint("event_slug", name="uq_game_events_event_slug"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    event_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    event_title: Mapped[str] = mapped_column(String(255), nullable=False)

    game_id: Mapped[int | None] = mapped_column(Integer)
    game_date: Mapped[date] = mapped_column(Date, nullable=False)
    game_period: Mapped[str] = mapped_column(String(20), nullable=True)
    game_status: Mapped[str] = mapped_column(String(20), nullable=False)

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
    __tablename__ = "event_markets"
    __table_args__ = (UniqueConstraint("event_id", "market_question", name="uq_event_markets_event_question"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("game_events.id", ondelete="CASCADE"), nullable=False)

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
    __tablename__ = "market_prices"

    __table_args__ = (UniqueConstraint("market_id", "timestamp", name="uq_market_prices_market_ts"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("event_markets.id", ondelete="CASCADE"), nullable=False)

    timestamp: Mapped[int] = mapped_column(Integer, nullable=False, doc="Unix timestamp (seconds, UTC)")
    price_guest_buy: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=True)
    price_guest_sell: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=True)
    price_host_buy: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=True)
    price_host_sell: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=True)

    market: Mapped["NBAMarketsModel"] = relationship(back_populates="prices")
