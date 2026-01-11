from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint, event, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import DeclarativeBase, Mapped, Mapper, mapped_column, relationship


class BaseModel(DeclarativeBase):
    pass


class NBAGamesModel(BaseModel):
    __tablename__ = "nba_games"

    id: Mapped[int] = mapped_column(primary_key=True)

    event_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    event_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    event_title: Mapped[str] = mapped_column(String(255), nullable=False)

    game_id: Mapped[int | None] = mapped_column(Integer)
    game_date: Mapped[date | None] = mapped_column(Date)
    game_status: Mapped[str | None] = mapped_column(String(50))
    game_is_live: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    guest_team: Mapped[str | None] = mapped_column(String(100))
    guest_score: Mapped[int | None] = mapped_column(Integer)
    host_team: Mapped[str | None] = mapped_column(String(100))
    host_score: Mapped[int | None] = mapped_column(Integer)

    markets: Mapped[list["NBAMarketsModel"]] = relationship(
        back_populates="event",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NBAMarketsModel(BaseModel):
    __tablename__ = "nba_markets"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("nba_games.id", ondelete="CASCADE"), nullable=False)

    market_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    market_type: Mapped[str | None] = mapped_column(String(50))
    market_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    market_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    order_min_price: Mapped[float | None] = mapped_column(Float)
    order_min_size: Mapped[float | None] = mapped_column(Float)

    token_id_guest: Mapped[str] = mapped_column(String(100))
    token_id_host: Mapped[str] = mapped_column(String(100))

    event: Mapped["NBAGamesModel"] = relationship(back_populates="markets")
    prices: Mapped[list["NBAPricesModel"]] = relationship(
        back_populates="market",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NBAPricesModel(BaseModel):
    __tablename__ = "nba_prices"

    __table_args__ = (UniqueConstraint("market_id", "timestamp", name="uq_market_price_market_ts"),)

    Index("ix_prices_ts", "market_id", "timestamp")

    id: Mapped[int] = mapped_column(primary_key=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("nba_markets.id", ondelete="CASCADE"), nullable=False)

    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price_guest: Mapped[float] = mapped_column(Float, nullable=False)
    price_host: Mapped[float] = mapped_column(Float, nullable=False)

    market: Mapped["NBAMarketsModel"] = relationship(back_populates="prices")


@event.listens_for(NBAPricesModel, "before_update")
def prevent_price_update(mapper: Mapper["NBAPricesModel"], connection: Connection, target: "NBAPricesModel") -> None:
    raise RuntimeError("NBAPricesModel rows are immutable")
