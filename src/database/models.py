from datetime import datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Market(Base):
    __tablename__ = "market_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    game_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    market_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    market_slug: Mapped[str | None] = mapped_column(String(100))
    market_title: Mapped[str | None] = mapped_column(String(100))
    host_team: Mapped[str] = mapped_column(String(100), nullable=False)
    guest_team: Mapped[str] = mapped_column(String(100), nullable=False)
    host_points: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    guest_points: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    overtimes: Mapped[int | None] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    start_time: Mapped[datetime | None] = mapped_column(DateTime)
    end_time: Mapped[datetime | None] = mapped_column(DateTime)
    host_clob_token_id: Mapped[str] = mapped_column(String(100), nullable=False)
    guest_clob_token_id: Mapped[str] = mapped_column(String(100), nullable=False)

    prices: Mapped[list["Price"]] = relationship(
        back_populates="market",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class Price(Base):
    __tablename__ = "market_prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("market_history.market_id", ondelete="CASCADE"), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    host_price: Mapped[float] = mapped_column(Float, server_default="0", nullable=False)
    guest_price: Mapped[float] = mapped_column(Float, server_default="0", nullable=False)

    market: Mapped["Market"] = relationship(back_populates="prices")
