from collections.abc import AsyncGenerator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy_utils import create_database, database_exists  # type: ignore

from src.config import settings
from src.database.models import Base
from src.logger import logger

SYNC_DATABASE_URL = f"postgresql+psycopg2://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

engine = create_engine(SYNC_DATABASE_URL)


def create_db():
    if not database_exists(engine.url):
        create_database(engine.url)
        logger.info(f"Database {settings.DB_NAME} created.")
    else:
        logger.info(f"Database {settings.DB_NAME} exists.")


ASYNC_DATABASE_URL = f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

async_engine = create_async_engine(ASYNC_DATABASE_URL)
async_session_maker = async_sessionmaker(async_engine, expire_on_commit=False)


async def create_tables():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session
