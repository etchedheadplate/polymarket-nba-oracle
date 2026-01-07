from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config import settings

ASYNC_DATABASE_URL = f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

async_engine = create_async_engine(ASYNC_DATABASE_URL)
async_session_maker = async_sessionmaker(async_engine, expire_on_commit=False)
