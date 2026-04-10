from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DB_NAME: str = "polymarket_nba_orcale"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PASS: str = "postgres"

    DUMPS_DIR: Path = Path("dumps")

    REDIS_BROKER: str = "redis://localhost:6379/0"
    REDIS_BACKEND: str = "redis://localhost:6379/1"

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_VHOST: str = "/"

    EXCHANGE_NAME: str = "polymarket.nba"
    QUEUE_TG_BOT: str = "tg_bot"
    QUEUE_ORACLE: str = "oracle"
    QUEUE_REPORT: str = "reort"
    RK_REQUEST: str = "request"
    RK_RESPONSE: str = "response"

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()  # pyright: ignore[reportCallIssue]
