from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    DUMPS_DIR: Path = Path("dumps")

    DB_NAME: str = "polymarket_nba_oracle"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PASS: str = "postgres"

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_VHOST: str = "/"

    EXCHANGE_NAME: str = "polymarket.nba"
    QUEUE_TG_BOT: str = "tg_bot"
    QUEUE_ORACLE: str = "oracle"
    QUEUE_REPORT: str = "report"
    RK_REQUEST: str = "request"
    RK_RESPONSE: str = "response"

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()  # pyright: ignore[reportCallIssue]
