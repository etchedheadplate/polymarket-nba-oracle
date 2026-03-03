from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    SERVICE_NAME: str
    LOG_DIR: Path

    DB_NAME: str
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str

    REDIS_BROKER: str
    REDIS_BACKEND: str

    RABBITMQ_HOST: str
    RABBITMQ_PORT: int
    RABBITMQ_USER: str
    RABBITMQ_PASSWORD: str
    RABBITMQ_VHOST: str

    EXCHANGE_NAME: str
    QUEUE_TGBOT: str
    QUEUE_ORACLE: str
    QUEUE_REPORT: str
    RK_REQUEST: str
    RK_RESPONSE: str

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()  # pyright: ignore[reportCallIssue]
