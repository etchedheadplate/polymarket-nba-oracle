import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from src.config import settings


class RabbitMQConnection:
    def __init__(self):
        self.url = (
            f"amqp://{settings.RABBITMQ_USER}:{settings.RABBITMQ_PASSWORD}"
            f"@{settings.RABBITMQ_HOST}:{settings.RABBITMQ_PORT}/{settings.RABBITMQ_VHOST}"
        )
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None

    async def get_channel(self) -> AbstractChannel:
        if self._channel and not self._channel.is_closed:
            return self._channel
        await self.connect()
        if self._channel is None:
            raise RuntimeError("Channel was not initialized after connection")
        return self._channel

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)

    async def close(self):
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
