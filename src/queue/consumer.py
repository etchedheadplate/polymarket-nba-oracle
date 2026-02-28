import json
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika

from src.config import Settings
from src.logger import logger
from src.queue.connection import RabbitMQConnection


class RabbitMQConsumer:
    def __init__(self, connection: RabbitMQConnection):
        settings = Settings()  # pyright: ignore[reportCallIssue]
        self.connection = connection
        self.service_name = settings.SERVICE_NAME

    async def consume(self, exchange_name: str, routing_key: str, callback: Callable[[Any], Awaitable[None]]):
        if not self.connection.channel:
            await self.connection.connect()

        if self.connection.channel is None:
            raise RuntimeError("Channel was not initialized after connection")

        exchange = await self.connection.channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        queue_suffix = routing_key.split(".")[-1] if "." in routing_key else routing_key
        queue_name = f"{exchange_name}_{self.service_name}_{queue_suffix}"
        queue = await self.connection.channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key)

        logger.info(f"SUB: exchange={exchange_name}, queue={queue_name}, topic={routing_key}")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    payload = json.loads(message.body)
                    await callback(payload)
