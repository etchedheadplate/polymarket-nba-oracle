import json
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika

from src.logger import logger
from src.queue.connection import RabbitMQConnection


class RabbitMQConsumer:
    def __init__(self, connection: RabbitMQConnection):
        self._connection = connection

    async def consume(
        self,
        exchange_name: str,
        routing_key: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ):
        channel = await self._connection.get_channel()
        exchange = await channel.declare_exchange(exchange_name, aio_pika.ExchangeType.TOPIC, durable=True)

        queue_name = f"{exchange_name}.{routing_key}"
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key)

        logger.info(f"SUB: queue={queue_name}")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    payload = json.loads(message.body)
                    logger.info(f" IN: queue={queue_name}")
                    logger.debug(f" IN: queue={queue_name}, message={payload}")
                    await callback(payload)
