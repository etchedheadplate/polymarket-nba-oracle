import json
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika

from src.logger import logger
from src.queue.connection import RabbitMQConnection


class RabbitMQConsumer:
    def __init__(self, connection: RabbitMQConnection, service_name: str):
        self._connection = connection
        self._service_name = service_name

    async def consume(
        self,
        exchange_name: str,
        routing_key: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ):
        channel = await self._connection.get_channel()
        exchange = await channel.declare_exchange(exchange_name, aio_pika.ExchangeType.TOPIC, durable=True)

        queue_name = f"{exchange_name}_{self._service_name}_{routing_key.replace('.', '_')}"
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key)

        logger.info(f"SUB: exchange={exchange_name}, queue={queue_name}, topic={routing_key}")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    payload = json.loads(message.body)
                    logger.info(f"IN: exchange={exchange_name}, routing_key={routing_key}, payload={payload}")
                    await callback(payload)
