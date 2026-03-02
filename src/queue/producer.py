import json
from typing import Any

import aio_pika

from src.logger import logger
from src.queue.connection import RabbitMQConnection


class RabbitMQProducer:
    def __init__(self, connection: RabbitMQConnection):
        self._connection = connection

    async def send_message(self, exchange_name: str, routing_key: str, message: dict[str, Any]):
        channel = await self._connection.get_channel()
        exchange = await channel.declare_exchange(exchange_name, aio_pika.ExchangeType.TOPIC, durable=True)

        body = json.dumps(message).encode()
        await exchange.publish(
            aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=routing_key,
        )

        logger.info(f"OUT: queue={exchange_name}.{routing_key}")
        logger.debug(f"OUT: queue={exchange_name}.{routing_key}, message={message}")
