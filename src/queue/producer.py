import json
from typing import Any

import aio_pika

from src.queue.connection import RabbitMQConnection


class RabbitMQProducer:
    def __init__(self, connection: RabbitMQConnection):
        self.connection = connection

    async def send_message(self, exchange_name: str, routing_key: str, message: dict[str, Any]):
        if not self.connection.channel:
            await self.connection.connect()

        if self.connection.channel is None:
            raise RuntimeError("Channel was not initialized after connection")

        exchange = await self.connection.channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        body = json.dumps(message).encode()

        await exchange.publish(
            aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=routing_key,
        )
