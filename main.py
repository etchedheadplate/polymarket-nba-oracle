import asyncio
import signal
from typing import Any

from src.config import settings
from src.logger import logger
from src.queue import RabbitMQConnection, RabbitMQConsumer


async def run_rabbit_consumer(stop_event: asyncio.Event):
    rabbit_connection = RabbitMQConnection()
    consumer = RabbitMQConsumer(rabbit_connection)

    ROUTING_KEYS_STATUS = [
        settings.ROUTING_KEY_QUERY_DB,
    ]

    async def handle_message(message: dict[str, Any], routing_key: str):
        logger.info(f"IN: key={routing_key}, message={message}")

    tasks: list[asyncio.Task[Any]] = []

    for routing_key in ROUTING_KEYS_STATUS:
        task = asyncio.create_task(
            consumer.consume(
                settings.EXCHANGE_NAME,
                routing_key,
                lambda msg, rk=routing_key: handle_message(msg, rk),
            )
        )
        tasks.append(task)

    await stop_event.wait()

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    await rabbit_connection.close()
    logger.info("Disconnected from RabbitMQ")


async def main():
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await run_rabbit_consumer(stop_event)


if __name__ == "__main__":
    asyncio.run(main())
