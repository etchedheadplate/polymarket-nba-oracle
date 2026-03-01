import asyncio
import contextlib
import signal
from typing import Any

from src.config import settings
from src.queue.connection import RabbitMQConnection
from src.queue.consumer import RabbitMQConsumer
from src.queue.producer import RabbitMQProducer
from src.service.tasks import construct_game_schedule


async def main():
    connection = RabbitMQConnection()
    consumer = RabbitMQConsumer(connection, service_name=settings.SERVICE_NAME)
    producer = RabbitMQProducer(connection)

    stop_event = asyncio.Event()

    async def handle_message(msg: dict[str, Any]):
        result = await construct_game_schedule(msg["schedule_range"])
        await producer.send_message(
            settings.EXCHANGE_NAME,
            settings.RK_ORACLE_RESPONSE,
            result,
        )

    async def start_consumer():
        with contextlib.suppress(asyncio.CancelledError):
            await consumer.consume(
                settings.EXCHANGE_NAME,
                settings.RK_ORACLE_QUERY,
                handle_message,
            )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    tasks = [
        asyncio.create_task(start_consumer()),
    ]

    await stop_event.wait()

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
