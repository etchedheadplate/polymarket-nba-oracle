import asyncio
import contextlib
import signal
from typing import Any

from src.config import settings
from src.queue import RabbitMQConnection, RabbitMQConsumer, RabbitMQProducer
from src.workers import Handler, Request


async def main():
    connection = RabbitMQConnection()
    consumer = RabbitMQConsumer(connection)
    producer = RabbitMQProducer(connection)

    stop_event = asyncio.Event()

    async def handle_message(msg: dict[str, Any]) -> None:
        message = Request(**msg)
        response = await Handler().process(message)
        await producer.send_message(
            exchange_name=settings.EXCHANGE_NAME,
            routing_key=f"{settings.QUEUE_TG_BOT}.{settings.RK_RESPONSE}",
            message=response.model_dump(),
        )

    async def start_consumer():
        with contextlib.suppress(asyncio.CancelledError):
            await consumer.consume(
                exchange_name=settings.EXCHANGE_NAME,
                routing_key=f"{settings.QUEUE_ORACLE}.{settings.RK_REQUEST}",
                callback=handle_message,
            )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    tasks = [asyncio.create_task(start_consumer())]

    await stop_event.wait()

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
