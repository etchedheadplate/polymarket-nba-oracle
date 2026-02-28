import asyncio
import signal
from typing import Any

from src.config import settings
from src.queue.connection import RabbitMQConnection
from src.queue.consumer import RabbitMQConsumer
from src.queue.producer import RabbitMQProducer
from src.service.tasks import construct_game_schedule


async def main():
    connection = RabbitMQConnection()
    consumer = RabbitMQConsumer(connection, service_name="nba-oracle")
    producer = RabbitMQProducer(connection)

    async def handle_message(msg: dict[str, Any]):
        schedule_range = msg["schedule_range"]
        result = await construct_game_schedule(schedule_range)
        await producer.send_message(
            exchange_name=settings.EXCHANGE_NAME, routing_key=settings.RK_ORACLE_RESPONSE, message=result
        )

    await consumer.consume(
        exchange_name=settings.EXCHANGE_NAME, routing_key=settings.RK_ORACLE_QUERY, callback=handle_message
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    task = asyncio.create_task(consumer.consume(settings.EXCHANGE_NAME, settings.RK_ORACLE_QUERY, handle_message))

    await stop_event.wait()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
