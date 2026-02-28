from src.config import settings
from src.queue import RabbitMQConnection, RabbitMQProducer
from src.service.domain import ScheduleRange
from src.service.tasks import construct_game_schedule


async def get_games_schedule(connection: RabbitMQConnection, schedule_range: ScheduleRange):
    producer = RabbitMQProducer(connection)
    exchange = settings.EXCHANGE_NAME
    key = settings.RK_ORACLE_RESPONSE
    message = await construct_game_schedule(schedule_range)
    await producer.send_message(exchange, key, message)
