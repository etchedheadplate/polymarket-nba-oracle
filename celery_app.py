import os
import sys
from pathlib import Path

from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

load_dotenv()

sys.path.append(str(Path(__file__).resolve().parent))

app = Celery(
    "app",
    broker=os.getenv("REDIS_BROKER"),
    backend=os.getenv("REDIS_BACKEND"),
)

app.conf.update(  # pyright: ignore[reportUnknownMemberType]
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    timezone="UTC",
    enable_utc=True,
)


app.conf.beat_schedule = {  # pyright: ignore[reportUnknownMemberType]
    "daily-update": {
        "task": "tasks.update_database_task",
        "schedule": crontab(hour=3, minute=0),
    }
}


@app.task(  # pyright: ignore[reportUntypedFunctionDecorator, reportUnknownMemberType]
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def update_database_task(self: object) -> None:
    import asyncio

    from src.service.update import update_database  # pyright: ignore[reportAttributeAccessIssue]

    asyncio.run(update_database())
