from collections.abc import Callable, Coroutine
from typing import Any

from src.workers.schemas import Request, Response
from src.workers.tasks import run_database_update

Task = Callable[..., Coroutine[Any, Any, dict[str, Any]]]


class Handler:
    _task_map: dict[str, Task] = {"update": run_database_update}

    def _resolve(self, name: str) -> Task:
        return self._task_map[name]

    async def process(self, message: Request) -> Response:
        handler = self._resolve(message.name)
        try:
            result = await handler(message.payload)
            return Response(id=message.id, done=True, payload=result)
        except Exception:
            return Response(id=message.id, done=False, payload={})
