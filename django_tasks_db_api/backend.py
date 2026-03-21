from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.base import Task, TaskResult, TaskResultStatus
from django_tasks.utils import get_module_path
from typing_extensions import ParamSpec

from .worker import APIWorkerClient

T = TypeVar("T")
P = ParamSpec("P")


class APIBackend(BaseTaskBackend):
    """Django Tasks backend that enqueues tasks via the DB API over HTTP.

    Configure in settings::

        TASKS = {
            "default": {
                "BACKEND": "django_tasks_db_api.backend.APIBackend",
                "OPTIONS": {
                    "base_url": "http://my-server:8000",
                    "worker_id": "my-worker",
                    # Optional:
                    "headers": {"Authorization": "Bearer ..."},
                },
            }
        }
    """

    supports_defer = True
    supports_priority = True

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias, params)

        self.client = APIWorkerClient(
            base_url=self.options["base_url"],
            worker_id=self.options.get("worker_id", "api-backend"),
            headers=self.options.get("headers"),
        )

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        data = self.client.enqueue_task(
            task_path=get_module_path(task.func),
            args_kwargs={"args": list(args), "kwargs": dict(kwargs)},
            priority=task.priority,
            queue_name=task.queue_name,
            backend_name=self.alias,
            run_after=task.run_after,
        )

        return TaskResult(
            task=task,
            id=data["id"],
            status=TaskResultStatus[data["status"]],
            enqueued_at=_parse_dt(data.get("enqueued_at")),
            started_at=_parse_dt(data.get("started_at")),
            finished_at=_parse_dt(data.get("finished_at")),
            last_attempted_at=None,
            args=list(args),
            kwargs=dict(kwargs),
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )


def _parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    from django.utils.dateparse import parse_datetime
    return parse_datetime(value)
