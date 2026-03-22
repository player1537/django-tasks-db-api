from __future__ import annotations

import logging
import random
import signal
import sys
import time
import traceback as tb_module
from datetime import datetime
from types import FrameType

import requests
from django.utils.module_loading import import_string

logger = logging.getLogger("django_tasks_db_api")


def calculate_backoff_delay(
    attempt: int, initial_delay: float = 1.0, max_delay: float = 30.0
) -> float:
    """Calculate exponential backoff delay with jitter.

    Args:
        attempt: The attempt number (0-indexed)
        initial_delay: Initial delay in seconds (default 1.0)
        max_delay: Maximum delay in seconds (default 30.0)

    Returns:
        Delay in seconds with random jitter applied
    """
    # Exponential backoff: 1, 2, 4, 8, 16, 32... seconds
    delay = min(max_delay, initial_delay * (2 ** attempt))
    # Add jitter: randomize between 50-100% of calculated delay
    # Cap result to max_delay to ensure we don't exceed the maximum
    jittered_delay = min(max_delay, delay * (0.5 + random.random()))
    return jittered_delay


class APIWorkerClient:
    """HTTP client that communicates with the django_tasks_db_api REST endpoints."""

    def __init__(
        self, *, base_url: str, worker_id: str, headers: dict[str, str] | None = None
    ):
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.headers = headers or {}

    def get_headers(self) -> dict[str, str]:
        """Return headers for the next request. Override for dynamic auth (e.g. JWT refresh)."""
        return dict(self.headers)

    def claim_task(self, *, queue_name: str | None = None, lease_seconds: int = 300) -> dict | None:
        url = f"{self.base_url}/tasks/ready/"
        params = {}
        if queue_name:
            params["queue_name"] = queue_name

        response = requests.post(
            url,
            json={"worker_id": self.worker_id, "lease_seconds": lease_seconds},
            params=params,
            headers=self.get_headers(),
            timeout=30,
        )
        if response.status_code == 204:
            return None
        response.raise_for_status()
        return response.json()

    def enqueue_task(
        self,
        *,
        task_path: str,
        args_kwargs: dict | None = None,
        priority: int = 0,
        queue_name: str = "default",
        backend_name: str = "default",
        run_after: "datetime | None" = None,
    ) -> dict:
        from datetime import datetime

        payload: dict = {
            "task_path": task_path,
            "args_kwargs": args_kwargs or {"args": [], "kwargs": {}},
            "priority": priority,
            "queue_name": queue_name,
            "backend_name": backend_name,
        }
        if run_after is not None:
            payload["run_after"] = run_after.isoformat()

        response = requests.post(
            f"{self.base_url}/tasks/",
            json=payload,
            headers=self.get_headers(),
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def submit_result(
        self,
        *,
        task_id: str,
        status: str,
        return_value=None,
        exception_class_path: str = "",
        traceback: str = "",
    ) -> None:
        payload: dict = {"status": status}
        if status == "SUCCESSFUL":
            payload["return_value"] = return_value
        else:
            payload["exception_class_path"] = exception_class_path
            payload["traceback"] = traceback

        response = requests.post(
            f"{self.base_url}/tasks/{task_id}/result/",
            json=payload,
            headers=self.get_headers(),
            timeout=30,
        )
        response.raise_for_status()


class APIWorker:
    """Worker that polls the DB API for tasks, runs them locally, and reports results."""

    def __init__(
        self,
        *,
        client: APIWorkerClient,
        batch: bool = False,
        lease_seconds: int = 300,
        interval: float = 1.0,
        max_tasks: int | None = None,
        queue_name: str | None = None,
    ):
        self.client = client
        self.batch = batch
        self.lease_seconds = lease_seconds
        self.interval = interval
        self.max_tasks = max_tasks
        self.queue_name = queue_name
        self.running = True
        self._run_tasks = 0

    def shutdown(self, signum: int, frame: FrameType | None) -> None:
        if not self.running:
            logger.warning(
                "Received %s - terminating immediately.", signal.strsignal(signum)
            )
            sys.exit(1)

        logger.warning(
            "Received %s - shutting down gracefully...", signal.strsignal(signum)
        )
        self.running = False

    def configure_signals(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def run(self) -> None:
        logger.info("Starting API worker")

        while self.running:
            task_data = self._claim_task_with_backoff()

            if task_data is None:
                if self.batch:
                    logger.info("No more tasks - exiting (batch mode).")
                    return
                if self.running:
                    time.sleep(self.interval)
                continue

            self.run_task(task_data)

            if self.max_tasks is not None and self._run_tasks >= self.max_tasks:
                logger.info("Reached max tasks (%d) - exiting.", self._run_tasks)
                return

    def _claim_task_with_backoff(self) -> dict | None:
        """Claim a task with exponential backoff on connection failures.

        Returns:
            Task data dict if a task is available, None if no tasks, or keeps retrying on error.
        """
        attempt = 0
        while True:
            try:
                return self.client.claim_task(
                    queue_name=self.queue_name,
                    lease_seconds=self.lease_seconds,
                )
            except requests.RequestException as exc:
                delay = calculate_backoff_delay(attempt)
                logger.warning(
                    "Failed to claim task (attempt %d), retrying in %.1f seconds: %s",
                    attempt + 1,
                    delay,
                    exc,
                )
                if not self.running:
                    raise
                time.sleep(delay)
                attempt += 1

    def run_task(self, task_data: dict) -> None:
        task_id = task_data["id"]
        task_path = task_data["task_path"]
        args_kwargs = task_data["args_kwargs"]

        logger.info("Running task %s (%s)", task_id, task_path)

        try:
            task_func = import_string(task_path)
            result = task_func.call(
                *args_kwargs.get("args", []),
                **args_kwargs.get("kwargs", {}),
            )
            self._submit_result_with_backoff(
                task_id=task_id,
                status="SUCCESSFUL",
                return_value=result,
            )
        except Exception as exc:
            self._submit_result_with_backoff(
                task_id=task_id,
                status="FAILED",
                exception_class_path=f"{type(exc).__module__}.{type(exc).__qualname__}",
                traceback=tb_module.format_exc(),
            )
        finally:
            self._run_tasks += 1

    def _submit_result_with_backoff(
        self,
        *,
        task_id: str,
        status: str,
        return_value=None,
        exception_class_path: str = "",
        traceback: str = "",
    ) -> None:
        """Submit task result with exponential backoff on connection failures."""
        attempt = 0
        while True:
            try:
                self.client.submit_result(
                    task_id=task_id,
                    status=status,
                    return_value=return_value,
                    exception_class_path=exception_class_path,
                    traceback=traceback,
                )
                return
            except requests.RequestException as exc:
                delay = calculate_backoff_delay(attempt)
                logger.warning(
                    "Failed to submit result for task %s (attempt %d), retrying in %.1f seconds: %s",
                    task_id,
                    attempt + 1,
                    delay,
                    exc,
                )
                if not self.running:
                    raise
                time.sleep(delay)
                attempt += 1
