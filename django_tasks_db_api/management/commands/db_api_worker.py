import logging
from argparse import ArgumentParser

from django.core.management.base import BaseCommand
from django_tasks.utils import get_random_id

from django_tasks_db_api.worker import APIWorker, APIWorkerClient

logger = logging.getLogger("django_tasks_db_api")


class Command(BaseCommand):
    help = "Run a worker that polls the DB API for tasks, executes them, and reports results"

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--api-url",
            required=True,
            type=str,
            help="Base URL of the django_tasks_db_api server (e.g. http://localhost:8000)",
        )
        parser.add_argument(
            "--worker-id",
            type=str,
            default=get_random_id(),
            help="Unique worker identifier (default: auto-generated)",
        )
        parser.add_argument(
            "--lease-seconds",
            type=int,
            default=300,
            help="How long to claim a task for (default: 300)",
        )
        parser.add_argument(
            "--interval",
            type=float,
            default=1.0,
            help="Polling interval in seconds (default: 1.0)",
        )
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Process all available tasks then exit",
        )
        parser.add_argument(
            "--max-tasks",
            type=int,
            default=None,
            help="Maximum number of tasks to process before exiting",
        )
        parser.add_argument(
            "--auth-token",
            type=str,
            default=None,
            help="Bearer token for API authentication",
        )
        parser.add_argument(
            "--queue-name",
            type=str,
            default=None,
            help="Queue name to claim tasks from (optional, claims from any queue if not specified)",
        )

    def configure_logging(self, verbosity: int) -> None:
        if verbosity == 0:
            logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.DEBUG)

        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(self.stdout))

    def handle(self, *, verbosity: int, **options) -> None:
        self.configure_logging(verbosity)

        headers = {}
        if options["auth_token"]:
            headers["Authorization"] = f"Bearer {options['auth_token']}"

        client = APIWorkerClient(
            base_url=options["api_url"],
            worker_id=options["worker_id"],
            headers=headers,
        )

        worker = APIWorker(
            client=client,
            batch=options["batch"],
            lease_seconds=options["lease_seconds"],
            interval=options["interval"],
            max_tasks=options["max_tasks"],
            queue_name=options["queue_name"],
        )

        worker.configure_signals()
        worker.run()
