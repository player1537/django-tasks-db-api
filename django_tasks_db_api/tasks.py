import logging

from django.db import transaction
from django_tasks import task
from django_tasks.base import TaskResultStatus

logger = logging.getLogger("django_tasks_db_api")


@task()
def reset_single_task_lease(task_id: str) -> None:
    """
    Reset a specific task from RUNNING back to READY if the lease has expired.

    This task is automatically enqueued with run_after=lease expiry when a
    task is claimed via the API. When it fires, if the original task is still
    RUNNING, it gets reset so another worker can pick it up.
    """
    from django_tasks_db.models import DBTaskResult

    from .models import TaskLease

    with transaction.atomic():
        try:
            db_task = DBTaskResult.objects.select_for_update().get(pk=task_id)
        except DBTaskResult.DoesNotExist:
            logger.warning("Task %s no longer exists, skipping lease reset.", task_id)
            return

        if db_task.status != TaskResultStatus.RUNNING:
            logger.info(
                "Task %s is %s (not RUNNING), skipping lease reset.",
                task_id,
                db_task.status,
            )
            TaskLease.objects.filter(task_result_id=task_id).delete()
            return

        logger.info("Resetting expired lease for task %s", task_id)
        db_task.status = TaskResultStatus.READY
        db_task.started_at = None
        db_task.save(update_fields=["status", "started_at"])
        TaskLease.objects.filter(task_result_id=task_id).delete()
