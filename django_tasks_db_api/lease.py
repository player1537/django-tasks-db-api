import logging

from django.db import transaction
from django.utils import timezone
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult

from .models import TaskLease

logger = logging.getLogger("django_tasks_db_api")


def reset_expired_leases():
    """
    Find all expired leases on RUNNING tasks and reset them to READY.

    Each task is locked individually to avoid races with concurrent
    result submissions.

    Returns the number of tasks reset.
    """
    now = timezone.now()
    expired_task_ids = list(
        TaskLease.objects.filter(
            expires_at__lte=now,
            task_result__status=TaskResultStatus.RUNNING,
        ).values_list("task_result_id", flat=True)
    )

    count = 0
    for task_id in expired_task_ids:
        with transaction.atomic():
            try:
                task = DBTaskResult.objects.select_for_update().get(pk=task_id)
            except DBTaskResult.DoesNotExist:
                TaskLease.objects.filter(task_result_id=task_id).delete()
                continue

            if task.status != TaskResultStatus.RUNNING:
                # Task completed between our query and the lock — skip it
                TaskLease.objects.filter(task_result_id=task_id).delete()
                continue

            logger.info("Resetting expired lease for task %s", task.id)
            task.status = TaskResultStatus.READY
            task.started_at = None
            task.save(update_fields=["status", "started_at"])
            TaskLease.objects.filter(task_result_id=task_id).delete()
            count += 1

    return count
