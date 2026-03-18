import logging

from django.utils import timezone
from django_tasks.base import TaskResultStatus

from .models import TaskLease

logger = logging.getLogger("django_tasks_db_api")


def reset_expired_leases():
    """
    Find all expired leases on RUNNING tasks and reset them to READY.

    Returns the number of tasks reset.
    """
    now = timezone.now()
    expired_leases = TaskLease.objects.filter(
        expires_at__lte=now,
        task_result__status=TaskResultStatus.RUNNING,
    ).select_related("task_result")

    count = 0
    for lease in expired_leases:
        task = lease.task_result
        logger.info("Resetting expired lease for task %s", task.id)
        task.status = TaskResultStatus.READY
        task.started_at = None
        task.save(update_fields=["status", "started_at"])
        lease.delete()
        count += 1

    return count
