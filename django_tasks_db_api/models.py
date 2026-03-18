from django.db import models
from django_tasks_db.models import DBTaskResult


class TaskLease(models.Model):
    task_result = models.OneToOneField(
        DBTaskResult,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="lease",
    )
    expires_at = models.DateTimeField(db_index=True)

    class Meta:
        verbose_name = "Task Lease"
        verbose_name_plural = "Task Leases"

    def __str__(self):
        return f"Lease for {self.task_result_id} expires {self.expires_at}"
