import datetime

import pytest
from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult

from django_tasks_db_api.models import TaskLease


@pytest.mark.django_db(transaction=True)
class TestClearExpiredLeasesCommand(TestCase):
    """Tests for the clear_expired_leases management command."""

    def _create_running_task_with_lease(self, expires_at):
        task = DBTaskResult.objects.create(
            args_kwargs={"args": [], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
            status=TaskResultStatus.RUNNING,
            started_at=timezone.now() - datetime.timedelta(seconds=600),
        )
        TaskLease.objects.create(task_result=task, expires_at=expires_at)
        return task

    def test_command_resets_expired_leases(self):
        expired = timezone.now() - datetime.timedelta(seconds=60)
        t1 = self._create_running_task_with_lease(expires_at=expired)
        t2 = self._create_running_task_with_lease(expires_at=expired)

        call_command("clear_expired_leases")

        t1.refresh_from_db()
        t2.refresh_from_db()
        self.assertEqual(t1.status, TaskResultStatus.READY)
        self.assertEqual(t2.status, TaskResultStatus.READY)

    def test_command_does_not_reset_active_leases(self):
        future = timezone.now() + datetime.timedelta(seconds=300)
        task = self._create_running_task_with_lease(expires_at=future)

        call_command("clear_expired_leases")

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.RUNNING)

    def test_command_outputs_count(self):
        from io import StringIO

        expired = timezone.now() - datetime.timedelta(seconds=60)
        self._create_running_task_with_lease(expires_at=expired)

        out = StringIO()
        call_command("clear_expired_leases", stdout=out)

        self.assertIn("1", out.getvalue())
