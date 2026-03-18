import datetime
from unittest.mock import patch

import pytest
from django.test import TestCase, override_settings
from django.utils import timezone
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult


CUSTOM_SETTINGS = {
    "LEASE_RESET_BACKEND": "default",
    "LEASE_RESET_QUEUE": "maintenance",
}


@pytest.mark.django_db(transaction=True)
class TestLeaseModel(TestCase):
    """Tests for the TaskLease model that tracks lease expiry times."""

    def test_lease_created_on_claim(self):
        """When a task is claimed via the API, a TaskLease record is created."""
        from django_tasks_db_api.models import TaskLease
        from rest_framework.test import APIClient

        task = DBTaskResult.objects.create(
            args_kwargs={"args": ["world"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )

        client = APIClient()
        response = client.post(
            "/tasks/ready/",
            {"worker_id": "w1", "lease_seconds": 120},
            format="json",
        )
        self.assertEqual(response.status_code, 200)

        lease = TaskLease.objects.get(task_result_id=task.id)
        self.assertIsNotNone(lease.expires_at)
        # expires_at should be ~120 seconds from now
        expected_min = timezone.now() + datetime.timedelta(seconds=115)
        expected_max = timezone.now() + datetime.timedelta(seconds=125)
        self.assertGreaterEqual(lease.expires_at, expected_min)
        self.assertLessEqual(lease.expires_at, expected_max)

    def test_lease_not_created_when_no_task_claimed(self):
        """When no task is available, no lease is created."""
        from django_tasks_db_api.models import TaskLease
        from rest_framework.test import APIClient

        client = APIClient()
        response = client.post(
            "/tasks/ready/",
            {"worker_id": "w1", "lease_seconds": 120},
            format="json",
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(TaskLease.objects.count(), 0)


@pytest.mark.django_db(transaction=True)
class TestResetExpiredLeases(TestCase):
    """Tests for the task that resets expired leases back to READY."""

    def _create_running_task_with_lease(self, expires_at):
        from django_tasks_db_api.models import TaskLease

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

    def test_expired_lease_resets_task_to_ready(self):
        from django_tasks_db_api.lease import reset_expired_leases

        expired_at = timezone.now() - datetime.timedelta(seconds=60)
        task = self._create_running_task_with_lease(expires_at=expired_at)

        count = reset_expired_leases()

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.READY)
        self.assertIsNone(task.started_at)
        self.assertEqual(count, 1)

    def test_non_expired_lease_not_reset(self):
        from django_tasks_db_api.lease import reset_expired_leases

        future = timezone.now() + datetime.timedelta(seconds=300)
        task = self._create_running_task_with_lease(expires_at=future)

        count = reset_expired_leases()

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.RUNNING)
        self.assertEqual(count, 0)

    def test_already_finished_task_not_reset(self):
        """A task that completed before the lease expired should not be touched."""
        from django_tasks_db_api.models import TaskLease

        task = DBTaskResult.objects.create(
            args_kwargs={"args": [], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
            status=TaskResultStatus.SUCCESSFUL,
            started_at=timezone.now() - datetime.timedelta(seconds=600),
            finished_at=timezone.now() - datetime.timedelta(seconds=300),
        )
        expired_at = timezone.now() - datetime.timedelta(seconds=60)
        TaskLease.objects.create(task_result=task, expires_at=expired_at)

        from django_tasks_db_api.lease import reset_expired_leases
        count = reset_expired_leases()

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(count, 0)

    def test_lease_deleted_after_reset(self):
        from django_tasks_db_api.models import TaskLease
        from django_tasks_db_api.lease import reset_expired_leases

        expired_at = timezone.now() - datetime.timedelta(seconds=60)
        task = self._create_running_task_with_lease(expires_at=expired_at)

        reset_expired_leases()

        self.assertFalse(TaskLease.objects.filter(task_result_id=task.id).exists())

    def test_multiple_expired_leases_reset(self):
        from django_tasks_db_api.lease import reset_expired_leases

        expired_at = timezone.now() - datetime.timedelta(seconds=60)
        t1 = self._create_running_task_with_lease(expires_at=expired_at)
        t2 = self._create_running_task_with_lease(expires_at=expired_at)

        count = reset_expired_leases()

        self.assertEqual(count, 2)
        t1.refresh_from_db()
        t2.refresh_from_db()
        self.assertEqual(t1.status, TaskResultStatus.READY)
        self.assertEqual(t2.status, TaskResultStatus.READY)


@pytest.mark.django_db(transaction=True)
class TestLeaseSettings(TestCase):
    """Tests for configurable backend/queue settings for the reset task."""

    def test_default_settings(self):
        from django_tasks_db_api.conf import get_setting

        self.assertEqual(get_setting("LEASE_RESET_BACKEND"), "default")
        self.assertEqual(get_setting("LEASE_RESET_QUEUE"), "default")

    @override_settings(DJANGO_TASKS_DB_API=CUSTOM_SETTINGS)
    def test_custom_settings(self):
        from django_tasks_db_api.conf import get_setting

        self.assertEqual(get_setting("LEASE_RESET_BACKEND"), "default")
        self.assertEqual(get_setting("LEASE_RESET_QUEUE"), "maintenance")

    @override_settings(DJANGO_TASKS_DB_API={"LEASE_RESET_QUEUE": "ops"})
    def test_partial_override(self):
        from django_tasks_db_api.conf import get_setting

        self.assertEqual(get_setting("LEASE_RESET_BACKEND"), "default")
        self.assertEqual(get_setting("LEASE_RESET_QUEUE"), "ops")
