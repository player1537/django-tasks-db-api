import datetime
from unittest.mock import patch

import pytest
from django.test import TestCase, override_settings
from django.utils import timezone
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult
from rest_framework.test import APIClient

from django_tasks_db_api.models import TaskLease


@pytest.mark.django_db(transaction=True)
class TestResetSingleTaskLease(TestCase):
    """Tests for the django task that resets a specific expired task."""

    def test_resets_running_task_to_ready(self):
        """If the task is still RUNNING when the reset task fires, reset it."""
        from django_tasks_db_api.tasks import reset_single_task_lease

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
        TaskLease.objects.create(
            task_result=task,
            expires_at=timezone.now() - datetime.timedelta(seconds=60),
        )

        reset_single_task_lease.call(str(task.id))

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.READY)
        self.assertIsNone(task.started_at)
        self.assertFalse(TaskLease.objects.filter(task_result_id=task.id).exists())

    def test_does_not_reset_successful_task(self):
        """If the task already finished successfully, do nothing."""
        from django_tasks_db_api.tasks import reset_single_task_lease

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
        TaskLease.objects.create(
            task_result=task,
            expires_at=timezone.now() - datetime.timedelta(seconds=60),
        )

        reset_single_task_lease.call(str(task.id))

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.SUCCESSFUL)

    def test_does_not_reset_failed_task(self):
        """If the task already failed, do nothing."""
        from django_tasks_db_api.tasks import reset_single_task_lease

        task = DBTaskResult.objects.create(
            args_kwargs={"args": [], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
            status=TaskResultStatus.FAILED,
            started_at=timezone.now() - datetime.timedelta(seconds=600),
            finished_at=timezone.now() - datetime.timedelta(seconds=300),
        )

        reset_single_task_lease.call(str(task.id))

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.FAILED)

    def test_handles_nonexistent_task_gracefully(self):
        """If the task no longer exists, just return without error."""
        from django_tasks_db_api.tasks import reset_single_task_lease

        # Should not raise
        reset_single_task_lease.call("00000000-0000-0000-0000-000000000000")

    def test_is_a_django_task(self):
        """reset_single_task_lease should be decorated as a django task."""
        from django_tasks_db_api.tasks import reset_single_task_lease
        from django_tasks.base import Task

        self.assertIsInstance(reset_single_task_lease, Task)


@pytest.mark.django_db(transaction=True)
class TestAutoResetEnqueuedOnClaim(TestCase):
    """When a task is claimed, a reset_single_task_lease task should be enqueued."""

    def setUp(self):
        self.client = APIClient()

    def test_reset_task_enqueued_on_claim(self):
        """Claiming a task should enqueue a deferred reset_single_task_lease."""
        task = DBTaskResult.objects.create(
            args_kwargs={"args": ["world"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )

        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": "w1", "lease_seconds": 120},
            format="json",
        )
        self.assertEqual(response.status_code, 200)

        # A reset_single_task_lease DBTaskResult should have been enqueued
        reset_tasks = DBTaskResult.objects.filter(
            task_path="django_tasks_db_api.tasks.reset_single_task_lease",
        )
        self.assertEqual(reset_tasks.count(), 1)
        reset_task = reset_tasks.first()
        self.assertEqual(reset_task.args_kwargs["args"], [str(task.id)])
        # run_after should be ~120 seconds from now (the lease expiry)
        expected_min = timezone.now() + datetime.timedelta(seconds=115)
        expected_max = timezone.now() + datetime.timedelta(seconds=125)
        self.assertGreaterEqual(reset_task.run_after, expected_min)
        self.assertLessEqual(reset_task.run_after, expected_max)

    def test_no_reset_task_when_no_claim(self):
        """When no task is claimed, no reset task should be enqueued."""
        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": "w1", "lease_seconds": 120},
            format="json",
        )
        self.assertEqual(response.status_code, 204)

        reset_tasks = DBTaskResult.objects.filter(
            task_path="django_tasks_db_api.tasks.reset_single_task_lease",
        )
        self.assertEqual(reset_tasks.count(), 0)

    @override_settings(DJANGO_TASKS_DB_API={
        "LEASE_RESET_BACKEND": "default",
        "LEASE_RESET_QUEUE": "maintenance",
    })
    def test_reset_task_uses_configured_queue(self):
        """The enqueued reset task should use the configured queue."""
        task = DBTaskResult.objects.create(
            args_kwargs={"args": ["world"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )

        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": "w1", "lease_seconds": 120},
            format="json",
        )
        self.assertEqual(response.status_code, 200)

        reset_task = DBTaskResult.objects.get(
            task_path="django_tasks_db_api.tasks.reset_single_task_lease",
        )
        self.assertEqual(reset_task.queue_name, "maintenance")

    def test_reset_task_uses_default_backend(self):
        """By default, the reset task should use the 'default' backend."""
        task = DBTaskResult.objects.create(
            args_kwargs={"args": ["world"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )

        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": "w1", "lease_seconds": 120},
            format="json",
        )
        self.assertEqual(response.status_code, 200)

        reset_task = DBTaskResult.objects.get(
            task_path="django_tasks_db_api.tasks.reset_single_task_lease",
        )
        self.assertEqual(reset_task.backend_name, "default")
        self.assertEqual(reset_task.queue_name, "default")
