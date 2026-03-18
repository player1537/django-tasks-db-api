import uuid

import pytest
from django.test import TestCase, override_settings
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult
from rest_framework.test import APIClient


@pytest.mark.django_db(transaction=True)
class TestTaskClaimView(TestCase):
    """Tests for POST /tasks/ready/ - claim the next ready task."""

    def setUp(self):
        self.client = APIClient()
        self.worker_id = "test-worker-1"

    def _create_ready_task(self, task_path="django_tasks_db_api.tests.test_tasks.sample_task", **kwargs):
        defaults = {
            "args_kwargs": {"args": ["world"], "kwargs": {}},
            "priority": 50,
            "task_path": task_path,
            "queue_name": "default",
            "backend_name": "default",
            "run_after": "9999-01-01T00:00:00Z",
        }
        defaults.update(kwargs)
        return DBTaskResult.objects.create(**defaults)

    def test_claim_returns_200_with_task_data(self):
        """Claiming a ready task should return 200 with task details."""
        task = self._create_ready_task()
        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": self.worker_id, "lease_seconds": 300},
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["id"], str(task.id))
        self.assertEqual(data["status"], TaskResultStatus.RUNNING)
        self.assertEqual(data["task_path"], "django_tasks_db_api.tests.test_tasks.sample_task")
        self.assertEqual(data["args_kwargs"], {"args": ["world"], "kwargs": {}})

    def test_claim_marks_task_as_running(self):
        """Claiming a task should transition it to RUNNING status in the DB."""
        task = self._create_ready_task()
        self.client.post(
            "/tasks/ready/",
            {"worker_id": self.worker_id, "lease_seconds": 300},
            format="json",
        )
        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.RUNNING)
        self.assertIn(self.worker_id, task.worker_ids)

    def test_claim_returns_204_when_no_tasks(self):
        """When no ready tasks exist, should return 204 No Content."""
        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": self.worker_id, "lease_seconds": 300},
            format="json",
        )
        self.assertEqual(response.status_code, 204)

    def test_claim_respects_queue_filter(self):
        """Should only claim tasks from the specified queue."""
        self._create_ready_task(queue_name="other-queue")
        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": self.worker_id, "lease_seconds": 300, "queue_name": "default"},
            format="json",
        )
        self.assertEqual(response.status_code, 204)

    def test_claim_respects_priority_ordering(self):
        """Higher priority tasks should be claimed first."""
        low = self._create_ready_task(priority=10)
        high = self._create_ready_task(priority=90)
        response = self.client.post(
            "/tasks/ready/",
            {"worker_id": self.worker_id, "lease_seconds": 300},
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["id"], str(high.id))

    def test_claim_requires_worker_id(self):
        """POST without worker_id should return 400."""
        self._create_ready_task()
        response = self.client.post(
            "/tasks/ready/",
            {"lease_seconds": 300},
            format="json",
        )
        self.assertEqual(response.status_code, 400)


@pytest.mark.django_db(transaction=True)
class TestTaskResultView(TestCase):
    """Tests for POST /tasks/<uuid>/result/ - submit task completion."""

    def setUp(self):
        self.client = APIClient()

    def _create_running_task(self):
        task = DBTaskResult.objects.create(
            args_kwargs={"args": ["world"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
            status=TaskResultStatus.RUNNING,
        )
        return task

    def test_submit_success_result(self):
        """Submitting a successful result should mark task SUCCESSFUL."""
        task = self._create_running_task()
        response = self.client.post(
            f"/tasks/{task.id}/result/",
            {
                "status": "SUCCESSFUL",
                "return_value": "Hello, world",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(task.return_value, "Hello, world")
        self.assertIsNotNone(task.finished_at)

    def test_submit_failure_result(self):
        """Submitting a failed result should mark task FAILED."""
        task = self._create_running_task()
        response = self.client.post(
            f"/tasks/{task.id}/result/",
            {
                "status": "FAILED",
                "exception_class_path": "builtins.ValueError",
                "traceback": "Traceback: ...",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.FAILED)
        self.assertEqual(task.exception_class_path, "builtins.ValueError")
        self.assertIsNotNone(task.finished_at)

    def test_submit_result_for_nonexistent_task(self):
        """Submitting result for non-existent task should return 404."""
        fake_id = uuid.uuid4()
        response = self.client.post(
            f"/tasks/{fake_id}/result/",
            {
                "status": "SUCCESSFUL",
                "return_value": "test",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 404)

    def test_submit_result_for_non_running_task(self):
        """Cannot submit result for a task that's not RUNNING."""
        task = DBTaskResult.objects.create(
            args_kwargs={"args": [], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
            status=TaskResultStatus.SUCCESSFUL,
        )
        response = self.client.post(
            f"/tasks/{task.id}/result/",
            {
                "status": "SUCCESSFUL",
                "return_value": "test",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 409)


@pytest.mark.django_db(transaction=True)
class TestTaskDetailView(TestCase):
    """Tests for GET /tasks/<uuid>/ - get task details."""

    def setUp(self):
        self.client = APIClient()

    def test_get_task_detail(self):
        """Should return task details."""
        task = DBTaskResult.objects.create(
            args_kwargs={"args": ["world"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )
        response = self.client.get(f"/tasks/{task.id}/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["id"], str(task.id))
        self.assertEqual(data["status"], TaskResultStatus.READY)
        self.assertEqual(data["task_path"], "django_tasks_db_api.tests.test_tasks.sample_task")

    def test_get_nonexistent_task(self):
        """Should return 404 for non-existent task."""
        fake_id = uuid.uuid4()
        response = self.client.get(f"/tasks/{fake_id}/")
        self.assertEqual(response.status_code, 404)
