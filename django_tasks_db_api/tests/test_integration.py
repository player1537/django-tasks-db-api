"""Integration test: enqueue a task via the DB, claim via API, run, and report back."""
import pytest
from django.test import TestCase
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult
from rest_framework.test import APIClient


@pytest.mark.django_db(transaction=True)
class TestEndToEndWorkflow(TestCase):
    """Test the full workflow: enqueue -> claim -> execute -> report."""

    def setUp(self):
        self.client = APIClient()

    def test_full_task_lifecycle(self):
        # 1. A task gets enqueued (simulating what the Django app does)
        task = DBTaskResult.objects.create(
            args_kwargs={"args": ["integration"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )
        self.assertEqual(task.status, TaskResultStatus.READY)

        # 2. Worker claims the task via API
        claim_response = self.client.post(
            "/tasks/ready/",
            {"worker_id": "integration-worker", "lease_seconds": 300},
            format="json",
        )
        self.assertEqual(claim_response.status_code, 200)
        claimed = claim_response.json()
        self.assertEqual(claimed["id"], str(task.id))
        self.assertEqual(claimed["status"], TaskResultStatus.RUNNING)

        # 3. Verify task is now RUNNING in DB
        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.RUNNING)

        # 4. Worker executes the task locally and submits success
        # (In real life, the worker would import_string and call the task)
        from django_tasks_db_api.tests.test_tasks import sample_task
        result_value = sample_task.call(*claimed["args_kwargs"]["args"])

        result_response = self.client.post(
            f"/tasks/{task.id}/result/",
            {"status": "SUCCESSFUL", "return_value": result_value},
            format="json",
        )
        self.assertEqual(result_response.status_code, 200)

        # 5. Verify task is SUCCESSFUL in DB
        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(task.return_value, "Hello, integration")
        self.assertIsNotNone(task.finished_at)

    def test_full_task_failure_lifecycle(self):
        task = DBTaskResult.objects.create(
            args_kwargs={"args": [], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.failing_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )

        # Claim
        claim_response = self.client.post(
            "/tasks/ready/",
            {"worker_id": "integration-worker", "lease_seconds": 300},
            format="json",
        )
        self.assertEqual(claim_response.status_code, 200)

        # Submit failure
        result_response = self.client.post(
            f"/tasks/{task.id}/result/",
            {
                "status": "FAILED",
                "exception_class_path": "builtins.ValueError",
                "traceback": "ValueError: This task always fails",
            },
            format="json",
        )
        self.assertEqual(result_response.status_code, 200)

        task.refresh_from_db()
        self.assertEqual(task.status, TaskResultStatus.FAILED)
        self.assertEqual(task.exception_class_path, "builtins.ValueError")

    def test_no_double_claim(self):
        """Once a task is claimed, it shouldn't be claimable again."""
        DBTaskResult.objects.create(
            args_kwargs={"args": ["test"], "kwargs": {}},
            priority=50,
            task_path="django_tasks_db_api.tests.test_tasks.sample_task",
            queue_name="default",
            backend_name="default",
            run_after="9999-01-01T00:00:00Z",
        )

        # First claim succeeds
        r1 = self.client.post(
            "/tasks/ready/",
            {"worker_id": "worker-1", "lease_seconds": 300},
            format="json",
        )
        self.assertEqual(r1.status_code, 200)

        # Second claim finds no tasks
        r2 = self.client.post(
            "/tasks/ready/",
            {"worker_id": "worker-2", "lease_seconds": 300},
            format="json",
        )
        self.assertEqual(r2.status_code, 204)
