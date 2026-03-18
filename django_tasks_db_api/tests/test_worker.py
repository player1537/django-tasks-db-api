import json
from unittest.mock import MagicMock, patch

import pytest
from django.test import TestCase, override_settings
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult


class TestAPIWorkerClient(TestCase):
    """Tests for the APIWorkerClient that communicates with the REST API."""

    def test_client_can_be_instantiated(self):
        from django_tasks_db_api.worker import APIWorkerClient

        client = APIWorkerClient(base_url="http://localhost:8000", worker_id="w1")
        self.assertEqual(client.base_url, "http://localhost:8000")
        self.assertEqual(client.worker_id, "w1")

    @patch("django_tasks_db_api.worker.requests.post")
    def test_claim_task_success(self, mock_post):
        from django_tasks_db_api.worker import APIWorkerClient

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "status": "RUNNING",
            "task_path": "myapp.tasks.sample_task",
            "args_kwargs": {"args": ["hello"], "kwargs": {}},
            "priority": 50,
            "queue_name": "default",
            "backend_name": "default",
        }
        mock_post.return_value = mock_response

        client = APIWorkerClient(base_url="http://localhost:8000", worker_id="w1")
        result = client.claim_task(lease_seconds=300)

        self.assertIsNotNone(result)
        self.assertEqual(result["id"], "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        mock_post.assert_called_once_with(
            "http://localhost:8000/tasks/ready/",
            json={"worker_id": "w1", "lease_seconds": 300},
            headers={},
            timeout=30,
        )

    @patch("django_tasks_db_api.worker.requests.post")
    def test_claim_task_none_available(self, mock_post):
        from django_tasks_db_api.worker import APIWorkerClient

        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response

        client = APIWorkerClient(base_url="http://localhost:8000", worker_id="w1")
        result = client.claim_task(lease_seconds=300)

        self.assertIsNone(result)

    @patch("django_tasks_db_api.worker.requests.post")
    def test_submit_success_result(self, mock_post):
        from django_tasks_db_api.worker import APIWorkerClient

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "SUCCESSFUL"}
        mock_post.return_value = mock_response

        client = APIWorkerClient(base_url="http://localhost:8000", worker_id="w1")
        client.submit_result(
            task_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            status="SUCCESSFUL",
            return_value="Hello, hello",
        )

        mock_post.assert_called_once_with(
            "http://localhost:8000/tasks/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/result/",
            json={
                "status": "SUCCESSFUL",
                "return_value": "Hello, hello",
            },
            headers={},
            timeout=30,
        )

    @patch("django_tasks_db_api.worker.requests.post")
    def test_submit_failure_result(self, mock_post):
        from django_tasks_db_api.worker import APIWorkerClient

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "FAILED"}
        mock_post.return_value = mock_response

        client = APIWorkerClient(base_url="http://localhost:8000", worker_id="w1")
        client.submit_result(
            task_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            status="FAILED",
            exception_class_path="builtins.ValueError",
            traceback="Traceback: ...",
        )

        mock_post.assert_called_once_with(
            "http://localhost:8000/tasks/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/result/",
            json={
                "status": "FAILED",
                "exception_class_path": "builtins.ValueError",
                "traceback": "Traceback: ...",
            },
            headers={},
            timeout=30,
        )


class TestAPIWorkerRun(TestCase):
    """Tests for the APIWorker run loop that claims, executes, and reports tasks."""

    @patch("django_tasks_db_api.worker.APIWorkerClient")
    def test_worker_executes_task_and_reports_success(self, MockClient):
        from django_tasks_db_api.worker import APIWorker

        mock_client = MockClient.return_value
        # First call returns a task, second returns None (to stop loop)
        mock_client.claim_task.side_effect = [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "status": "RUNNING",
                "task_path": "django_tasks_db_api.tests.test_tasks.sample_task",
                "args_kwargs": {"args": ["world"], "kwargs": {}},
            },
            None,
        ]

        worker = APIWorker(
            client=mock_client,
            batch=True,
            lease_seconds=300,
            interval=0,
        )
        worker.run()

        mock_client.submit_result.assert_called_once_with(
            task_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            status="SUCCESSFUL",
            return_value="Hello, world",
        )

    @patch("django_tasks_db_api.worker.APIWorkerClient")
    def test_worker_reports_failure_on_exception(self, MockClient):
        from django_tasks_db_api.worker import APIWorker

        mock_client = MockClient.return_value
        mock_client.claim_task.side_effect = [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "status": "RUNNING",
                "task_path": "django_tasks_db_api.tests.test_tasks.failing_task",
                "args_kwargs": {"args": [], "kwargs": {}},
            },
            None,
        ]

        worker = APIWorker(
            client=mock_client,
            batch=True,
            lease_seconds=300,
            interval=0,
        )
        worker.run()

        call_args = mock_client.submit_result.call_args
        self.assertEqual(call_args.kwargs["task_id"], "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        self.assertEqual(call_args.kwargs["status"], "FAILED")
        self.assertIn("builtins.ValueError", call_args.kwargs["exception_class_path"])
        self.assertIn("This task always fails", call_args.kwargs["traceback"])

    @patch("django_tasks_db_api.worker.APIWorkerClient")
    def test_worker_batch_mode_exits_when_no_tasks(self, MockClient):
        from django_tasks_db_api.worker import APIWorker

        mock_client = MockClient.return_value
        mock_client.claim_task.return_value = None

        worker = APIWorker(
            client=mock_client,
            batch=True,
            lease_seconds=300,
            interval=0,
        )
        worker.run()

        mock_client.claim_task.assert_called_once()
        mock_client.submit_result.assert_not_called()

    @patch("django_tasks_db_api.worker.APIWorkerClient")
    def test_worker_max_tasks_limit(self, MockClient):
        from django_tasks_db_api.worker import APIWorker

        mock_client = MockClient.return_value
        mock_client.claim_task.return_value = {
            "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "status": "RUNNING",
            "task_path": "django_tasks_db_api.tests.test_tasks.sample_task",
            "args_kwargs": {"args": ["world"], "kwargs": {}},
        }

        worker = APIWorker(
            client=mock_client,
            batch=False,
            lease_seconds=300,
            interval=0,
            max_tasks=2,
        )
        worker.run()

        self.assertEqual(mock_client.submit_result.call_count, 2)
