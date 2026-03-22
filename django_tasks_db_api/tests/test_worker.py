import json
from unittest.mock import MagicMock, patch

import pytest
from django.test import TestCase, override_settings
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult


class TestBackoffCalculation(TestCase):
    """Tests for the exponential backoff delay calculation."""

    def test_backoff_delay_grows_exponentially(self):
        from django_tasks_db_api.worker import calculate_backoff_delay

        # Mock random to control jitter for predictable testing
        with patch("django_tasks_db_api.worker.random.random") as mock_random:
            # Set random to return 0.0 so jitter range is 0.5-0.5 (minimum)
            mock_random.return_value = 0.0

            # Verify exponential growth: 1, 2, 4, 8, 16, 32... with jitter 0.5x
            # Attempt 0: 1 * (2^0) * 0.5 = 0.5
            delay0 = calculate_backoff_delay(0)
            self.assertAlmostEqual(delay0, 0.5)

            # Attempt 1: 1 * (2^1) * 0.5 = 1.0
            delay1 = calculate_backoff_delay(1)
            self.assertAlmostEqual(delay1, 1.0)

            # Attempt 2: 1 * (2^2) * 0.5 = 2.0
            delay2 = calculate_backoff_delay(2)
            self.assertAlmostEqual(delay2, 2.0)

            # Attempt 4: 1 * (2^4) * 0.5 = 8.0
            delay4 = calculate_backoff_delay(4)
            self.assertAlmostEqual(delay4, 8.0)

            # Attempt 5: min(30, 1 * 32) * 0.5 = 30 * 0.5 = 15.0
            delay5 = calculate_backoff_delay(5)
            self.assertAlmostEqual(delay5, 15.0)

            # Attempt 10: still capped at 30 * 0.5 = 15.0
            delay10 = calculate_backoff_delay(10)
            self.assertAlmostEqual(delay10, 15.0)

    def test_backoff_delay_respects_max_delay(self):
        from django_tasks_db_api.worker import calculate_backoff_delay

        with patch("django_tasks_db_api.worker.random.random") as mock_random:
            mock_random.return_value = 1.0  # Max jitter

            # Attempt 5 would be 32 seconds (before jitter), capped at 30
            # With jitter (0.5 + 1.0 = 1.5x), would be 45 but capped at max_delay=30
            delay = calculate_backoff_delay(5, initial_delay=1.0, max_delay=30.0)
            self.assertAlmostEqual(delay, 30.0)

            # Attempt 10 should still be capped at 30
            delay = calculate_backoff_delay(10, initial_delay=1.0, max_delay=30.0)
            self.assertAlmostEqual(delay, 30.0)

    def test_backoff_delay_includes_jitter(self):
        from django_tasks_db_api.worker import calculate_backoff_delay

        # Attempt 2 without jitter would be 4 seconds
        # With jitter between 0.5-1.5x (from 0.5 + random [0,1)), should be between 2-4 seconds
        # (capped at max_delay=30, but 4*1.5 < 30 so no cap applies)
        delays = []
        for _ in range(100):
            delay = calculate_backoff_delay(2)
            delays.append(delay)

        min_delay = min(delays)
        max_delay = max(delays)

        self.assertGreaterEqual(min_delay, 2.0)  # 4 * 0.5
        self.assertLess(max_delay, 6.0)  # 4 * 1.5 (random never returns exactly 1.0)


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
            params={},
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
            exception_class_path="",
            traceback="",
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

    @patch("django_tasks_db_api.worker.time.sleep")
    @patch("django_tasks_db_api.worker.APIWorkerClient")
    def test_worker_retries_claim_task_on_connection_failure(self, MockClient, mock_sleep):
        from django_tasks_db_api.worker import APIWorker
        import requests

        mock_client = MockClient.return_value
        # First two calls fail with connection error, third succeeds, then None to stop
        mock_client.claim_task.side_effect = [
            requests.ConnectionError("Connection failed"),
            requests.ConnectionError("Connection failed"),
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

        # Verify claim_task was called 4 times (2 failures + 1 success + 1 for exit)
        self.assertEqual(mock_client.claim_task.call_count, 4)
        # Verify task was submitted despite earlier connection failures
        self.assertEqual(mock_client.submit_result.call_count, 1)
        # Verify sleep was called for backoff (2 times for 2 failures)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("django_tasks_db_api.worker.time.sleep")
    @patch("django_tasks_db_api.worker.APIWorkerClient")
    def test_worker_retries_submit_result_on_connection_failure(self, MockClient, mock_sleep):
        from django_tasks_db_api.worker import APIWorker
        import requests

        mock_client = MockClient.return_value
        mock_client.claim_task.return_value = {
            "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "status": "RUNNING",
            "task_path": "django_tasks_db_api.tests.test_tasks.sample_task",
            "args_kwargs": {"args": ["world"], "kwargs": {}},
        }
        # First two submit_result calls fail, third succeeds
        mock_client.submit_result.side_effect = [
            requests.ConnectionError("Connection failed"),
            requests.ConnectionError("Connection failed"),
            None,
        ]
        # Only claim task once to avoid infinite loop
        mock_client.claim_task.side_effect = [mock_client.claim_task.return_value, None]

        worker = APIWorker(
            client=mock_client,
            batch=True,
            lease_seconds=300,
            interval=0,
        )
        worker.run()

        # Verify submit_result was called 3 times (2 failures + 1 success)
        self.assertEqual(mock_client.submit_result.call_count, 3)
        # Verify sleep was called for backoff (2 times for 2 failures)
        self.assertEqual(mock_sleep.call_count, 2)
