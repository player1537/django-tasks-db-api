# WARNING

As of the current (2026-03-18) commit (`5229a64`), everything in this project is entirely AI-generated, with the exception of this section of the README.

This solves a specific problem I have and is intended to be reusable. The kind of work that this package is doing is really not that interesting, I just needed it to exist so that I could build a part of my current project. I figured this would be useful enough that others could also use it.

I don't have any current plans to push this to PyPI. Instead, I think the best way to install it is to pip install from the git repository directly, or else to add it as a submodule.


# django-tasks-db-api

A Django Rest Framework API on top of [django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db). Its primary purpose is to enable a `manage.py db_api_worker` command that polls the API for new jobs, claims them for a fixed amount of time, runs the task, and sends the result back via the API.

This decouples workers from the database — workers only need HTTP access to the API server, not direct database credentials.


## Installation

### Server Installation

For a full server setup with Django and djangorestframework:

```
pip install "django-tasks-db-api[server]"
```

Add to `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...
    "rest_framework",
    "django_tasks_db",
    "django_tasks_db_api",
]
```

Include the URLs:

```python
from django.urls import include, path

urlpatterns = [
    path("api/", include("django_tasks_db_api.urls")),
]
```

Run migrations (for the `TaskLease` model):

```
manage.py migrate django_tasks_db_api
```

### Client-Only Installation

If you only need the `APIWorkerClient` class for a standalone worker (no Django server):

```
pip install django-tasks-db-api
```

This installs only the minimal dependencies (`requests`). The client is a plain Python class with no Django dependencies.

## API Endpoints

### `POST /tasks/ready/` — Claim a task

Atomically claims the highest-priority ready task and returns it. The task is marked as RUNNING and a lease is created. If no tasks are available, returns 204.

**Request:**

```json
{
    "worker_id": "my-worker-1",
    "lease_seconds": 300,
    "queue_name": "default",
    "backend_name": "default"
}
```

`worker_id` is required. All other fields are optional.

### `POST /queue/<queue_name>/tasks/ready/` — Claim a task from a specific queue

Same as above, but the queue name is specified in the URL path instead of the request body. Useful when workers are dedicated to a single queue.

**Request:**

```json
{
    "worker_id": "my-worker-1",
    "lease_seconds": 300,
    "backend_name": "default"
}
```

`worker_id` is required. `queue_name` is taken from the URL path.

**Response (200):**

```json
{
    "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "status": "RUNNING",
    "task_path": "myapp.tasks.my_task",
    "args_kwargs": {"args": ["hello"], "kwargs": {}},
    "priority": 50,
    "queue_name": "default",
    "backend_name": "default",
    "enqueued_at": "2025-01-01T00:00:00Z",
    "started_at": "2025-01-01T00:01:00Z",
    "finished_at": null,
    "return_value": null
}
```

### `POST /tasks/<uuid>/result/` — Submit a result

Report the outcome of a task. The task must be in RUNNING state or you'll get 409.

**Success:**

```json
{
    "status": "SUCCESSFUL",
    "return_value": "Hello, world"
}
```

**Failure:**

```json
{
    "status": "FAILED",
    "exception_class_path": "builtins.ValueError",
    "traceback": "Traceback (most recent call last): ..."
}
```

### `GET /tasks/<uuid>/` — Get task details

Returns the current state of a task.

## Management Commands

### `db_api_worker`

Polls the API for tasks, runs them locally, and reports results back.

```
manage.py db_api_worker --api-url http://localhost:8000/api
```

Options:

- `--api-url` (required) — Base URL of the API server.
- `--worker-id` — Unique worker ID. Auto-generated if not set.
- `--lease-seconds` — How long to hold a claimed task (default: 300).
- `--interval` — Seconds between polls when idle (default: 1).
- `--batch` — Process all available tasks, then exit.
- `--max-tasks` — Exit after processing this many tasks.

### `clear_expired_leases`

Bulk-reset all tasks whose lease has expired from RUNNING back to READY.

```
manage.py clear_expired_leases
```

Useful as a cron job or manual fallback. Note that expired leases are also reset automatically — when a task is claimed, a deferred `reset_single_task_lease` django task is enqueued to fire after the lease expires.

## Settings

Configure the backend and queue used by the automatic lease-reset task:

```python
DJANGO_TASKS_DB_API = {
    "LEASE_RESET_BACKEND": "default",
    "LEASE_RESET_QUEUE": "default",
}
```

Both default to `"default"`. The lease-reset task is a normal `django-tasks` task, so it will be picked up by whatever `db_worker` is processing that backend/queue.

## Writing a Standalone Worker

The `db_api_worker` command is built on two classes you can use directly: `APIWorkerClient` and `APIWorker`.

### Using `APIWorkerClient` alone

If you want full control over the poll loop (or you're not using Django on the worker side), use the client directly:

```python
from django_tasks_db_api.worker import APIWorkerClient

client = APIWorkerClient(base_url="http://localhost:8000/api", worker_id="my-worker")

# Claim a task
task = client.claim_task(lease_seconds=300)
if task is not None:
    print(task["task_path"], task["args_kwargs"])

    # ... do the work ...

    # Report success
    client.submit_result(
        task_id=task["id"],
        status="SUCCESSFUL",
        return_value="result here",
    )

    # Or report failure
    client.submit_result(
        task_id=task["id"],
        status="FAILED",
        exception_class_path="builtins.RuntimeError",
        traceback="...",
    )
```

`APIWorkerClient` is a plain Python class that uses `requests`. It has no Django dependency beyond what's needed to import it.

### Using `APIWorker` for a managed loop

If you want the poll loop, signal handling, batch mode, and max-tasks behavior but want to customize something:

```python
from django_tasks_db_api.worker import APIWorker, APIWorkerClient

client = APIWorkerClient(base_url="http://localhost:8000/api", worker_id="my-worker")
worker = APIWorker(
    client=client,
    lease_seconds=600,
    interval=2.0,
    batch=False,
    max_tasks=100,
)
worker.configure_signals()
worker.run()
```

You can subclass `APIWorker` and override `run_task()` to change how tasks are executed — for example, to run them in a subprocess or container instead of calling `import_string` directly.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
