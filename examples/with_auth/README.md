# with_auth example

Demonstrates using `rest_framework_simplejwt` to require JWT authentication on all `django_tasks_db_api` endpoints.

## Setup

```bash
pip install django-tasks-db-api djangorestframework-simplejwt

cd examples/with_auth
python manage.py migrate
python manage.py createsuperuser --username admin --email admin@example.com
```

## Running the server

```bash
python manage.py runserver
```

## Getting a token

```bash
curl -X POST http://localhost:8000/api/token/ \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "yourpassword"}'
```

This returns an `access` and `refresh` token. Use the access token for API calls.

## Running the worker with auth

```bash
python manage.py db_api_worker \
  --api-url http://localhost:8000/api \
  --auth-token <access-token>
```

For long-running workers, the access token will eventually expire. See below for a worker that handles token refresh automatically.

## Worker with automatic token refresh

For production use, you can subclass `APIWorkerClient` to handle JWT refresh:

```python
import requests
from django_tasks_db_api.worker import APIWorker, APIWorkerClient


class JWTWorkerClient(APIWorkerClient):
    def __init__(self, *, base_url, worker_id, token_url, username, password):
        super().__init__(base_url=base_url, worker_id=worker_id)
        self.token_url = token_url
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self._authenticate()

    def _authenticate(self):
        response = requests.post(
            self.token_url,
            json={"username": self.username, "password": self.password},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        self.access_token = data["access"]
        self.refresh_token = data["refresh"]

    def get_headers(self):
        return {"Authorization": f"Bearer {self.access_token}"}


client = JWTWorkerClient(
    base_url="http://localhost:8000/api",
    worker_id="my-worker",
    token_url="http://localhost:8000/api/token/",
    username="admin",
    password="yourpassword",
)

worker = APIWorker(client=client)
worker.configure_signals()
worker.run()
```

You could extend `get_headers()` to check token expiry and call the refresh endpoint automatically.
