from django_tasks import task


@task()
def sample_task(name: str) -> str:
    return f"Hello, {name}"


@task()
def failing_task() -> None:
    raise ValueError("This task always fails")
