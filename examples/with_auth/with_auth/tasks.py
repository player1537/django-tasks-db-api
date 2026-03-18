from django_tasks import task


@task()
def greet(name: str) -> str:
    """A simple example task that greets someone."""
    return f"Hello, {name}!"


@task()
def add(a: int, b: int) -> int:
    """An example task that adds two numbers."""
    return a + b
