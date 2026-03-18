import datetime

from django.db import transaction
from django.utils import timezone
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .conf import get_setting
from .models import TaskLease
from .serializers import (
    DBTaskResultSerializer,
    TaskClaimRequestSerializer,
    TaskResultSubmitSerializer,
)


class TaskClaimView(APIView):
    """POST /tasks/ready/ - Claim the next ready task for a worker."""

    def post(self, request):
        serializer = TaskClaimRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        worker_id = serializer.validated_data["worker_id"]
        queue_name = serializer.validated_data.get("queue_name")
        backend_name = serializer.validated_data.get("backend_name", "default")

        tasks = DBTaskResult.objects.ready().filter(backend_name=backend_name)
        if queue_name:
            tasks = tasks.filter(queue_name=queue_name)

        lease_seconds = serializer.validated_data["lease_seconds"]

        with transaction.atomic(using=tasks.db):
            try:
                task_result = tasks.select_for_update(skip_locked=True).first()
            except NotImplementedError:
                # SQLite doesn't support select_for_update; fall back to simple query
                task_result = tasks.first()
            if task_result is not None:
                task_result.claim(worker_id)
                expires_at = timezone.now() + datetime.timedelta(seconds=lease_seconds)
                TaskLease.objects.create(
                    task_result=task_result,
                    expires_at=expires_at,
                )

        if task_result is not None:
            from .tasks import reset_single_task_lease

            reset_single_task_lease.using(
                backend=get_setting("LEASE_RESET_BACKEND"),
                queue_name=get_setting("LEASE_RESET_QUEUE"),
                run_after=expires_at,
            ).enqueue(str(task_result.id))

        if task_result is None:
            return Response(status=status.HTTP_204_NO_CONTENT)

        return Response(
            DBTaskResultSerializer(task_result).data,
            status=status.HTTP_200_OK,
        )


class TaskResultView(APIView):
    """POST /tasks/<uuid>/result/ - Submit task completion result."""

    def post(self, request, pk):
        serializer = TaskResultSubmitSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        submitted_status = serializer.validated_data["status"]

        with transaction.atomic():
            try:
                task_result = DBTaskResult.objects.select_for_update().get(pk=pk)
            except DBTaskResult.DoesNotExist:
                return Response(
                    {"detail": "Task not found."},
                    status=status.HTTP_404_NOT_FOUND,
                )

            if task_result.status != TaskResultStatus.RUNNING:
                return Response(
                    {"detail": "Task is not in RUNNING state."},
                    status=status.HTTP_409_CONFLICT,
                )

            now = timezone.now()

            if submitted_status == "SUCCESSFUL":
                task_result.status = TaskResultStatus.SUCCESSFUL
                task_result.finished_at = now
                task_result.return_value = serializer.validated_data.get("return_value")
                task_result.exception_class_path = ""
                task_result.traceback = ""
            else:
                task_result.status = TaskResultStatus.FAILED
                task_result.finished_at = now
                task_result.exception_class_path = serializer.validated_data.get("exception_class_path", "")
                task_result.traceback = serializer.validated_data.get("traceback", "")
                task_result.return_value = None

            task_result.save(
                update_fields=[
                    "status", "return_value", "finished_at",
                    "exception_class_path", "traceback",
                ]
            )

            # Clean up the lease now that the task is finished
            TaskLease.objects.filter(task_result_id=pk).delete()

        return Response(
            DBTaskResultSerializer(task_result).data,
            status=status.HTTP_200_OK,
        )


class TaskDetailView(APIView):
    """GET /tasks/<uuid>/ - Get task details."""

    def get(self, request, pk):
        try:
            task_result = DBTaskResult.objects.get(pk=pk)
        except DBTaskResult.DoesNotExist:
            return Response(
                {"detail": "Task not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(
            DBTaskResultSerializer(task_result).data,
            status=status.HTTP_200_OK,
        )
