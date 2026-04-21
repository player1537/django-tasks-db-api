import datetime
import logging

from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django_filters.rest_framework import DjangoFilterBackend
from django_tasks.base import TaskResultStatus
from django_tasks_db.models import DBTaskResult
from rest_framework import status
from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.views import APIView

from .conf import get_setting
from .filters import TaskClaimFilter
from .models import TaskLease
from .serializers import (
    DBTaskResultSerializer,
    TaskClaimRequestSerializer,
    TaskEnqueueSerializer,
    TaskResultSubmitSerializer,
)

logger = logging.getLogger(__name__)


class TaskClaimView(APIView):
    """POST /tasks/ready/ - Claim the next ready task for a worker.

    Supports query string filtering via django-filters:
        ?queue_name=default
        ?queue_name__in=foo,bar,baz
    """

    filter_backends = [DjangoFilterBackend]
    filterset_class = TaskClaimFilter

    def get_queryset(self):
        # Get ready tasks
        queryset = DBTaskResult.objects.ready()

        # Exclude tasks with expired leases
        # Keep tasks that either have no lease OR have a lease that hasn't expired yet
        queryset = queryset.exclude(
            Q(lease__expires_at__lt=timezone.now())
        )

        return queryset

    def filter_queryset(self, queryset):
        for backend in self.filter_backends:
            queryset = backend().filter_queryset(self.request, queryset, self)
        return queryset

    def post(self, request):
        serializer = TaskClaimRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        worker_id = serializer.validated_data['worker_id']
        backend_name = serializer.validated_data.get('backend_name', 'default')

        tasks = self.get_queryset().filter(backend_name=backend_name)
        tasks = self.filter_queryset(tasks)

        lease_seconds = serializer.validated_data['lease_seconds']

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
                backend=get_setting('LEASE_RESET_BACKEND'),
                queue_name=get_setting('LEASE_RESET_QUEUE'),
                run_after=expires_at,
            ).enqueue(str(task_result.id))

        if task_result is None:
            logger.debug('No tasks available to claim')
            return Response(status=status.HTTP_204_NO_CONTENT)

        response_data = DBTaskResultSerializer(task_result).data
        logger.debug('Task claimed: %s', response_data)
        return Response(
            response_data,
            status=status.HTTP_200_OK,
        )


class TaskResultView(APIView):
    """POST /tasks/<uuid>/result/ - Submit task completion result."""

    def post(self, request, pk):
        serializer = TaskResultSubmitSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        submitted_status = serializer.validated_data['status']

        with transaction.atomic():
            try:
                task_result = DBTaskResult.objects.select_for_update().get(pk=pk)
            except DBTaskResult.DoesNotExist:
                logger.debug('Task not found: %s', pk)
                return Response(
                    {'detail': 'Task not found.'},
                    status=status.HTTP_404_NOT_FOUND,
                )

            if task_result.status != TaskResultStatus.RUNNING:
                logger.debug(
                    'Task %s is not in RUNNING state, current status: %s', pk, task_result.status
                )
                return Response(
                    {'detail': 'Task is not in RUNNING state.'},
                    status=status.HTTP_409_CONFLICT,
                )

            now = timezone.now()

            if submitted_status == 'SUCCESSFUL':
                task_result.status = TaskResultStatus.SUCCESSFUL
                task_result.finished_at = now
                task_result.return_value = serializer.validated_data.get('return_value')
                task_result.exception_class_path = ''
                task_result.traceback = ''
            else:
                task_result.status = TaskResultStatus.FAILED
                task_result.finished_at = now
                task_result.exception_class_path = serializer.validated_data.get(
                    'exception_class_path', ''
                )
                task_result.traceback = serializer.validated_data.get('traceback', '')
                task_result.return_value = None

            task_result.save(
                update_fields=[
                    'status',
                    'return_value',
                    'finished_at',
                    'exception_class_path',
                    'traceback',
                ]
            )

            # Clean up the lease now that the task is finished
            TaskLease.objects.filter(task_result_id=pk).delete()

        response_data = DBTaskResultSerializer(task_result).data
        logger.debug('Task result submitted: %s, status: %s', pk, submitted_status)
        return Response(
            response_data,
            status=status.HTTP_200_OK,
        )


class TaskDetailView(APIView):
    """GET /tasks/<uuid>/ - Get task details."""

    def get(self, request, pk):
        try:
            task_result = DBTaskResult.objects.get(pk=pk)
        except DBTaskResult.DoesNotExist:
            logger.debug('Task not found: %s', pk)
            return Response(
                {'detail': 'Task not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        response_data = DBTaskResultSerializer(task_result).data
        logger.debug('Task retrieved: %s', response_data)
        return Response(
            response_data,
            status=status.HTTP_200_OK,
        )


class TaskEnqueueView(APIView):
    """POST /tasks/ - Enqueue a new task."""

    def post(self, request):
        serializer = TaskEnqueueSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        task_result = DBTaskResult.objects.create(
            task_path=serializer.validated_data['task_path'],
            args_kwargs=serializer.validated_data.get('args_kwargs', {'args': [], 'kwargs': {}}),
            priority=serializer.validated_data.get('priority', 0),
            queue_name=serializer.validated_data.get('queue_name', 'default'),
            backend_name=serializer.validated_data.get('backend_name', 'default'),
            run_after=serializer.validated_data.get('run_after'),
        )

        response_data = DBTaskResultSerializer(task_result).data
        logger.debug('Task enqueued: %s', response_data)
        return Response(
            response_data,
            status=status.HTTP_201_CREATED,
        )
