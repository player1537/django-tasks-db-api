from rest_framework import serializers


class DBTaskResultSerializer(serializers.Serializer):
    id = serializers.UUIDField(read_only=True)
    status = serializers.CharField(read_only=True)
    task_path = serializers.CharField(read_only=True)
    args_kwargs = serializers.JSONField(read_only=True)
    priority = serializers.IntegerField(read_only=True)
    queue_name = serializers.CharField(read_only=True)
    backend_name = serializers.CharField(read_only=True)
    enqueued_at = serializers.DateTimeField(read_only=True)
    started_at = serializers.DateTimeField(read_only=True)
    finished_at = serializers.DateTimeField(read_only=True)
    return_value = serializers.JSONField(read_only=True)


class TaskClaimRequestSerializer(serializers.Serializer):
    worker_id = serializers.CharField(required=True, max_length=64)
    lease_seconds = serializers.IntegerField(required=False, default=300)
    backend_name = serializers.CharField(required=False, default="default")


class TaskResultSubmitSerializer(serializers.Serializer):
    status = serializers.ChoiceField(choices=["SUCCESSFUL", "FAILED"])
    return_value = serializers.JSONField(required=False, default=None)
    exception_class_path = serializers.CharField(required=False, default="")
    traceback = serializers.CharField(required=False, default="")


class TaskEnqueueSerializer(serializers.Serializer):
    task_path = serializers.CharField(required=True)
    args_kwargs = serializers.JSONField(required=False, default=dict)
    priority = serializers.IntegerField(required=False, default=0)
    queue_name = serializers.CharField(required=False, default="default")
    backend_name = serializers.CharField(required=False, default="default")
    run_after = serializers.DateTimeField(required=False, default=None, allow_null=True)
