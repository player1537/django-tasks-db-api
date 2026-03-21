from django_filters import rest_framework as filters
from django_tasks_db.models import DBTaskResult


class TaskClaimFilter(filters.FilterSet):
    queue_name = filters.CharFilter(field_name="queue_name", lookup_expr="exact")
    queue_name__in = filters.BaseInFilter(field_name="queue_name", lookup_expr="in")

    class Meta:
        model = DBTaskResult
        fields = []
