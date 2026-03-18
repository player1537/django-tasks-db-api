from django.urls import include, path

from . import views

app_name = "django_tasks_db_api"

urlpatterns = [
    path("tasks/ready/", views.TaskClaimView.as_view(), name="task-claim"),
    path("queue/<str:queue_name>/tasks/ready/", views.TaskClaimView.as_view(), name="task-claim-by-queue"),
    path("tasks/<uuid:pk>/result/", views.TaskResultView.as_view(), name="task-result"),
    path("tasks/<uuid:pk>/", views.TaskDetailView.as_view(), name="task-detail"),
]
