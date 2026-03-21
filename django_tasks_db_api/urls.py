from django.urls import path

from . import views

app_name = "django_tasks_db_api"

urlpatterns = [
    path("tasks/", views.TaskEnqueueView.as_view(), name="task-enqueue"),
    path("tasks/ready/", views.TaskClaimView.as_view(), name="task-claim"),
    path("tasks/<uuid:pk>/result/", views.TaskResultView.as_view(), name="task-result"),
    path("tasks/<uuid:pk>/", views.TaskDetailView.as_view(), name="task-detail"),
]
