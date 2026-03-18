from django.urls import include, path

urlpatterns = [
    path("api/", include("django_tasks_db_api.urls")),
]
