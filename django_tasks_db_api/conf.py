from django.conf import settings

DEFAULTS = {
    "LEASE_RESET_BACKEND": "default",
    "LEASE_RESET_QUEUE": "default",
}


def get_setting(name):
    user_settings = getattr(settings, "DJANGO_TASKS_DB_API", {})
    return user_settings.get(name, DEFAULTS[name])
