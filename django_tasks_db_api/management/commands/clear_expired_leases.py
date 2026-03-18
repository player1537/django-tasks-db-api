from django.core.management.base import BaseCommand

from django_tasks_db_api.lease import reset_expired_leases


class Command(BaseCommand):
    help = "Reset tasks with expired leases from RUNNING back to READY"

    def handle(self, **options):
        count = reset_expired_leases()
        self.stdout.write(f"Reset {count} expired lease(s).\n")
