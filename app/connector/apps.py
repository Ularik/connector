from django.apps import AppConfig
import os


class ConnectorConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'connector'

    def ready(self):
        from .views import init_snapshot
        try:
            if os.environ.get("RUN_MAIN") == "true":
                init_snapshot()
            print("✅ Snapshot initialized")
        except Exception as e:
            print(f"⚠️ Snapshot init failed: {e}")