from app.repositories.metrics_repository import (
    MetricsRepository
)


class MetricsService:

    def __init__(self):
        self.repository = (
            MetricsRepository()
        )

    def increment_processed(
        self,
        service_name
    ):
        pass