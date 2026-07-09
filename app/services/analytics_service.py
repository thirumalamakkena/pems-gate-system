from datetime import datetime, timezone

from app.repositories.analytics_repository import (
    AnalyticsRepository
)


class AnalyticsService:

    def __init__(self):
        self.repository = (
            AnalyticsRepository()
        )

    def update_hourly_stats(
        self,
        event
    ):
        pass