from datetime import datetime,timezone,UTC,timedelta

from app.config.settings import (
    INITIAL_RETRY_DELAY 
)

class CurrentTimeStamp:

    def current_time(self):
        """Return a timezone-aware datetime truncated to milliseconds."""
        dt = datetime.now(timezone.utc)
        return dt.replace(
            microsecond=(dt.microsecond // 1000) * 1000
        )

    def current_time_iso(self):
        """Return ISO 8601 timestamp with milliseconds."""
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    
    def next_retry_time(self):
        return (
            datetime.now(timezone.utc) +
            timedelta(seconds=INITIAL_RETRY_DELAY)
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z")



















