"""
Simple class to handle the date creation and localization
"""

from datetime import datetime, timedelta, timezone


class Fecha:
    @property
    def _now(self) -> datetime:
        """Returns current time in date in AR timezone"""
        timezone_ar = timezone(timedelta(hours=-3))
        return datetime.now(timezone_ar)

    @property
    def hoy(self) -> str:
        """Returns the current date in YYYY-MM-DD format"""
        return self._now.strftime("%Y-%m-%d")

    @property
    def hoy_full(self) -> str:
        """Returns the current date in YYYY-MM-DD_HH:MM:SS format"""
        return self._now.strftime("%Y-%m-%d_%H:%M:%S")
