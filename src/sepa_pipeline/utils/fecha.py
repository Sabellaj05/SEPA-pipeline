"""
Simple class to handle the date creation and localization
"""

from datetime import datetime, timedelta, timezone


class Fecha:

    # Spanish day names mapping
    SPANISH_DAYS = {
        0: "lunes",  # Monday
        1: "martes",  # Tuesday
        2: "miercoles",  # Wednesday (no accent in filename)
        3: "jueves",  # Thursday
        4: "viernes",  # Friday
        5: "sabado",  # Saturday (no accent in filename)
        6: "domingo",  # Sunday
    }

    @property
    def _now(self) -> datetime:
        """Returns current time in date in AR timezone"""
        timezone_ar = timezone(timedelta(hours=-3))
        return datetime.now(timezone_ar)
    @property
    def ahora(self) -> datetime:
        """Public, Current AR(UTC-3) timezone-aware datetime object"""
        return self._now

    @property
    def hoy(self) -> str:
        """Returns the current date in YYYY-MM-DD format"""
        return self._now.strftime("%Y-%m-%d")

    @property
    def hoy_full(self) -> str:
        """Returns the current date in YYYY-MM-DD_HH:MM:SS format"""
        return self._now.strftime("%Y-%m-%d_%H:%M:%S")

    @property
    def nombre_weekday(self) -> str:
        """Returns the current weekday name in spanish
        lowercase (lunes, martes, ...)
        """
        day_index = self._now.weekday()
        return self.SPANISH_DAYS[day_index]
