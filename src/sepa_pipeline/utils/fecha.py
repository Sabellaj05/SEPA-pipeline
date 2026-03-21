"""
Simple class to handle the date creation and localization
"""

from datetime import datetime, timedelta, timezone, date


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

    def __init__(self, target_date: str | date | datetime | None = None):
        """Initializes Fecha, optionally overriding the 'now' context."""
        self._target_date = target_date

    @property
    def _now(self) -> datetime:
        """Returns current time in date in AR timezone"""
        timezone_ar = timezone(timedelta(hours=-3))
        if self._target_date:
            if isinstance(self._target_date, str):
                dt = datetime.strptime(self._target_date, "%Y-%m-%d")
                return dt.replace(tzinfo=timezone_ar)
            elif isinstance(self._target_date, date) and not isinstance(
                self._target_date, datetime
            ):
                return datetime.combine(
                    self._target_date, datetime.min.time()
                ).replace(tzinfo=timezone_ar)
            elif isinstance(self._target_date, datetime):
                if self._target_date.tzinfo is None:
                    return self._target_date.replace(tzinfo=timezone_ar)
                return self._target_date.astimezone(timezone_ar)
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
