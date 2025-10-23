"""Tests for the Fecha utility class."""

from datetime import datetime

import pytest

from sepa_pipeline.utils.fecha import Fecha


class TestFecha:
    """Test cases for Fecha class."""

    def test_fecha_initialization(self):
        """Test that Fecha initializes correctly."""
        fecha = Fecha()
        assert fecha is not None
        assert hasattr(fecha, "hoy")

    def test_hoy_property_format(self):
        """Test that hoy property returns date in correct format."""
        fecha = Fecha()
        hoy = fecha.hoy

        # Should be in YYYY-MM-DD format
        assert isinstance(hoy, str)
        assert len(hoy) == 10
        assert hoy.count("-") == 2

        # Should be a valid date
        try:
            datetime.strptime(hoy, "%Y-%m-%d")
        except ValueError:
            pytest.fail(f"hoy property returned invalid date format: {hoy}")

    def test_hoy_property_today(self):
        """Test that hoy property returns today's date."""
        fecha = Fecha()
        hoy = fecha.hoy
        today = datetime.now().strftime("%Y-%m-%d")

        assert hoy == today, f"Expected today's date {today}, got {hoy}"
