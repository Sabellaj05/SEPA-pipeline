"""Tests for the main entry point."""

import pytest
from unittest.mock import AsyncMock, patch

from sepa_pipeline.main import main


class TestMain:
    """Test cases for main entry point."""

    @pytest.mark.asyncio
    @patch("sepa_pipeline.main.SepaScraper")
    async def test_main_success(self, mock_scraper_class):
        """Test successful main execution."""
        # Setup async mock for the context manager
        mock_scraper_instance = AsyncMock()
        mock_scraper_instance.hurtar_datos.return_value = True

        # When the class is instantiated, its __aenter__ method should return our
        # instance
        mock_scraper_class.return_value.__aenter__.return_value = mock_scraper_instance

        # Run main
        result = await main()

        # Verify scraper was created with correct parameters
        mock_scraper_class.assert_called_once_with(
            url="https://datos.produccion.gob.ar/dataset/sepa-precios", data_dir="data"
        )

        # Verify hurtar_datos was called
        mock_scraper_instance.hurtar_datos.assert_awaited_once()

        # Verify exit code
        assert result == 0

    @pytest.mark.asyncio
    @patch("sepa_pipeline.main.SepaScraper")
    async def test_main_failure(self, mock_scraper_class):
        """Test main execution when scraping fails."""
        # Setup async mock for the context manager
        mock_scraper_instance = AsyncMock()
        mock_scraper_instance.hurtar_datos.return_value = False

        mock_scraper_class.return_value.__aenter__.return_value = mock_scraper_instance

        # Run main
        result = await main()

        # Verify scraper was created
        mock_scraper_class.assert_called_once()

        # Verify hurtar_datos was called
        mock_scraper_instance.hurtar_datos.assert_awaited_once()

        # Verify exit code
        assert result == 1

    def test_main_imports(self):
        """Test that main module can be imported without errors."""
        from sepa_pipeline.main import main

        assert callable(main)
