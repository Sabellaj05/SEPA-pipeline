"""Tests for the main entry point."""

from unittest.mock import Mock, patch

from sepa_pipeline.main import main


class TestMain:
    """Test cases for main entry point."""

    @patch("sepa_pipeline.main.asyncio.run")
    @patch("sepa_pipeline.main.SepaScraper")
    def test_main_success(self, mock_scraper_class, mock_asyncio_run):
        """Test successful main execution."""
        # Setup mocks
        mock_scraper = Mock()
        mock_scraper_class.return_value = mock_scraper
        mock_asyncio_run.return_value = True

        # Run main
        main()

        # Verify scraper was created with correct parameters
        mock_scraper_class.assert_called_once_with(
            url="https://datos.produccion.gob.ar/dataset/sepa-precios", data_dir="data"
        )

        # Verify asyncio.run was called
        mock_asyncio_run.assert_called_once()
        # Check that the call was made with a callable (the method)
        call_args = mock_asyncio_run.call_args[0]
        assert callable(call_args[0])

    @patch("sepa_pipeline.main.asyncio.run")
    @patch("sepa_pipeline.main.SepaScraper")
    def test_main_failure(self, mock_scraper_class, mock_asyncio_run):
        """Test main execution when scraping fails."""
        # Setup mocks
        mock_scraper = Mock()
        mock_scraper_class.return_value = mock_scraper
        mock_asyncio_run.return_value = False

        # Run main
        main()

        # Verify scraper was created
        mock_scraper_class.assert_called_once()

        # Verify asyncio.run was called
        mock_asyncio_run.assert_called_once()
        # Check that the call was made with a callable (the method)
        call_args = mock_asyncio_run.call_args[0]
        assert callable(call_args[0])

    def test_main_imports(self):
        """Test that main module can be imported without errors."""
        from sepa_pipeline.main import main

        assert callable(main)
