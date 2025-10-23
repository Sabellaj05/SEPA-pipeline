"""Tests for the SepaScraper class."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from sepa_pipeline.scraper import SepaScraper


class TestSepaScraper:
    """Test cases for SepaScraper class."""

    def test_scraper_initialization(self, sample_url, sample_data_dir):
        """Test that SepaScraper initializes correctly."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        assert scraper.url == sample_url
        assert scraper.data_dir == sample_data_dir
        assert scraper.fecha is not None
        assert scraper.client is not None

    def test_scraper_initialization_with_path(self, sample_url, sample_data_dir):
        """Test that SepaScraper works with Path objects."""
        scraper = SepaScraper(url=sample_url, data_dir=sample_data_dir)

        assert scraper.data_dir == sample_data_dir

    @pytest.mark.asyncio
    async def test_connect_to_source_success(
        self, sample_url, sample_data_dir, mock_httpx_response
    ):
        """Test successful connection to source."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        with patch.object(scraper.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_httpx_response

            response = await scraper._connect_to_source()

            assert response is not None
            assert response == mock_httpx_response
            mock_get.assert_called_once_with(sample_url)

    @pytest.mark.asyncio
    async def test_connect_to_source_failure(self, sample_url, sample_data_dir):
        """Test connection failure handling."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        with patch.object(scraper.client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = Exception("Connection failed")

            # The retry decorator will retry 3 times, so we expect a RetryError
            with pytest.raises(Exception):  # This will catch the RetryError
                await scraper._connect_to_source()

    def test_parse_html_success(self, sample_url, sample_data_dir, mock_httpx_response):
        """Test successful HTML parsing."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        # Mock the fecha.hoy to return a date that matches our mock HTML
        with patch("sepa_pipeline.scraper.Fecha") as mock_fecha_class:
            mock_fecha = Mock()
            mock_fecha.hoy = "2025-10-23"
            mock_fecha_class.return_value = mock_fecha
            scraper.fecha = mock_fecha

            download_link = scraper._parse_html(mock_httpx_response)

            assert download_link is not None
            assert download_link == "https://example.com/download.zip"

    def test_parse_html_no_match(
        self, sample_url, sample_data_dir, mock_httpx_response
    ):
        """Test HTML parsing when no matching date is found."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        # Mock the fecha.hoy to return a date that doesn't match our mock HTML
        with patch("sepa_pipeline.scraper.Fecha") as mock_fecha_class:
            mock_fecha = Mock()
            mock_fecha.hoy = "2025-12-31"
            mock_fecha_class.return_value = mock_fecha
            scraper.fecha = mock_fecha

            download_link = scraper._parse_html(mock_httpx_response)

            assert download_link is None

    def test_parse_html_no_containers(self, sample_url, sample_data_dir):
        """Test HTML parsing when no package containers are found."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        # Create a mock response with no package containers
        mock_response = Mock()
        mock_response.text = "<html><body><div>No packages here</div></body></html>"

        download_link = scraper._parse_html(mock_response)

        assert download_link is None

    @pytest.mark.asyncio
    async def test_download_data_success(self, sample_url, sample_data_dir):
        """Test successful data download."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))
        download_link = "https://example.com/test.zip"

        # Mock the fecha.hoy and client.stream
        with patch("sepa_pipeline.scraper.Fecha") as mock_fecha_class:
            # Setup fecha mock
            mock_fecha = Mock()
            mock_fecha.hoy = "2025-10-23"
            mock_fecha_class.return_value = mock_fecha
            scraper.fecha = mock_fecha

            # Create a mock async context manager
            mock_response = Mock()
            mock_response.headers = {"content-length": "1024"}

            # Create an async iterator for aiter_bytes
            async def mock_aiter_bytes():
                yield b"test data"

            mock_response.aiter_bytes.return_value = mock_aiter_bytes()

            # Create a proper async context manager
            class MockAsyncContext:
                def __init__(self, response):
                    self.response = response

                async def __aenter__(self):
                    return self.response

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

            # Mock the stream method to return our context manager
            scraper.client.stream = Mock(return_value=MockAsyncContext(mock_response))

            result = await scraper._download_data(download_link)

            assert result is True
            # Check that file was created
            expected_file = sample_data_dir / "sepa_precios_2025-10-23.zip"
            assert expected_file.exists()

    @pytest.mark.asyncio
    async def test_download_data_no_link(self, sample_url, sample_data_dir):
        """Test download with no link provided."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        result = await scraper._download_data("")

        assert result is False

    @pytest.mark.asyncio
    async def test_hurtar_datos_success(
        self, sample_url, sample_data_dir, mock_httpx_response
    ):
        """Test successful complete scraping process."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        with (
            patch.object(
                scraper, "_connect_to_source", return_value=mock_httpx_response
            ),
            patch.object(
                scraper, "_parse_html", return_value="https://example.com/test.zip"
            ),
            patch.object(scraper, "_download_data", return_value=True),
        ):

            result = await scraper.hurtar_datos()

            assert result is True

    @pytest.mark.asyncio
    async def test_hurtar_datos_connection_failure(self, sample_url, sample_data_dir):
        """Test scraping process when connection fails."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        with patch.object(scraper, "_connect_to_source", return_value=None):
            result = await scraper.hurtar_datos()

            assert result is False

    @pytest.mark.asyncio
    async def test_hurtar_datos_no_download_link(
        self, sample_url, sample_data_dir, mock_httpx_response
    ):
        """Test scraping process when no download link is found."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        with (
            patch.object(
                scraper, "_connect_to_source", return_value=mock_httpx_response
            ),
            patch.object(scraper, "_parse_html", return_value=None),
        ):

            result = await scraper.hurtar_datos()

            assert result is False
