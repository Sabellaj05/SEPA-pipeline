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

        download_link = scraper._parse_html(mock_httpx_response)

        assert download_link is not None
        assert download_link == "https://example.com/sepa_jueves.zip"

    def test_parse_html_no_match(self, sample_url, sample_data_dir):
        """Test HTML parsing when no matching day-of-week file is found."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        # Create a mock response with no matching day-of-week files
        mock_response = Mock()
        mock_response.text = """
        <html>
            <body>
                <a href="https://example.com/sepa_miercoles.zip">Download miercoles</a>
                <a href="https://example.com/sepa_viernes.zip">Download viernes</a>
                <a href="https://example.com/other_file.txt">Other file</a>
            </body>
        </html>
        """

        download_link = scraper._parse_html(mock_response)

        assert download_link is None

    def test_parse_html_no_links(self, sample_url, sample_data_dir):
        """Test HTML parsing when no links are found."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        # Create a mock response with no links
        mock_response = Mock()
        mock_response.text = "<html><body><div>No links here</div></body></html>"

        download_link = scraper._parse_html(mock_response)

        assert download_link is None

    @pytest.mark.asyncio
    async def test_download_data_success(self, sample_url, sample_data_dir):
        """Test successful data download with valid file size."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))
        download_link = "https://example.com/test.zip"

        # Mock the fecha.hoy and client.stream
        with patch("sepa_pipeline.scraper.Fecha") as mock_fecha_class:
            # Setup fecha mock
            mock_fecha = Mock()
            mock_fecha.hoy = "2025-10-23"
            mock_fecha_class.return_value = mock_fecha
            scraper.fecha = mock_fecha

            # Create a mock async context manager with large content
            mock_response = Mock()
            mock_response.headers = {"content-length": str(200 * 1024 * 1024)}  # 200MB

            # Create an async iterator that yields a large amount of data
            async def mock_aiter_bytes():
                # Yield chunks that total to about 200MB
                chunk_size = 1024 * 1024  # 1MB chunks
                total_chunks = 200
                for _ in range(total_chunks):
                    yield b"x" * chunk_size

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

            result = await scraper._download_data(download_link, min_file_size_mb=150)

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
                scraper,
                "_parse_html",
                return_value="https://example.com/sepa_jueves.zip",
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

    @pytest.mark.asyncio
    async def test_download_data_file_size_validation_success(
        self, sample_url, sample_data_dir
    ):
        """Test successful download with valid file size."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))
        download_link = "https://example.com/test.zip"

        # Mock the fecha.hoy
        with patch("sepa_pipeline.scraper.Fecha") as mock_fecha_class:
            mock_fecha = Mock()
            mock_fecha.hoy = "2025-10-23"
            mock_fecha_class.return_value = mock_fecha
            scraper.fecha = mock_fecha

            # Create a mock async context manager with large content
            mock_response = Mock()
            mock_response.headers = {"content-length": str(200 * 1024 * 1024)}  # 200MB

            # Create an async iterator that yields a large amount of data
            async def mock_aiter_bytes():
                # Yield chunks that total to about 200MB
                chunk_size = 1024 * 1024  # 1MB chunks
                total_chunks = 200
                for _ in range(total_chunks):
                    yield b"x" * chunk_size

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

            result = await scraper._download_data(download_link, min_file_size_mb=150)

            assert result is True
            # Check that file was created
            expected_file = sample_data_dir / "sepa_precios_2025-10-23.zip"
            assert expected_file.exists()

    @pytest.mark.asyncio
    async def test_download_data_file_size_validation_failure(
        self, sample_url, sample_data_dir
    ):
        """Test download failure when file size is too small."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))
        download_link = "https://example.com/test.zip"

        # Mock the fecha.hoy
        with patch("sepa_pipeline.scraper.Fecha") as mock_fecha_class:
            mock_fecha = Mock()
            mock_fecha.hoy = "2025-10-23"
            mock_fecha_class.return_value = mock_fecha
            scraper.fecha = mock_fecha

            # Create a mock async context manager with small content
            mock_response = Mock()
            mock_response.headers = {
                "content-length": "172"
            }  # Small file like the current issue

            # Create an async iterator that yields a small amount of data
            async def mock_aiter_bytes():
                yield b"x" * 172  # Small file

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

            result = await scraper._download_data(download_link, min_file_size_mb=150)

            assert result is False
            # Check that file was NOT created (should be deleted)
            expected_file = sample_data_dir / "sepa_precios_2025-10-23.zip"
            assert not expected_file.exists()

    @pytest.mark.asyncio
    async def test_hurtar_datos_with_file_size_validation(
        self, sample_url, sample_data_dir, mock_httpx_response
    ):
        """Test complete scraping process with file size validation."""
        scraper = SepaScraper(url=sample_url, data_dir=str(sample_data_dir))

        with (
            patch.object(
                scraper, "_connect_to_source", return_value=mock_httpx_response
            ),
            patch.object(
                scraper,
                "_parse_html",
                return_value="https://example.com/sepa_jueves.zip",
            ),
            patch.object(scraper, "_download_data", return_value=True),
        ):

            result = await scraper.hurtar_datos(min_file_size_mb=150)

            assert result is True
            scraper._download_data.assert_called_once_with(
                "https://example.com/sepa_jueves.zip", 150
            )
