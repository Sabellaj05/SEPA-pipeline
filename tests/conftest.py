"""Pytest configuration and fixtures."""

from unittest.mock import Mock

import pytest


@pytest.fixture
def sample_data_dir(tmp_path):
    """Create a temporary data directory for testing."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def mock_httpx_response():
    """Create a mock httpx response for testing."""
    response = Mock()
    response.status_code = 200
    response.text = """
    <html>
        <body>
            <a href="https://example.com/sepa_jueves.zip">Download jueves</a>
            <a href="https://example.com/sepa_lunes.zip">Download lunes</a>
            <a href="https://example.com/other_file.txt">Other file</a>
        </body>
    </html>
    """
    response.headers = {"content-length": "1024"}
    return response


@pytest.fixture
def sample_url():
    """Sample URL for testing."""
    return "https://datos.produccion.gob.ar/dataset/sepa-precios"
