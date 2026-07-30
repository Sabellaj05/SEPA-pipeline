import os
import pytest
from unittest import mock

from sepa_pipeline.config import SEPAConfig


def test_config_loads_required_env_vars():
    """Test that SEPAConfig correctly loads and validates required environment variables."""
    with mock.patch.dict(os.environ, {
        "MINIO_ENDPOINT": "http://localhost:9000",
        "MINIO_BUCKET": "test-bucket",
        "MINIO_ACCESS_KEY": "test_access",
        "MINIO_SECRET_KEY": "test_secret",
    }, clear=True):
        config = SEPAConfig()
        
        assert config.minio_endpoint == "http://localhost:9000"
        assert config.minio_bucket == "test-bucket"
        assert config.minio_access_key == "test_access"
        assert config.minio_secret_key == "test_secret"


def test_config_raises_on_missing_required_vars():
    """Test that SEPAConfig raises a ValueError if MINIO_ENDPOINT or MINIO_BUCKET are missing."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError) as excinfo:
            SEPAConfig()
        
        err_msg = str(excinfo.value)
        assert "Missing required environment variables" in err_msg
        assert "MINIO_ENDPOINT" in err_msg
        assert "MINIO_BUCKET" in err_msg


def test_config_fallback_to_minio_user():
    """Test that MINIO_ACCESS_KEY falls back to MINIO_USER if not explicitly set."""
    with mock.patch.dict(os.environ, {
        "MINIO_ENDPOINT": "http://localhost:9000",
        "MINIO_BUCKET": "test-bucket",
        "MINIO_USER": "fallback_user",
        "MINIO_PASSWORD": "fallback_password",
    }, clear=True):
        config = SEPAConfig()
        
        assert config.minio_access_key == "fallback_user"
        assert config.minio_secret_key == "fallback_password"
