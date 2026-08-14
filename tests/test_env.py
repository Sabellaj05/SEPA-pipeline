import os
import pytest
from unittest import mock

from sepa_pipeline.config import SEPAConfig


def test_config_loads_required_env_vars():
    """Test that SEPAConfig correctly loads and validates required environment variables."""
    with mock.patch.dict(
        os.environ,
        {
            "RUSTFS_ENDPOINT": "http://localhost:9000",
            "RUSTFS_BUCKET": "test-bucket",
            "RUSTFS_ACCESS_KEY": "test_access",
            "RUSTFS_SECRET_KEY": "test_secret",
            "POLARIS_URI": "http://localhost:8181/api/catalog",
            "POLARIS_REALM": "test_realm",
            "POLARIS_CLIENT_ID": "test_id",
            "POLARIS_CLIENT_SECRET": "test_secret",
        },
        clear=True,
    ):
        config = SEPAConfig()

        assert config.rustfs_endpoint == "http://localhost:9000"
        assert config.rustfs_bucket == "test-bucket"
        assert config.rustfs_access_key == "test_access"
        assert config.rustfs_secret_key == "test_secret"
        assert config.minio_endpoint == "http://localhost:9000"
        assert config.minio_bucket == "test-bucket"
        assert config.polaris_uri == "http://localhost:8181/api/catalog"
        assert config.polaris_realm == "test_realm"
        assert config.polaris_client_id == "test_id"
        assert config.polaris_client_secret == "test_secret"


def test_config_raises_on_missing_required_vars():
    """Test that SEPAConfig raises a ValueError if RUSTFS_ENDPOINT or RUSTFS_BUCKET are missing."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError) as excinfo:
            SEPAConfig()

        err_msg = str(excinfo.value)
        assert "Missing required environment variables" in err_msg
        assert "RUSTFS_ENDPOINT" in err_msg
        assert "RUSTFS_BUCKET" in err_msg


def test_config_fallback_to_minio_user():
    """Test that RUSTFS_ACCESS_KEY falls back to RUSTFS_USER or MINIO_ACCESS_KEY if not explicitly set."""
    with mock.patch.dict(
        os.environ,
        {
            "MINIO_ENDPOINT": "http://localhost:9000",
            "MINIO_BUCKET": "test-bucket",
            "MINIO_USER": "fallback_user",
            "MINIO_PASSWORD": "fallback_password",
        },
        clear=True,
    ):
        config = SEPAConfig()

        assert config.rustfs_access_key == "fallback_user"
        assert config.rustfs_secret_key == "fallback_password"
        assert config.minio_access_key == "fallback_user"
        assert config.minio_secret_key == "fallback_password"
