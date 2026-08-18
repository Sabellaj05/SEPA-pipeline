import os
from pathlib import Path


def test_mcp_config_injects_pyiceberg_home():
    """
    Test that importing lakehouse_mcp.config automatically resolves the .env
    file and injects PYICEBERG_HOME into os.environ.
    """
    # Ensure PYICEBERG_HOME is not set before the test
    original_home = os.environ.pop("PYICEBERG_HOME", None)

    try:
        import importlib
        import lakehouse_mcp.config

        importlib.reload(lakehouse_mcp.config)

        assert "PYICEBERG_HOME" in os.environ

        # Verify it points to the project root (where .env is)
        expected_path = (
            Path(lakehouse_mcp.config.__file__).resolve().parent.parent.parent
        )
        actual_path = Path(os.environ["PYICEBERG_HOME"]).resolve()

        # Since it resolves find_dotenv(), it should match the project root when found
        assert actual_path.is_absolute()
        assert actual_path == expected_path

    finally:
        # Restore environment
        if original_home is not None:
            os.environ["PYICEBERG_HOME"] = original_home
        else:
            os.environ.pop("PYICEBERG_HOME", None)
