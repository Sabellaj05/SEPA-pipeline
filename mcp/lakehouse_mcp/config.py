import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# Locate SEPA project root robustly (mcp/lakehouse_mcp/config.py -> repo root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Try find_dotenv() first; if outside repo or not found, use PROJECT_ROOT / ".env"
env_path = find_dotenv()
if env_path and Path(env_path).parent.resolve() == PROJECT_ROOT.resolve():
    load_dotenv(env_path)
elif (PROJECT_ROOT / ".env").is_file():
    env_path = str(PROJECT_ROOT / ".env")
    load_dotenv(env_path)
else:
    load_dotenv()

# Ensure PYICEBERG_HOME points to repo root where .pyiceberg.yaml lives
if "PYICEBERG_HOME" not in os.environ:
    if (PROJECT_ROOT / ".pyiceberg.yaml").is_file():
        os.environ["PYICEBERG_HOME"] = str(PROJECT_ROOT)
    elif env_path and (Path(env_path).parent / ".pyiceberg.yaml").is_file():
        os.environ["PYICEBERG_HOME"] = str(Path(env_path).parent)

# PyIceberg catalog environment fallbacks
polaris_uri = os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog")
if "PYICEBERG_CATALOG__DEFAULT__URI" not in os.environ:
    os.environ["PYICEBERG_CATALOG__DEFAULT__URI"] = polaris_uri
if "PYICEBERG_CATALOG__DEFAULT__TYPE" not in os.environ:
    os.environ["PYICEBERG_CATALOG__DEFAULT__TYPE"] = "rest"
if "PYICEBERG_CATALOG__DEFAULT__WAREHOUSE" not in os.environ:
    os.environ["PYICEBERG_CATALOG__DEFAULT__WAREHOUSE"] = "default"
if "PYICEBERG_CATALOG__DEFAULT__CREDENTIAL" not in os.environ:
    client_id = os.getenv("POLARIS_CLIENT_ID", "polaris")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "polaris")
    os.environ["PYICEBERG_CATALOG__DEFAULT__CREDENTIAL"] = (
        f"{client_id}:{client_secret}"
    )
if "PYICEBERG_CATALOG__DEFAULT__SCOPE" not in os.environ:
    os.environ["PYICEBERG_CATALOG__DEFAULT__SCOPE"] = "PRINCIPAL_ROLE:ALL"

from sepa_pipeline.config import SEPAConfig  # noqa: E402

config = SEPAConfig()
