from dotenv import find_dotenv, load_dotenv
from sepa_pipeline.config import SEPAConfig

import os
from pathlib import Path

# Load .env from project root before importing SEPAConfig
env_path = find_dotenv()
load_dotenv(env_path)

if env_path and "PYICEBERG_HOME" not in os.environ:
    os.environ["PYICEBERG_HOME"] = str(Path(env_path).parent)

config = SEPAConfig()
