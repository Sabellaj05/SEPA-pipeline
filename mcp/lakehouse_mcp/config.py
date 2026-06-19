from dotenv import find_dotenv, load_dotenv
from sepa_pipeline.config import SEPAConfig

# Load .env from project root before importing SEPAConfig
load_dotenv(find_dotenv())

config = SEPAConfig()
