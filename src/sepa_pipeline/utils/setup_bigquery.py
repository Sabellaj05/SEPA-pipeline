from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

config = SEPAConfig()


def setup_gold_dataset(project_id: str, dataset_id: str, location: str) -> None:
    """
    Ensures the specified BigQuery dataset exists.
    Idempotent operation: creates the dataset if it doesn't exist, safely ignores if it does.
    """
    assert project_id is not None, "please provide a project id"
    assert dataset_id is not None, "please provide a dataset id"
    assert location is not None, "please provide a location "

    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location

    try:
        client.create_dataset(dataset, timeout=30)
        logger.info(
            f"Successfully created dataset '{dataset_ref}' in location '{location}'."
        )
    except Conflict:
        logger.info(f"Dataset '{dataset_ref}' already exists. Skipping creation.")
    except Exception as e:
        logger.error(f"Failed to create dataset '{dataset_ref}': {e}")
        raise


if __name__ == "__main__":
    logger.info("Starting BigQuery environment setup...")
    setup_gold_dataset(config.gcp_project, config.gcp_dataset_gold, config.gcp_location)
    logger.info("BigQuery environment setup complete.")
