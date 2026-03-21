import os

import boto3
from botocore.exceptions import ClientError

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

config = SEPAConfig()
s3_client = boto3.client(
    "s3",
    endpoint_url=config.minio_endpoint,
    aws_access_key_id=config.minio_access_key,
    aws_secret_access_key=config.minio_secret_key,
    region_name="us-east-1",
)


def bootstrap_lakehouse(local_dir: str | None = None) -> None:
    """
    Bootstraps the MinIO Lakehouse by creating the main bucket.
    Optionally uploads files from a directory to the Bronze layer with Hive-style partitioning.
    """

    # Initialize S3 Client

    # Use consistent bucket name from config
    batch_bucket = config.minio_bucket or "sepa-lakehouse"

    logger.info(f"Bootstrapping Lakehouse Bucket: {batch_bucket}...")

    existing_buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]

    if batch_bucket not in existing_buckets:
        logger.info(f"Creating bucket: {batch_bucket}")
        try:
            s3_client.create_bucket(Bucket=batch_bucket)
        except Exception as e:
            logger.error(f"Failed to create bucket {batch_bucket}: {e}")
    else:
        logger.info(f"Bucket already exists: {batch_bucket}")

    # Create folder placeholders (optional but nice for browsing)
    folders = ["bronze/", "silver/", "gold/"]
    for folder in folders:
        try:
            s3_client.put_object(Bucket=batch_bucket, Key=folder)
        except Exception as e:
            logger.warning(f"Error creating folders: {folders}: {e}")

    # Optional: Upload raw zip files manual backups
    if local_dir and os.path.isdir(local_dir):
        logger.info(f"Scanning {local_dir} for Manual Backups to upload...")
        for filename in os.listdir(local_dir):
            if filename.endswith(".zip") and "sepa_precios_" in filename:
                # Expected format: sepa_precios_YYYY-MM-DD.zip
                # Target path: bronze/raw/year=YYYY/month=MM/day=DD/filename.zip
                try:
                    date_part = filename.replace("sepa_precios_", "").replace(
                        ".zip", ""
                    )
                    yyyy, mm, dd = date_part.split("-")

                    # Hive-style partitioning expected by Extractor
                    target_key = (
                        f"bronze/raw/year={yyyy}/month={mm}/day={dd}/{filename}"
                    )

                    logger.info(
                        f"Uploading {filename} -> s3://{batch_bucket}/{target_key}"
                    )

                    file_path = os.path.join(local_dir, filename)
                    if check_exists_file(batch_bucket, target_key):
                        logger.info(f"Skipping: {filename} already exists in s3")
                        continue

                    s3_client.upload_file(file_path, batch_bucket, target_key)

                except ValueError:
                    logger.warning(
                        f"Could not parse date from {filename}, skipping auto-upload."
                    )
                except Exception as e:
                    logger.error(f"Failed to upload {filename}: {e}")

    logger.info("Lakehouse bootstrap complete.")


def check_exists_file(bucket, target_key) -> bool:
    try:
        # head object only retrieves metadata
        s3_client.head_object(Bucket=bucket, Key=target_key)
        return True
    except ClientError as e:
        # 404 file doesn't exist
        if e.response["Error"]["Code"] == "404":
            return False
        # if you get 403, check permissions of 's3:ListBucket'
        raise e


if __name__ == "__main__":
    import sys

    manual_dir = sys.argv[1] if len(sys.argv) > 1 else None
    bootstrap_lakehouse(manual_dir)
