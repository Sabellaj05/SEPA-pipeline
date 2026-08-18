import os

import boto3
import requests
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

config = SEPAConfig()
s3_client = boto3.client(
    "s3",
    endpoint_url=config.rustfs_endpoint,
    aws_access_key_id=config.rustfs_access_key,
    aws_secret_access_key=config.rustfs_secret_key,
    region_name=config.rustfs_region,
)


def ensure_polaris_catalog() -> None:
    """Ensure catalog 'default' exists in Apache Polaris."""
    polaris_uri = config.polaris_uri or "http://localhost:8181/api/catalog"
    realm = config.polaris_realm or "default"
    client_id = config.polaris_client_id or "polaris"
    client_secret = config.polaris_client_secret or "polaris"
    bucket = config.rustfs_bucket or "sepa-lakehouse"
    warehouse_loc = f"s3://{bucket}/silver/iceberg"

    base_url = polaris_uri.rstrip("/").removesuffix("/v1")
    token_url = f"{base_url}/v1/oauth/tokens"
    mgmt_url = f"{base_url.removesuffix('/catalog')}/management/v1/catalogs"

    try:
        token_res = requests.post(
            token_url,
            headers={"Polaris-Realm": realm},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            timeout=10,
        )
        if not token_res.ok:
            logger.warning(
                f"Failed to fetch Polaris token for bootstrap: {token_res.text}"
            )
            return
        token = token_res.json().get("access_token")

        cat_res = requests.post(
            mgmt_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Polaris-Realm": realm,
                "Content-Type": "application/json",
            },
            json={
                "catalog": {
                    "name": "default",
                    "type": "INTERNAL",
                    "readOnly": False,
                    "properties": {"default-base-location": warehouse_loc},
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": [warehouse_loc],
                        "stsUnavailable": True,
                    },
                }
            },
            timeout=10,
        )
        if cat_res.status_code in (200, 201):
            logger.info("Created catalog 'default' in Apache Polaris.")
        elif cat_res.status_code in (400, 409):
            logger.info("Catalog 'default' already exists in Apache Polaris.")
        else:
            logger.warning(
                f"Polaris catalog setup status {cat_res.status_code}: {cat_res.text}"
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm,
            "Content-Type": "application/json",
        }
        # Create catalog role and assign CATALOG_MANAGE_CONTENT
        requests.post(
            f"{mgmt_url}/default/catalog-roles",
            headers=headers,
            json={"catalogRole": {"name": "catalog_admin"}},
            timeout=10,
        )
        requests.put(
            f"{mgmt_url}/default/catalog-roles/catalog_admin/grants",
            headers=headers,
            json={"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"},
            timeout=10,
        )
        mgmt_base = mgmt_url.removesuffix("/catalogs")
        requests.put(
            f"{mgmt_base}/principal-roles/service_admin/catalog-roles/default",
            headers=headers,
            json={"catalogRole": {"name": "catalog_admin"}},
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"Could not bootstrap Polaris catalog: {e}")


def bootstrap_lakehouse(local_dir: str | None = None) -> None:
    """
    Bootstraps the RustFS Lakehouse by creating the main bucket.
    Optionally uploads files from a directory to the Bronze layer with Hive-style partitioning.
    """
    ensure_polaris_catalog()

    # Initialize S3 Client

    # Use consistent bucket name from config
    batch_bucket = config.rustfs_bucket or "sepa-lakehouse"

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


def teardown_silver_tables() -> None:
    """
    Drop all silver Iceberg tables from Polaris REST Catalog and delete all underlying S3 data.

    Drops the catalog entries from Polaris, then deletes everything under silver/iceberg/sepa/
    in RustFS — which covers all UUID variants regardless of how many rebuild cycles
    have run.
    """
    catalog = load_catalog("default")
    bucket = config.rustfs_bucket or "sepa-lakehouse"

    silver_tables = [
        "sepa.precios",
        "sepa.dim_comercios",
        "sepa.dim_sucursales",
        "sepa.dim_productos",
    ]

    for identifier in silver_tables:
        try:
            catalog.drop_table(identifier, purge_requested=False)
            logger.info(f"[TEARDOWN] Dropped table: {identifier}")
        except NoSuchTableError:
            logger.info(f"[TEARDOWN] Table not found, skipping: {identifier}")
        except Exception as e:
            logger.error(f"[TEARDOWN] Failed to drop {identifier}: {e}")

    try:
        catalog.drop_namespace("sepa")
        logger.info("[TEARDOWN] Dropped namespace: sepa")
    except NoSuchNamespaceError:
        pass
    except Exception as e:
        logger.warning(f"[TEARDOWN] Could not drop namespace sepa: {e}")

    # Delete all S3 objects under the sepa namespace prefix.
    # This covers all UUID-suffixed directories left behind by Nessie across
    # any number of previous rebuild cycles.
    prefix = "silver/iceberg/sepa/"
    logger.info(f"[TEARDOWN] Deleting all S3 objects under s3://{bucket}/{prefix}")
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        keys = [obj["Key"] for page in pages for obj in page.get("Contents", [])]
        if keys:
            # S3 batch delete accepts up to 1000 keys per request
            for i in range(0, len(keys), 1000):
                batch = [{"Key": k} for k in keys[i : i + 1000]]
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            logger.info(f"[TEARDOWN] Deleted {len(keys)} S3 objects.")
        else:
            logger.info("[TEARDOWN] No S3 objects found under prefix.")
    except Exception as e:
        logger.error(f"[TEARDOWN] S3 cleanup failed: {e}")

    logger.info("[TEARDOWN] Silver teardown complete.")


def check_exists_file(bucket: str, target_key: str) -> bool:
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
    import argparse

    parser = argparse.ArgumentParser(
        description="Bootstrap or teardown the SEPA Lakehouse."
    )
    parser.add_argument(
        "local_dir",
        nargs="?",
        default=None,
        help="Optional path to a local directory of sepa_precios_YYYY-MM-DD.zip files to upload to bronze.",
    )
    parser.add_argument(
        "--teardown-silver",
        action="store_true",
        help="Drop all silver Iceberg tables from Polaris before bootstrapping. "
        "Use this to rebuild silver with corrected table locations/partition specs.",
    )
    args = parser.parse_args()

    if args.teardown_silver:
        teardown_silver_tables()

    bootstrap_lakehouse(args.local_dir)
