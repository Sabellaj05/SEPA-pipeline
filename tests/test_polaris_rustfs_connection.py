import boto3
import pytest
from pyiceberg.catalog import load_catalog

from sepa_pipeline.config import SEPAConfig


def test_rustfs_s3_connection():
    """Verify that boto3 S3 client connects cleanly to local RustFS server."""
    config = SEPAConfig()
    s3_client = boto3.client(
        "s3",
        endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_access_key,
        aws_secret_access_key=config.minio_secret_key,
        region_name=config.minio_region,
    )
    # List buckets to confirm RustFS S3 API works
    response = s3_client.list_buckets()
    assert "Buckets" in response
    bucket_names = [b["Name"] for b in response["Buckets"]]
    assert config.minio_bucket in bucket_names


def test_polaris_catalog_connection():
    """Verify that PyIceberg REST catalog connects and authenticates with Apache Polaris."""
    catalog = load_catalog("default")
    assert catalog is not None
    # Verify catalog can list namespaces without error
    namespaces = catalog.list_namespaces()
    assert isinstance(namespaces, list)
