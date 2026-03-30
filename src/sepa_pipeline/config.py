"""
SEPA Pipeline Configuration
"""

import os
from pathlib import Path
from typing import Dict

import polars as pl
from dotenv import load_dotenv

load_dotenv()

COMERCIO_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio": pl.Utf8,
    "id_bandera": pl.Utf8,
    "comercio_cuit": pl.Utf8,
    "comercio_razon_social": pl.Utf8,
    "comercio_bandera_nombre": pl.Utf8,
    "comercio_bandera_url": pl.Utf8,
    "comercio_ultima_actualizacion": pl.Utf8,
    "comercio_version_sepa": pl.Utf8,
}

SUCURSALES_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio": pl.Utf8,
    "id_bandera": pl.Utf8,
    "id_sucursal": pl.Utf8,
    "sucursales_nombre": pl.Utf8,
    "sucursales_tipo": pl.Utf8,
    "sucursales_calle": pl.Utf8,
    "sucursales_numero": pl.Utf8,
    "sucursales_latitud": pl.Utf8,
    "sucursales_longitud": pl.Utf8,
    "sucursales_observaciones": pl.Utf8,
    "sucursales_barrio": pl.Utf8,
    "sucursales_codigo_postal": pl.Utf8,
    "sucursales_localidad": pl.Utf8,
    "sucursales_provincia": pl.Utf8,
    "sucursales_lunes_horario_atencion": pl.Utf8,
    "sucursales_martes_horario_atencion": pl.Utf8,
    "sucursales_miercoles_horario_atencion": pl.Utf8,
    "sucursales_jueves_horario_atencion": pl.Utf8,
    "sucursales_viernes_horario_atencion": pl.Utf8,
    "sucursales_sabado_horario_atencion": pl.Utf8,
    "sucursales_domingo_horario_atencion": pl.Utf8,
}

PRODUCTOS_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio": pl.Utf8,
    "id_bandera": pl.Utf8,
    "id_sucursal": pl.Utf8,
    "id_producto": pl.Utf8,
    "productos_ean": pl.Utf8,
    "productos_descripcion": pl.Utf8,
    "productos_cantidad_presentacion": pl.Utf8,
    "productos_unidad_medida_presentacion": pl.Utf8,
    "productos_marca": pl.Utf8,
    "productos_precio_lista": pl.Utf8,
    "productos_precio_referencia": pl.Utf8,
    "productos_cantidad_referencia": pl.Utf8,
    "productos_unidad_medida_referencia": pl.Utf8,
    "productos_precio_unitario_promo1": pl.Utf8,
    "productos_leyenda_promo1": pl.Utf8,
    "productos_precio_unitario_promo2": pl.Utf8,
    "productos_leyenda_promo2": pl.Utf8,
}


def get_schema_dict(table_type: str) -> Dict[str, type[pl.DataType]]:
    """
    Get CSV schema for a specific table type

    Args:
        table_type: One of 'comercio', 'sucursales', 'productos'

    Returns:
        Dictionary mapping column names to Polars data types

    Raises:
        ValueError: If table_type is unknown
    """

    schemas: Dict[str, Dict] = {
        "comercio": COMERCIO_SCHEMA,
        "sucursales": SUCURSALES_SCHEMA,
        "productos": PRODUCTOS_SCHEMA,
    }

    if table_type not in schemas:
        raise ValueError(
            f"Unknown table type: {table_type} Expected one of {list(schemas.keys())}"
        )
    return schemas[table_type]


class SEPAConfig:
    """
    Configuration for SEPA pipeline

    Loads settings from environment variables
    Singleton style configuration object
    """

    def __init__(self):
        self.sepa_user: str | None = os.getenv("POSTGRES_USER")
        self.sepa_password: str | None = os.getenv("POSTGRES_PASSWORD")
        self.sepa_db: str | None = os.getenv("POSTGRES_DB")
        self.sepa_host: str | None = os.getenv("POSTGRES_HOST", "localhost")
        self.sepa_port: str | None = os.getenv("POSTGRES_PORT")

        # MinIO / S3 Configuration
        self.minio_endpoint: str | None = os.getenv("MINIO_ENDPOINT")
        # Fallback to MINIO_USER/PASSWORD if specific keys aren't set
        self.minio_access_key: str | None = os.getenv(
            "MINIO_ACCESS_KEY", os.getenv("MINIO_USER")
        )
        self.minio_secret_key: str | None = os.getenv(
            "MINIO_SECRET_KEY", os.getenv("MINIO_PASSWORD")
        )
        self.minio_bucket: str | None = os.getenv("MINIO_BUCKET")
        self.minio_region: str | None = os.getenv("MINIO_REGION")

        # Pipeline config
        self.retention_days_postgres: int = int(
            os.getenv("RETENTION_DAYS_POSTGRES", "90")
        )
        self.max_workers: int = int(os.getenv("MAX_WORKERS_POSTGRES", "8"))

        # validate required fields
        self._validate()

        # Directories
        # Defaults to /tmp, but can be overridden to use local disk (e.g., ./tmp or /mnt/data)
        self.temp_dir: Path = Path(os.getenv("SEPA_TEMP_DIR", "/tmp"))
        self.raw_data_dir: Path = Path("data")
        self.archive_dir: Path = Path("data/archive")

    def _validate(self) -> None:
        required = {
            "POSTGRES_USER": self.sepa_user,
            "POSTGRES_PASSWORD": self.sepa_password,
            "POSTGRES_DB": self.sepa_db,
            "POSTGRES_HOST": self.sepa_host,
            "POSTGRES_PORT": self.sepa_port,
            "MINIO_ENDPOINT": self.minio_endpoint,
            "MINIO_BUCKET": self.minio_bucket,
        }

        # GCP configurations for Biglake (optional for non-GCP runs, but required for BigQuery Loader)
        self.gcp_project: str | None = os.getenv("GCP_PROJECT", "sepa-lakehouse42")
        self.gcp_dataset: str | None = os.getenv("GCP_DATASET", "silver")
        self.gcp_bucket: str | None = os.getenv(
            "GCP_BUCKET", "sepa-lakehouse-silver-74dbadf7"
        )
        self.gcp_dataset_gold: str | None = os.getenv("GCP_DATASET_GOLD", "gold")
        self.gcp_location: str | None = os.getenv("GCP_LOCATION", "US")

        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    @property
    def postgres_dsn(self) -> str:
        """Construct PostgreSQL DSN from environment variables."""
        # Note: The original instruction used self.db_user etc.
        # Assuming sepa_user etc. are the intended attributes for the DSN.
        return f"postgresql://{self.sepa_user}:{self.sepa_password}@{self.sepa_host}:{self.sepa_port}/{self.sepa_db}"

    @property
    def iceberg_catalog_config(self) -> dict:
        """Configuration for PyIceberg SQL Catalog"""
        # SQLAlchemy URI for the catalog (metadata)
        db_uri = f"postgresql+psycopg2://{self.sepa_user}:{self.sepa_password}@{self.sepa_host}:{self.sepa_port}/{self.sepa_db}"
        # Ensure endpoint has scheme
        endpoint = self.minio_endpoint
        assert endpoint is not None
        # If running locally on host, ensure we target localhost if env is internal
        # if "minio" in endpoint and not "localhost" in endpoint and "127.0.0.1" not in endpoint:
        #      # Heuristic: if we are running the script outside docker but env var is 'minio:9000'
        #      endpoint = endpoint.replace("minio", "localhost")

        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"

        return {
            "type": "sql",
            "uri": db_uri,
            "warehouse": f"s3://{self.minio_bucket}/silver/iceberg",
            "s3.endpoint": endpoint,
            "s3.access-key-id": self.minio_access_key,
            "s3.secret-access-key": self.minio_secret_key,
            "s3.region": self.minio_region,
            # PyIceberg/PyArrow should handle scheme from endpoint, but path-style is usually needed for MinIO
            # "s3.path-style-access": "true", # PyArrow might default correctly with custom endpoint, but let's try WITHOUT explicit first if PyArrow script worked without it.
            # actually, PyArrow script didn't set it. let's comment it out to match.
            "s3.signer": "s3v4",  # Force S3v4 signing for MinIO compatibility
        }

    @property
    def bigquery_catalog_config(self) -> dict:
        """Configuration for PyIceberg BigQuery Catalog"""
        return {
            "type": "bigquery",
            "gcp.project-id": self.gcp_project,
            "gcp.bigquery.project-id": self.gcp_project,
            # BQ catalog creates metadata here and creates BQ tables referencing it.
            # MUST be a gs:// URI so BigLake can access it natively.
            "warehouse": f"gs://{self.gcp_bucket}/warehouse",
        }

    def get_schema(self, table_type: str) -> Dict[str, type[pl.DataType]]:
        """Method to access schema via config object"""
        return get_schema_dict(table_type)
