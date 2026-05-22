# SEPA Pipeline - AI Assistant Guide

This document provides a detailed, technical overview of the SEPA Price Pipeline project. It is strictly intended to give AI assistants and developers the necessary context to navigate the project's architecture, directory structure, data flow, and operational procedures, without being bogged down by historical changes (for historical context, see `docs/CHANGELOG.md`).

## 1. Project Overview

- **Purpose**: An asynchronous data pipeline that scrapes daily price data from the SEPA (Sistema Electrónico de Publicidad de Precios Argentino) government website and ingests it into a Lakehouse (MinIO/Iceberg + BigQuery BigLake) for analytics.
- **Core Functionality**: Navigates to the SEPA URL, extracts dynamic download links, downloads large `.zip` files containing nested ZIPs, materializes them as Bronze Parquet, validates the data (referential integrity, data types), and loads it into the Silver Iceberg layer.
- **Key Challenge**: Processing massive daily data dumps (~15M rows/day) reliably and efficiently within bounded memory (~1.5 GB peak).

## 2. Technology Stack

- **Language**: Python 3.12+ (Requires <3.14 due to dbt compatibility)
- **Object Storage**: MinIO (S3-compatible) & Google Cloud Storage (GCS)
- **Table Format**: Apache Iceberg
- **Catalog**: Project Nessie (REST) for local; BigQuery BigLake for cloud
- **Analytics Engine**: Google Cloud BigQuery / dbt (data build tool)
- **Dependency Management**: `uv`
- **Core Libraries**:
    - `polars`: High-performance data processing engine.
    - `pyiceberg`: Iceberg table interaction.
    - `boto3` / `minio`: S3 interaction.
    - `httpx` / `beautifulsoup4`: Scraping.
    - `tenacity`: Resilience/Retries.

## 3. Project Structure

```text
SEPA-pipeline/
├── .env                  # Local environment variables - NOT COMMITTED
├── dbt/                  # The dbt project for the Gold analytics layer
├── docs/                 # Documentation including DOCS.md, EXPLAINER.md, and CHANGELOG.md
├── sql/                  # DDL scripts for tables, views, and partitions
├── src/sepa_pipeline/    # Main Python package
│   ├── config.py         # Infrastructure config (Env vars, S3 paths, catalog config)
│   ├── schema.py         # Silver/Bronze schema contract & transforms
│   ├── extractor.py      # ZIP extraction & S3 Fetching logic
│   ├── pipeline.py       # Unified orchestrator & CLI entry point
│   ├── scraper.py        # Web scraping & Raw S3 Upload
│   ├── validator.py      # Data validation & referential integrity checks
│   ├── manage_iceberg.py # Maintenance utilities (snapshot expiry, etc)
│   ├── loaders/          # Classes responsible for materializing data
│   │   ├── base.py
│   │   ├── iceberg_loader.py  # Local MinIO/Nessie silver loader
│   │   ├── bigquery_loader.py # Native BigLake silver loader
│   │   ├── parquet_loader.py  # Bronze parquet materializer
│   │   └── bronze_audit.py    # Bronze/Silver audit tables writer
│   └── utils/            # Helper scripts (bootstrap, health checks, etc)
├── tests/                # Test suite
├── pyproject.toml        # Dependencies & Tool config
├── AGENT.md              # This file
└── README.md
```

## 4. Architecture: The Analytical Lakehouse

The pipeline implements a Medallion Lakehouse architecture using local (MinIO/Nessie) and cloud (GCS/BigQuery) environments.

### 4.1. Bronze Layer (MinIO)
- **Raw Archive**: Original ZIP files (`s3://sepa-lakehouse/bronze/raw/...`). Immutable disaster recovery.
- **Bronze Parquet**: Fast replay source. Nested ZIPs are extracted and consolidated into `comercio.parquet`, `sucursales.parquet`, and `productos.parquet` (`s3://sepa-lakehouse/bronze/parquet/...`).
- **Audit Tables**: `sepa.audit_bronze` tracks extraction stats and corrupted file counts.

### 4.2. Silver Layer (Iceberg)
The cleaned, analytical layer. Schemas are strictly defined in `schema.py`.
- **Fact (`sepa.precios`)**: ~15M rows/day, partitioned by `Day(fecha_vigencia)`. Denormalizes `descripcion` and `marca` from `productos` to avoid massive Gold layer joins.
- **Dims (`sepa.dim_comercios`, `sepa.dim_sucursales`, `sepa.dim_productos`)**: Unpartitioned, daily append-only snapshots. Unnecessary operational columns are excluded.
- **Audit Table**: `sepa.audit_silver` tracks validation drops and load counts.
- **Health Check**: Run `uv run python -m sepa_pipeline.utils.verify_silver` to validate row counts, schema, and partitions.

### 4.3. Gold Layer (dbt + BigQuery)
- **Location**: `dbt/sepa_analytics/`
- **Intermediate Models (`models/intermediate/`)**: Materializes deduplicated snapshots of the Silver append-only dimensions as tables (e.g. `dim_productos_current`) to prevent join cardinality explosion (row fanout) in the marts.
- **Mart Models**: Highly aggregated models (e.g. `mart_daily_price_summary`) built on the intermediate tables for end-user analytics.

## 5. Key Architectural Decisions

- **Polars for ETL**: Chosen over Pandas/PyArrow for zero-copy memory efficiency and speed when processing millions of rows.
- **Batched Silver Loading**: The `productos` table is streamed from Bronze Parquet in 500,000-row batches. This bounded the pipeline's memory usage to ~1.5 GB peak (down from 22 GB).
- **Application-Side Integrity**: `validator.py` enforces referential integrity in-memory before loading. Orphaned rows referencing missing dimensions are dropped and audited.
- **Strict Date Partitioning**: Partitions use `fecha_vigencia` (Business Date) rather than ingestion timestamp, solving timezone drift issues for late-night scrapes.
- **Pipeline Owns the Schema**: The Python pipeline (`schema.py`) owns the type contracts and transformations. `dbt` staging models strictly `SELECT *` from Silver.
- **Majority Vote Consensus Validation**: Protects against isolated inactive stores blocking pipeline ingestion. The scraper samples nested `.zip` files and accepts the package only if the majority of sampled files are fresh.

## 6. Development Workflow

1. **Start Infrastructure**:
   ```bash
   docker-compose up -d
   ```
2. **Lakehouse Initialization** (First time only):
   ```bash
   uv run python -m sepa_pipeline.utils.bootstrap_lakehouse
   uv run python -m sepa_pipeline.utils.setup_bigquery
   ```
3. **Run Pipeline** (`pipeline.py` CLI):
   ```bash
   # Full pipeline for today (Iceberg + BigQuery targets)
   uv run python -m sepa_pipeline.pipeline
   
   # Specific date & target
   uv run python -m sepa_pipeline.pipeline --date 2026-04-04 --target iceberg
   ```
4. **dbt Analytics**:
   ```bash
   uv run dbt run --project-dir dbt/sepa_analytics --profiles-dir dbt
   ```
5. **Testing**:
   ```bash
   uv run pytest
   ```

## 7. Storage Projections

| Layer | Estimated Size/Year | Retention | Purpose |
|:---|:---|:---|:---|
| **Bronze Raw ZIP** | 113 GB | Indefinite | Immutable Archive |
| **Bronze Parquet** | 39 GB | Indefinite | Fast Replay Source |
| **Silver Iceberg** | 27 GB | Indefinite | Analytics Ready |
| **Audit Iceberg** | < 1 MB | Indefinite | Pipeline Observability |
