# AGENT.md: AI Assistant Guide

This document provides a detailed, technical overview of the SEPA Price Pipeline project. It is intended for AI assistants to quickly understand the project's architecture, goals, and operational procedures.

## 1. Project Overview

- **Purpose**: This project is an asynchronous data pipeline that scrapes daily price data from the SEPA (Sistema Electrónico de Publicidad de Precios Argentino) government website. The goal is to ingest this data into a Hybrid Lakehouse (PostgreSQL + MinIO/Iceberg) for analytics.
- **Core Functionality**: It navigates to a specific URL, extracts dynamic download links, downloads large `.zip` files, validates the data, and loads it into both a transactional database and a data lake.
- **Key Challenge**: Processing massive daily data dumps (~15M rows/day) reliably and efficiently, handling schema evolution and timezone inconsistencies.

## 2. Technology Stack

- **Language**: Python 3.12+
- **Database**: PostgreSQL 16 (via Docker)
- **Object Storage**: MinIO (S3-compatible) & Google Cloud Storage (GCS)
- **Table Format**: Apache Iceberg
- **Cloud Query Engine**: Google Cloud BigLake (Serverless Analytics)
- **Dependency Management**: `uv`
- **Libraries**:
    - `polars`: High-performance data processing (the engine of the pipeline).
    - `pyiceberg`: Logic for interacting with Iceberg tables.
    - `boto3` / `minio`: S3 interaction.
    - `psycopg`: PostgreSQL adapter.
    - `httpx` / `beautifulsoup4`: Scraping.
    - `tenacity`: Resilience/Retries.

## 3. Project Structure

```
SEPA-pipeline/
├── .env                  # Local environment variables - NOT COMMITTED
├── .gitignore
├── data/                 # (Output) Local staging for .zip files.
├── docker-compose.yml    # Infrastructure: Postgres, MinIO, McClient.
├── logs/                 # (Output) Daily log files.
├── sql/
│   └── init.sql          # DDL: Tables, Views, Partitions.
├── src/
│   └── sepa_pipeline/
│       ├── __init__.py
│       ├── config.py     # Centralized config (Env vars, Paths, Constants).
│       ├── extractor.py  # ZIP extraction & S3 Fetching logic.
│       ├── pipeline.py   # Main Orchestrator (CLI Entry Point).
│       ├── scraper.py    # Web scraping & Raw S3 Upload.
│       ├── validator.py  # Data validation & Schema enforcement.
│       ├── loaders/
│       │   ├── base.py
│       │   ├── iceberg_loader.py
│       │   ├── postgres_loader.py
│       │   ├── parquet_loader.py
│       │   └── bigquery_loader.py # Native BigLake ingestion.
│       └── utils/
│           ├── bootstrap_lakehouse.py  # MinIO Setup & Backfill Utility.
│           ├── scan_bronze_dates.py    # Historical archive validation utility.
│           └── logger.py
├── tests/
├── AGENT.md              # This file.
├── pyproject.toml        # Dependencies & Tool config.
└── README.md
```

## 4. Architecture: The Hybrid Lakehouse

The project employs a dual-layer architecture to handle scale and distinct workloads:

### 4.1. Operational Layer (PostgreSQL)
- **Role**: Hot Store. Serves real-time/interactive queries for the web frontend.
- **Schema**:
    - **Dimensions**: `comercios`, `sucursales`, `productos_master` (Normalized).
    - **Fact**: `precios` (Partitioned by `fecha_vigencia`).
- **Optimization**:
    - **Partitioning**: The `precios` table is partitioned by `RANGE(fecha_vigencia)` (Date). This solves timezone drift issues faced with timestamp partitioning.
    - **Bulk Loading**: Uses `COPY` for speed. Referential integrity is enforced by the application (`validator.py`) before loading.

### 4.2. Analytical Layer (Iceberg Lakehouse)
- **Role**: Cold Store / Data Lake. Historical analysis and massive aggregations.
- **Structure**:
    - **Bronze**: Raw ZIP files stored in MinIO (`s3://sepa-lakehouse/bronze/raw`).
    - **Silver**: Cleaned data stored as Apache Iceberg tables in GCS (`gs://sepa-lakehouse-silver-74dbadf7/warehouse/silver/iceberg`) and MinIO. Matches the Operational schema (Star Schema):
        - **Fact**: `precios` (Partitioned by `Day(fecha_vigencia)`).
        - **Dimensions**: `dim_comercios`, `dim_sucursales`, `dim_productos` (Appended via Daily Snapshots, partitioned by `Day(fecha_vigencia)`).
- **Catalog Integration**: 
    - **Local**: PostgreSQL is used as the Iceberg Catalog.
    - **Cloud**: Google Cloud BigLake operates directly against PyIceberg's native BigQuery catalog (`bigquery_loader.py`) for serverless analytics.

## 5. Development Workflow

1.  **Start Infrastructure**:
    ```bash
    docker-compose up -d
    ```
2.  **Initialize/Reset Lakehouse** (First time or after reset):
    ```bash
    uv run python -m sepa_pipeline.utils.bootstrap_lakehouse
    uv run python -m sepa_pipeline.utils.setup_bigquery
    ```
3.  **Run Pipeline**:
    The pipeline is CLI-driven (`pipeline.py`).
    - **Run for specific date**:
      ```bash
      uv run python -m sepa_pipeline.pipeline --date 2026-01-04
      ```
    - **Run specific stages**:
      ```bash
      uv run python -m sepa_pipeline.pipeline --date 2026-01-04 --stages postgres
      ```
4.  **Run Tests**:
    ```bash
    uv run pytest
    ```

## 6. Key Architectural Decisions

- **Polars for ETL**: Polars is used for all in-memory data transformation due to its speed and memory efficiency compared to Pandas.
- **Application-Side Integrity**: To enable fast `COPY` inserts, foreign keys are **disabled on the fact table (`precios`)**. `validator.py` enforces referential integrity in-application before loading. Trade-off: Insert speed (+300%) vs database-enforced constraints.
- **Strict Partitioning**: Database partitions are strictly aligned with the Business Date (`fecha_vigencia`) rather than ingestion time, ensuring determinism.
- **Majority Vote Consensus Validation**: The scraping payload acts as the primary shield against stale data. It physically inspects the contents of a random sample (`N=20`) of nested store `.zip` files before fetching the final 180MB batch. If the *majority* of sampled files are older than 24h, the package is rejected. This prevents isolated inactive stores from blocking pipeline ingestion while guaranteeing overall partition freshness.
- **Data Retention Policy**:
    - **Postgres Hot Store**: 60-90 days (configurable, supports recent price lookups for web frontend)
    - **Iceberg Cold Store**: Indefinite retention (historical analytics, annual price trends, inflation analysis)
    - **Bronze Raw ZIP**: Indefinite retention (immutable source of truth for disaster recovery)

## 7. Storage Projections

| Layer | Size/Year | Retention | Purpose |
|:---|:---|:---|:---|
| **Bronze Raw ZIP** | 113 GB | Indefinite | Immutable Archive |
| **Bronze Parquet** | 39 GB | Indefinite | Fast Replay Source |
| **Silver Iceberg** | 27 GB | Indefinite | Analytics Ready |
| **Postgres Hot** | 330 GB | 60-90 Days | Web App Queries (Auto-truncated) |

## 8. Current Focus: Phase 4 - Analytics Layer (SPC-4)

With the core ETL pipeline complete (SPC-3 Done), the focus is building a **Gold layer** using **dbt**.

- **Linear Project**: `Analytics Layer SEPA-4` (issues SEP-257 through SEP-262)
- **Adapter**: `dbt-bigquery` (primary), `dbt-duckdb` (secondary/local)
- **Source**: Silver Iceberg tables in BigQuery (`sepa-lakehouse42.silver.*`)
- **Target**: Separate `gold` BigQuery dataset (`sepa-lakehouse42.gold`)
- **Scope**: SEPA source only (GS1/BCRA deferred)
- **Python**: `>=3.12,<3.14` (dbt-core does not support Python 3.14 yet due to pydantic.v1 incompatibility)

### dbt Setup (SEP-257 -- Done)

- **Project**: `dbt/sepa_analytics/`
- **Profile**: `dbt/profiles.yml` (in-repo, use `--profiles-dir`)
- **Auth**: Service Account (`keyfile` via `GOOGLE_APPLICATION_CREDENTIALS` env variable or direct path in `profiles.yml`)
- **Run dbt commands from repo root**:
  ```bash
  uv run dbt debug --project-dir dbt/sepa_analytics --profiles-dir dbt
  uv run dbt run --project-dir dbt/sepa_analytics --profiles-dir dbt
  ```

### Issue Sequence
1. **SEP-257**: Setup dbt project + BigQuery adapter -- **Done**
2. **SEP-258**: Setup Gold BigQuery dataset -- **Done**
3. **SEP-259**: Define Silver sources + staging models -- **Done**
4. **SEP-260**: Create Gold mart models (`mart_daily_price_summary`, `mart_store_coverage`) -- **Done**
5. **SEP-261**: End-to-end validation + documentation -- **Next**
6. **SEP-262**: Add DuckDB local target -- Backlog

### Key Architectural Notes for Gold Layer
- The Silver `precios` fact is a **narrow fact** -- `productos_descripcion` and `productos_marca` are NULL on it by design. Marts must JOIN `stg_dim_productos` for product attributes, not read from `stg_precios`.
- All ID columns in staging models must be cast to `STRING` to match `stg_precios`. This is a hard requirement -- the staging layer owns the type contract for its columns.
- Staging views are currently deployed to `sepa-lakehouse42.gold` (same dataset as marts). Schema separation will be cleaned up in SEP-261.
- Editing staging `.sql` files on disk does NOT update the deployed view in BigQuery. Run `dbt run --select staging` to redeploy.
- See `DOCS.md` Section 6 for the full Analytics Layer technical reference.

