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
│       ├── config.py     # Infrastructure config only (Env vars, Paths, catalog configs).
│       ├── schema.py     # Silver/Bronze schema contract (schemas, rename maps, transforms).
│       ├── extractor.py  # ZIP extraction & S3 Fetching logic.
│       ├── pipeline.py   # Main Orchestrator (CLI Entry Point).
│       ├── scraper.py    # Web scraping & Raw S3 Upload.
│       ├── validator.py  # CSV reading, validation, referential integrity.
│       ├── loaders/
│       │   ├── base.py
│       │   ├── iceberg_loader.py
│       │   ├── postgres_loader.py
│       │   ├── parquet_loader.py
│       │   └── bigquery_loader.py # Native BigLake ingestion.
│       └── utils/
│           ├── bootstrap_lakehouse.py  # MinIO Setup & Backfill Utility.
│           ├── scan_bronze_dates.py    # Historical archive validation utility.
│           ├── verify_silver.py        # Silver layer health check CLI.
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
    - **Silver**: Cleaned data stored as Apache Iceberg tables in GCS (`gs://sepa-lakehouse-silver-74dbadf7/warehouse/silver/iceberg`) and MinIO. Star schema with an explicit Python schema contract (`schema.py`):
        - **Fact**: `sepa.precios` — ~15M rows/day, partitioned by `Day(fecha_vigencia)`. Carries `descripcion` and `marca` denormalized from source to avoid per-query dim joins in Gold.
        - **Dim**: `sepa.dim_comercios` — unpartitioned, daily snapshot (append-only).
        - **Dim**: `sepa.dim_sucursales` — unpartitioned, daily snapshot. Excludes 7 horario columns (operational, never queried).
        - **Dim**: `sepa.dim_productos` — unpartitioned, daily snapshot. Deduped on `id_producto` at load time.
    - **Silver Schema Contract**: All four tables are defined in `schema.py` with column types, rename maps, and Silver transform functions (`to_silver_*()`). Bronze stays raw (all `Utf8`); transforms run at pipeline load time. dbt staging models do `SELECT *` — the schema is owned by the pipeline, not dbt.
    - **Data Quality Normalizations**:
        - `marca` null → `"S/D"` (standard SEPA placeholder for unknown brand)
        - `provincia = "Buenos Aires"` → `"AR-B"` ISO 3166-2 (some stores file full name)
- **Catalog Integration**: 
    - **Local**: Project Nessie REST catalog (backed by PostgreSQL) + MinIO for file storage.
    - **Cloud**: Google Cloud BigLake via PyIceberg's native BigQuery catalog (`bigquery_loader.py`) for serverless analytics.
- **Health Check**: `uv run python -m sepa_pipeline.utils.verify_silver [--date YYYY-MM-DD] [--catalog nessie|bigquery]` — validates row counts, schema, null audits, partition pruning, and province ISO format for all four Silver tables.

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

## 8. Phase 4 - Analytics Layer (SPC-4) -- Gold Layer Complete

With the core ETL pipeline complete (SPC-3 Done), the Gold layer using **dbt** has been built and hardened.

- **Linear Project**: `Analytics Layer SEPA-4` (issues SEP-257 through SEP-266)
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
5. **SEP-261**: End-to-end validation + documentation -- **Done**
6. **SEP-262**: Add DuckDB local target -- **Done**
7. **SEP-266**: Gold model hardening + intermediate layer -- **Done**

### Key Architectural Notes for Gold Layer

#### Intermediate Layer (`models/intermediate/`)
Introduced in SEP-266 to fix a critical 35x row fanout caused by unreliable view-level deduplication through BigQuery's BigLake Iceberg adapter. The root cause: Silver dimensions are append-only daily snapshots; `QUALIFY ROW_NUMBER()` inside dbt views was not reliably applied by BigQuery when evaluating through the adapter.

The intermediate layer resolves this by materializing deduplicated snapshots as **tables** at build time:
- `dim_produtos_current` -- current-state product dimension, unique on `id_produto`
- `dim_sucursales_current` -- current-state store dimension, unique on `(id_sucursal, id_comercio)`
- `dim_comercios_current` -- current-state merchant dimension, unique on `(id_comercio, id_bandera)`
- `fct_price_quotes` -- atomic deduped price fact (incremental, 2-day lookback); carries `descripcion` and `marca` directly from Silver, so downstream gold models no longer need to join `dim_produtos` for those attributes

#### Gold Model Architecture
- `gold_price_timeseries` references `fct_price_quotes` (not `stg_precios`) and uses `INNER JOIN dim_sucursales_current` (table-materialized, guaranteed 1:1). Verified on BigQuery: 7.4M rows for 2026-03-30, no high-cardinality join warnings.
- `gold_cross_price_elasticity` and `gold_price_hypothesis` are **deprecated** -- they carry the same staging-join fanout and are superseded by the corrected architecture. Do not build on them.
- All ID columns in staging models must be cast to `STRING` to match `stg_precios`. This is a hard requirement -- the staging layer owns the type contract for its columns.
- Editing staging `.sql` files on disk does NOT update the deployed view in BigQuery. Run `dbt run --select staging` to redeploy.
- See `DOCS.md` Section 6 for the full Analytics Layer technical reference.

## 9. Silver Layer Schema Contract (SEP-267+)

### What was done

A formal Silver schema contract was introduced to fix a data quality root cause: `descripcion` and `marca` columns were never written to Silver (they were silently dropped in pipeline Phase 3). This caused `gold_price_timeseries` to return 0 valid rows for all dates from 2026-03-24 onward.

**New / changed files:**

| File | Change |
|:---|:---|
| `schema.py` | **New.** Owns all Bronze schemas, Silver schemas, rename maps, and `to_silver_*()` transform functions. |
| `config.py` | **Updated.** Infrastructure config only. Re-exports `get_schema_dict` for backward compatibility. |
| `validator.py` | **Updated.** Added `_read_csv()`, `load_dimensions()`, and `load_productos_chunk()`. CSV reading logic moved out of pipeline.py. |
| `pipeline.py` | **Updated.** Phase 2/3 now call `validator.load_dimensions()` and `validator.load_productos_chunk()`, then apply `to_silver_*()` transforms before every Lakehouse write. |
| `utils/verify_silver.py` | **New.** CLI health check for all four Silver Iceberg tables. |

**Key design decisions:**

- Silver schema is the contract. dbt staging models do `SELECT *` and add no column logic beyond type casting.
- Dims are **unpartitioned** by design — they are daily snapshots (append-only), not point-in-time histories. Filtering by `fecha_vigencia` on unpartitioned tables is cheap at dim scale (<500K rows/day).
- `precios` (fact) is partitioned by `Day(fecha_vigencia)` — required for scan efficiency at 15M rows/day.
- `descripcion` and `marca` are denormalized onto `precios` to avoid a 90K-row dim join on every Gold aggregation.

### Pending: BigQuery Silver Rebuild + dbt Fix

The Silver schema fix applies immediately to new Nessie/MinIO writes. **BigQuery Silver tables still contain the old schema** (missing `descripcion`, `marca`, wrong column names on dims). This is a separate PR.

See **DOCS.md Section 9** for the full implementation guide, including:
- Teardown procedure for BigQuery Silver via PyIceberg
- Pipeline rerun command for backfill
- New `stg_precios.sql` (passthrough of Silver column names — no renaming needed)
- Updated staging models for dims
- dbt rebuild sequence: `staging` → `intermediate --full-refresh` → `gold`
- Verification via `verify_silver.py --catalog bigquery`
