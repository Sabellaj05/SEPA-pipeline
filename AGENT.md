# AGENT.md: AI Assistant Guide

This document provides a detailed, technical overview of the SEPA Price Pipeline project. It is intended for AI assistants to quickly understand the project's architecture, goals, and operational procedures.

## 1. Project Overview

- **Purpose**: This project is an asynchronous data pipeline that scrapes daily price data from the SEPA (Sistema Electrónico de Publicidad de Precios Argentino) government website. The goal is to ingest this data into a Hybrid Lakehouse (PostgreSQL + MinIO/Iceberg) for analytics.
- **Core Functionality**: It navigates to a specific URL, extracts dynamic download links, downloads large `.zip` files, validates the data, and loads it into both a transactional database and a data lake.
- **Key Challenge**: Processing massive daily data dumps (~15M rows/day) reliably and efficiently, handling schema evolution and timezone inconsistencies.

## 2. Technology Stack

- **Language**: Python 3.12+
- **Database**: PostgreSQL 16 (via Docker)
- **Object Storage**: MinIO (S3-compatible)
- **Table Format**: Apache Iceberg
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
│       ├── loader.py     # Loading logic for Postgres (COPY) & Iceberg (Append).
│       ├── pipeline.py   # Main Orchestrator (CLI Entry Point).
│       ├── scraper.py    # Web scraping & Raw S3 Upload.
│       ├── validator.py  # Data validation & Schema enforcement.
│       └── utils/
│           ├── bootstrap_lakehouse.py  # MinIO Setup & Backfill Utility.
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

### 4.2. Analytical Layer (MinIO + Iceberg)
- **Role**: Cold Store / Data Lake. Historical analysis and massive aggregations.
- **Structure**:
    - **Bronze**: Raw ZIP files stored in MinIO (`s3://sepa-lakehouse/bronze/raw`).
    - **Silver**: Cleaned data stored as Apache Iceberg tables (`s3://sepa-lakehouse/silver/iceberg`).
- **Catalog**: PostgreSQL is used as the Iceberg Catalog to track table metadata.

## 5. Development Workflow

1.  **Start Infrastructure**:
    ```bash
    docker-compose up -d
    ```
2.  **Initialize/Reset Lakehouse** (First time or after reset):
    ```bash
    uv run python -m sepa_pipeline.utils.bootstrap_lakehouse
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
- **Application-Side Integrity**: To enable fast `COPY` inserts, foreign keys are disabled on the fact table. `validator.py` ensures no orphaned records are inserted.
- **S3-Centric Flow**: The pipeline pulls data from the "Bronze" layer (MinIO) rather than the local file system (except in legacy/dev modes), decoupling scraping from processing.
- **Strict Partitioning**: Database partitions are strictly aligned with the Business Date (`fecha_vigencia`) rather than ingestion time, ensuring determinism.

## 7. Changelog & Debugging Log

### 2025-11-20: Ingestion Pipeline Implementation (SPC-3)

**Major Refactoring**:
- Split monolithic `ingestion.py` into modular components: `config.py`, `extractor.py`, `validator.py`, `loader.py`, `pipeline.py`.

**Bugs Fixed**:
1.  **`NotNullViolation` (Footer Detection)**: Fixed by `_drop_footer_rows` regex.
2.  **`CSV malformed` Warnings**: Fixed by `quote_char=None`.
3.  **`ForeignKeyViolation`**: Fixed by strict referential integrity checks in `validator.py`.

### 2026-01-03: Lakehouse Phase 2 - Iceberg Integration (Silver Layer)

**Goal**: Load daily price data into an Apache Iceberg table (`sepa.precios`) stored in MinIO.

**Major Changes**:
- **Dependencies**: Added `pyiceberg[duckdb,s3fs,sql-postgres]`.
- **Loader**: Enhanced `SEPALoader` to initialize PyIceberg Catalog and append data.

**Bugs Fixed**:
1.  **`AWS Error ACCESS_DENIED`**: Fixed by adding `"s3.signer": "s3v4"` to catalog config.

### 2026-01-03: Lakehouse Phase 3 - Bronze Layer Decoupling

**Goal**: Move to a "Cloud-Native" architecture.

**Key Changes**:
*   **`src/sepa_pipeline/scraper.py`**: Now uploads Raw ZIPs to `s3://sepa-lakehouse/bronze/raw`.
*   **`src/sepa_pipeline/extractor.py`**: Added `fetch_from_bronze` to download from S3.

**Bugs Fixed**:
*   **`pyarrow.fs.copy_file` API Error**: Replaced with manual `open_input_stream`.

### 2026-01-04: Lakehouse Phase 3b - Iceberg Partitioning Optimization

**Goal**: Partition `sepa.precios` by `Day(fecha_vigencia)`.

**Key Changes**:
- **Partitioning**: Applied `DayTransform` on `fecha_vigencia` in Iceberg.
- **Dependencies**: Added `pyiceberg[pyiceberg-core]`.

**Bugs Fixed**:
- **Strict Schema Validation**: Fixed crash when creating tables from generic Arrow schemas.

### 2026-01-06: Robustness, CLI & Schema Fixes (Phase 4-6)

**Goal**: Enhance operational control and data quality, fixing timezone-related partitioning issues.

**Key Changes**:
- **CLI Implementation**: Refactored `pipeline.py` to support `argparse` (`--date`, `--stages`).
- **Bootstrap Utility**: Added `bootstrap_lakehouse.py` to automate bucket creation.
- **Strict Validation**: Moved validation *inside* the processing loop to prevent bad data merges.
- **Schema Migration**: Changed `precios` partitioning from `scraped_at` (Timestamp) to `fecha_vigencia` (Date).

**Bugs Fixed**:
- **Timezone Drift**:
    - **Issue**: Timestamp partitioning caused late-night scrapes (UTC) to land in tomorrow's partition.
    - **Fix**: Switched to Date-based partitioning (`fecha_vigencia`).
- **Schema Contamination**:
    - **Issue**: `pl.concat` failed on mismatched chunks.
    - **Fix**: Validate-then-merge strategy implemented.


### 2026-01-25: Scraper Date Verification (Refactor)

**Goal**: Prevent ingestion of stale data when the SEPA website has not yet been updated for the current day.

**Key Changes**:
- **Scraper Hardening**: Modified `SepaScraper._parse_html` to extract the visible date string from the `.package-info` HTML block (e.g., `Precios SEPA Minoristas viernes, 2026-01-23`).
- **Strict Verification**: The scraper now compares the extracted HTML date with the target `iso_date`. If they mismatch, the process aborts immediately to avoid downloading old or incorrect files.
