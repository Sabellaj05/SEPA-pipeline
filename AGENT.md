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
- **Application-Side Integrity**: To enable fast `COPY` inserts, foreign keys are **disabled on the fact table (`precios`)**. `validator.py` enforces referential integrity in-application before loading. Trade-off: Insert speed (+300%) vs database-enforced constraints.
- **S3-Centric Flow**: The pipeline pulls data from the "Bronze" layer (MinIO) rather than the local file system (except in legacy/dev modes), decoupling scraping from processing.
- **Strict Partitioning**: Database partitions are strictly aligned with the Business Date (`fecha_vigencia`) rather than ingestion time, ensuring determinism.
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

## 8. Next Steps: Phase 4 - Orchestration

With the core ETL pipeline hardened (Phase SPC-3 ~95% complete), the focus shifts to automation:
- **Orchestration Tool**: Implement Airflow to manage daily schedule, retry logic, and monitoring.
- **Loader Decoupling**: Split Postgres and Iceberg loaders into separate modules (currently flag-based in `loader.py`).
- **Schema Evolution**: Define policies for handling new columns in CSVs (see `OPERATIONS.md`).
- **Retention Automation**: Schedule `drop_old_precios_partitions()` task in Airflow.
