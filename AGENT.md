# AGENT.md: AI Assistant Guide

This document provides a detailed, technical overview of the SEPA Price Pipeline project. It is intended for AI assistants to quickly understand the project's architecture, goals, and operational procedures.

## 1. Project Overview

- **Purpose**: This project is an asynchronous data pipeline that scrapes daily price data from the SEPA (Sistema Electrónico de Publicidad de Precios Argentino) government website. The goal is to load this data into a production-grade database for large-scale analytics.
- **Core Functionality**: It navigates to a specific URL, finds a dynamic download link, downloads a large `.zip` file, and is designed to process and load its contents into a PostgreSQL database.
- **Key Challenge**: The source provides massive daily data dumps (~15M rows/day) within a tight time window, requiring a highly performant and reliable data loading strategy.

## 2. Technology Stack

- **Language**: Python 3.12+
- **Package/Dependency Management**: `uv`
- **Database**: PostgreSQL 16 (via Docker)
- **Containerization**: Docker Compose
- **HTTP Client**: `httpx`
- **HTML Parsing**: `BeautifulSoup4`
- **Resilience**: `tenacity`
- **Code Quality & Formatting**: `ruff`
- **Type Checking**: `mypy`
- **Testing**: `pytest`

## 3. Project Structure

```
SEPA-pipeline/
├── .env                  # Local environment variables (DB credentials) - NOT COMMITTED
├── .gitignore
├── data/                 # (Output) Stores downloaded .zip files.
├── docker-compose.yml    # Defines PostgreSQL, MinIO, and setup services.
├── logs/                 # (Output) Contains daily log files.
├── sql/
│   └── init.sql          # DDL script for creating the database schema.
├── src/
│   └── sepa_pipeline/    # The main installable Python package.
│       ├── __init__.py
│       ├── config.py     # Configuration management.
│       ├── extractor.py  # ZIP extraction logic.
│       ├── loader.py     # Database loading (Postgres & MinIO).
│       ├── main.py       # Application entry point.
│       ├── pipeline.py   # Orchestration logic.
│       ├── scraper.py    # Web scraping logic.
│       ├── validator.py  # Data validation logic.
│       └── utils/
├── tests/
├── AGENT.md             # This file.
├── pyproject.toml
├── README.md
└── uv.lock
```

## 4. Core Components & Logic

### `src/sepa_pipeline/scraper.py`
- **Contains**: The `SepaScraper` class, which holds all core scraping logic.
- **Key Methods**: `hurtar_datos()` is the main public method that orchestrates the scraping process.

### `Database (PostgreSQL)`
- **Schema File**: `sql/init.sql`
- **Design Overview**: The schema is highly optimized for a high-volume, time-series workload.
    - **Dimension Tables**: `comercios` (Companies/Brands), `sucursales` (Stores).
    - **Master Table**: `productos_master` serves as a normalized product catalog to reduce data redundancy.
    - **Fact Table**: `precios` is the core table containing daily price observations.
- **Key Architectural Decisions**:
    - **Partitioning**: The `precios` table is partitioned by `RANGE(scraped_at)`. This is critical for query performance and efficient data management (e.g., dropping old partitions).
    - **No Foreign Keys on Fact Table**: Foreign key constraints are **intentionally omitted** on the `precios` table. This is a crucial performance decision to allow for extremely fast bulk loading via PostgreSQL's `COPY` command. Referential integrity is validated at the application layer *before* loading, which is the standard practice for high-volume data warehousing.

## 5. Development Workflow

1.  **Start the Database**: The project requires a running PostgreSQL instance, managed by Docker.
    ```bash
    docker-compose up -d
    ```
2.  **Installation**: The project uses `uv`. To install all dependencies into a new virtual environment, run:
    ```bash
    uv sync --all-groups
    ```
3.  **Running the Application**: To ensure portability, the application should be run as a module.
    ```bash
    uv run python -m src.sepa_pipeline.main
    ```
4.  **Running Tests**:
    ```bash
    uv run pytest
    ```

## 6. Key Architectural Decisions & History

- **`src` Layout & Editable Install**: The project uses a standard `src` layout and is installed via `uv pip install -e .` to ensure reliable, production-like imports.
- **Class-Based Scraper**: The scraping logic is encapsulated in the `SepaScraper` class for clarity and testability.
- **Containerized Database**: The project uses `docker-compose` to provide a reproducible PostgreSQL environment, separating the application from the database infrastructure.
- **High-Performance Loading Strategy**: The database schema and ETL process are designed around using PostgreSQL's native `COPY` command for maximum data ingestion speed. This involves validating data integrity at the application level rather than relying on database-level foreign key constraints on the fact table.

## 7. Project Roadmap & Future Goals

- **SPC-1: Project setup and Scraper**: ✅ Complete.
- **SPC-2: Database Infrastructure**: ✅ Complete. (Schema designed, Docker environment created).
- **SPC-3: ETL/ELT Pipeline**: ⏳ **Next Step**. This involves building the Python logic to:
    1.  Unzip the scraped files.
    2.  Perform application-level validation of data integrity.
    3.  Load the data into the PostgreSQL database using the `COPY` command, respecting the `comercios` -> `sucursales` -> `productos_master` -> `precios` loading order.
- **SPC-4: Pipeline Automation/Orchestration**: Future goal (e.g., Airflow).
- **SPC-5: Analytics Layer**: Future goal.


## 8. Changelog & Debugging Log

### 2025-11-20: Ingestion Pipeline Implementation (SPC-3)

**Major Refactoring**:
- Split monolithic `ingestion.py` into modular components:
    - `config.py`: Centralized configuration.
    - `extractor.py`: Parallel ZIP extraction.
    - `validator.py`: Data integrity and schema validation.
    - `loader.py`: Database loading logic (Upsert + COPY).
    - `pipeline.py`: Orchestration script.

**Bugs Fixed**:
1.  **`NotNullViolation` (Footer Detection)**:
    - **Issue**: Footer rows like "Última actualización" caused null violations.
    - **Fix**: Implemented robust, case-insensitive footer detection in `SEPAValidator._drop_footer_rows`.
2.  **`CSV malformed` Warnings**:
    - **Issue**: Pipe-separated data caused parsing warnings.
    - **Fix**: Added `quote_char=None` to `pl.read_csv`.
3.  **`ModuleNotFoundError`**:
    - **Issue**: Incorrect imports (`src.sepa_pipeline...`).
    - **Fix**: Corrected to `sepa_pipeline...`.
4.  **`NotNullViolation` on `id_bandera` / Location Fields**:
    - **Issue**: Null values in required DB columns.
    - **Fix**: Added strict null checks in `validator.py` to filter invalid rows before loading.
5.  **`ForeignKeyViolation`**:
    - **Issue**: `sucursales` referencing missing `comercios` and `productos` referencing missing `sucursales`.
    - **Fix**: Implemented strict referential integrity checks in `validator.py` to **drop** orphaned records.

**Current Status**:
- The pipeline runs successfully and loads data.
- **Known Issue**: `StringDataRightTruncation` on `sucursales_provincia` (value too long for `varchar(10)`). This is the next item to debug.

**Update (2025-11-20 02:00)**:
- **Fix**: Expanded `sucursales_provincia` to `VARCHAR(255)` in `sql/init.sql` and migrated the live DB.
- **Result**: Pipeline successfully ingested **14,217,967** price records in ~10 minutes.
- **Data Verification**:
    - Comercios: 45
    - Sucursales: 3,089
    - Products: 88,082
    - Prices: 14,217,967


## 9. Architecture Evolution: The Lakehouse Vision

The project is evolving from a single PostgreSQL database to a dual-layer architecture to handle scale and distinct workloads.

### 9.1. Operational Layer (Hot Store)
- **Purpose**: Serve real-time/interactive queries for the web frontend.
- **Data Retention**: Recent data only (e.g., last 90 days).
- **Tech Stack**:
    - **API**: FastAPI (Endpoints for `/price`, `/history`).
    - **Database**: PostgreSQL (Indexed for low-latency lookups).

### 9.2. Analytical Layer (Cold Store / Lakehouse)
- **Purpose**: Historical analysis, BI, and complex aggregations over massive datasets (~1TB/year).
- **Tech Stack**:
    - **Storage**: MinIO (S3-compatible object storage).
    - **Format**: Apache Iceberg (Table format) + Parquet (File format).
    - **Ingestion**: Python + Polars + PyIceberg.
    - **Query Engine**: DuckDB (for ad-hoc SQL over Iceberg).
    - **Transformation**: dbt (managing models in DuckDB/Iceberg).
    - **BI**: Metabase (Dashboards).

### 9.3. Data Flow
1.  **Ingestion**: `scraper` -> `pipeline` (Polars) -> **Iceberg/MinIO** (Raw/Bronze & Clean/Silver).
2.  **Operational Sync**: `pipeline` -> **PostgreSQL** (Recent data only).
3.  **Analytics**: **DuckDB** queries **Iceberg** -> **dbt** transforms -> **Metabase** visualizes.
