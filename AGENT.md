# GEMINI.md: AI Assistant Guide

This document provides a detailed, technical overview of the SEPA Price Pipeline project. It is intended for AI assistants to quickly understand the project's architecture, goals, and operational procedures.

## 1. Project Overview

- **Purpose**: This project is an asynchronous data pipeline that scrapes daily price data from the SEPA (Sistema Electrónico de Publicidad de Precios Argentino) government website.
- **Core Functionality**: It navigates to a specific URL, finds a download link based on the current day of the week, downloads a large `.zip` file, validates its size, and saves it to a local directory.
- **Key Challenge**: The target website does not provide a permanent link to the latest data. The download link is dynamically generated and named based on the Spanish name for the day of the week (e.g., `sepa_jueves.zip` for Thursday). The data is also only available during a specific time window each day.

## 2. Technology Stack

- **Language**: Python 3.12+
- **Package/Dependency Management**: `uv`
- **HTTP Client**: `httpx` (for asynchronous requests)
- **HTML Parsing**: `BeautifulSoup4`
- **Resilience**: `tenacity` (for retry logic on network connections)
- **UI**: `tqdm` (for download progress bars)
- **Code Quality & Formatting**: `ruff`
- **Type Checking**: `mypy`
- **Testing**: `pytest` with `pytest-cov`, `pytest-asyncio`, and `pytest-mock`.

## 3. Project Structure

```
SEPA-pipeline/
├── .gitignore
├── data/                 # (Output) Stores downloaded .zip files.
├── logs/                 # (Output) Contains daily log files.
├── src/
│   └── sepa_pipeline/    # The main installable Python package.
│       ├── __init__.py   # Makes the directory a package.
│       ├── main.py       # The primary application entry point.
│       ├── scraper.py    # Core logic: SepaScraper class.
│       └── utils/        # Utility sub-package.
│           ├── __init__.py
│           ├── fecha.py    # Date/time helper class (Fecha).
│           ├── logger.py   # Application-wide logger instance.
│           └── logger_config.py # Logging setup and configuration.
├── tests/                # Automated tests (currently mirrors src structure).
├── GEMINI.md             # This file.
├── pyproject.toml        # Project metadata, dependencies, and tool config (ruff, mypy, pytest).
├── python_packaging_guide.md # Guide on project structure and import logic.
├── README.md             # User-facing documentation.
└── uv.lock               # Pinned versions of all dependencies.
```

## 4. Core Components & Logic

### `src/sepa_pipeline/main.py`
- **Purpose**: The main executable entry point for the application.
- **Logic**: 
    1. Defines constants for the target `URL` and `DATA_DIR`.
    2. Initializes the `SepaScraper` class within an `async with` block (to manage the `httpx` client lifecycle).
    3. Calls the `scraper.hurtar_datos()` method to run the pipeline.
    4. Logs the final success or failure status.
    5. Exits with a status code (0 for success, 1 for failure).

### `src/sepa_pipeline/scraper.py`
- **Contains**: The `SepaScraper` class, which holds all core scraping logic.
- **Lifecycle**: Must be used as an async context manager (`async with SepaScraper(...) as scraper:`) to ensure the `httpx.AsyncClient` is properly opened and closed.
- **Key Methods**:
    - `__init__(self, url, data_dir)`: Initializes with the target URL and data directory.
    - `_scraped_filename()`: Determines the target filename based on the day of the week (e.g., `sepa_jueves.zip`).
    - `_storage_filename()`: Determines the filename for storage, based on the full date (e.g., `sepa_precios_2025-09-26.zip`).
    - `_connect_to_source()`: Connects to the main URL with retry logic (`@tenacity.retry`).
    - `_parse_html(self, response)`: Parses the HTML content using BeautifulSoup to find the correct download link that matches `_scraped_filename()`.
    - `_download_data(self, download_link, min_file_size_mb)`: Downloads the file in chunks with a `tqdm` progress bar. Critically, it validates the final file size against a minimum threshold to ensure a complete download.
    - `hurtar_datos(self, min_file_size_mb)`: The main public method that orchestrates the entire process by calling the private methods in sequence.

### `src/sepa_pipeline/utils/fecha.py`
- **Contains**: The `Fecha` class.
- **Purpose**: A helper class to handle all date and time logic, specifically for the AR timezone (UTC-3).
- **Key Properties**:
    - `hoy`: Returns the current date as `YYYY-MM-DD`.
    - `nombre_weekday`: Returns the lowercase Spanish name for the current day of the week, which is essential for finding the correct download link.

### `src/sepa_pipeline/utils/logger_config.py` & `logger.py`
- **Purpose**: Sets up a project-wide logger.
- **Logic**: `logger_config.py` defines the `logger_setup` function which configures a logger to write to both the console and a daily log file in the `logs/` directory. `logger.py` creates a single, reusable instance of this logger.

## 5. Development Workflow

1.  **Installation**: The project uses `uv`. To install all dependencies into a new virtual environment, run:
    ```bash
    uv sync --all-groups
    ```
2.  **Running the Application**: To ensure portability and simulate a production environment, the application should be run as a module. This guarantees that the installed package is being used correctly.
    ```bash
    uv run python -m src.sepa_pipeline.main
    ```
    > **Note**: While `uv run main.py` might also work, it relies on `uv`'s specific path handling. The `-m` method is the universal standard for Python and is guaranteed to work in any environment (like Docker or Airflow), which is the primary benefit of the `src` layout.
3.  **Running Tests**: Tests are run using `pytest`.
    ```bash
    uv run pytest
    ```
4.  **Code Quality**: A `pre-commit.sh` script is available, but the primary tools are configured in `pyproject.toml`.
    - **Format**: `uv run ruff format .`
    - **Lint**: `uv run ruff check .`
    - **Type Check**: `uv run mypy .`

## 6. Key Architectural Decisions & History

- **`src` Layout**: The project was refactored into a `src` layout to cleanly separate the installable package (`sepa_pipeline`) from project configuration files. This is a standard practice that prevents many common Python import issues.
- **Editable Install**: The package is installed using `uv pip install -e .`. This is crucial for development as it makes the `sepa_pipeline` package available throughout the virtual environment without copying files, ensuring changes are reflected immediately.
- **Class-Based Scraper**: The scraper logic was refactored from standalone functions into the `SepaScraper` class. This encapsulates state (URL, data directory, HTTP client), provides a clear interface, and improves testability.
- **Relative Imports**: All intra-package imports use relative syntax (e.g., `from .utils.logger import logger`). This is essential for the package to be runnable and importable correctly.

## 7. Project Roadmap & Future Goals

- **Database Integration**: The next major step is to add functionality to unzip the downloaded file, process the CSVs within, and load the data into a database (e.g., PostgreSQL, SQLite).
- **Orchestration**: The user has expressed interest in using Airflow to automate and schedule the pipeline runs.
- **Containerization**: The project is a good candidate for being containerized with Docker to create a portable and reproducible execution environment.
