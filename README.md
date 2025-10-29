# SEPA Price Pipeline

## Overview

This project is an asynchronous data pipeline designed to scrape, download, and validate the SEPA (Sistema Electronico de Publicidad de Precios Argentino) dataset from the official Argentine government data portal. It is built with modern Python tooling and emphasizes code quality, testing, and robustness.

## Project Structure

```
src/sepa_pipeline/  # Main application source code
tests/              # Pytest test suite
main.py             # Application entry point
pyproject.toml      # Project metadata, dependencies, and tool configuration
pre-commit.sh       # Script for running all code quality checks
```

## Setup and Installation

### 1. **Clone the repository:**
```bash
git clone <repository-url>
cd SEPA-pipeline
```

### 2. **Install uv** (if not already installed)
```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip
pip install uv
```

### 3. **Install Dependencies**

#### Option A: Install Everything
```bash
# this will also create the virtual environment and use it 
uv sync --all-groups
```

#### Option B: Install Specific Groups
```bash
# Core dependencies only
uv sync

# Development tools
uv sync --group dev

# Testing tools
uv sync --group test

# Both dev and test
uv sync --group dev --group test
```

### 4: **Verify Installation**
```bash
# Check if everything is installed
uv run python -c "import sepa_pipeline; print('Package installed successfully')"

# Run the application
uv run main.py
```

## Usage

To execute the scraper, run the main script from the project root:

```bash
uv run main.py
```

Downloaded data will be saved to the `data/` directory, and logs will be stored in the `logs/` directory

## Development and Testing

This project uses a collection of tools to ensure code quality. A convenience script is provided to run all checks.

**Run all quality checks:**
```bash
./pre-commit.sh
```

This script executes the following tools in sequence:

- **`ruff format`**: For consistent code formatting.
- **`ruff check`**: For linting and identifying potential errors.
- **`mypy`**: For static type checking.
- **`pytest`**: For running the automated test suite and reporting code coverage (with a minimum threshold of 80%).
