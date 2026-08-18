# Justfile — SEPA Pipeline task runner
# Run `just` or `just --list` to inspect available recipes.

set dotenv-load := true

# Default recipe: list available commands
default:
    @just --list

# ---------------------------------------------------------------------------
# Quality & Verification
# ---------------------------------------------------------------------------

# Run full pre-commit quality gate (format -> fix -> lint -> types -> tests)
pre-commit: fmt fix lint typecheck test

# Format code with ruff
fmt:
    uv run ruff format src/ tests/ agent/ mcp/

# Apply autofixes for ruff linter issues
fix:
    uv run ruff check --fix src/ tests/ agent/ mcp/

# Check code style and rules with ruff
lint:
    uv run ruff check src/ tests/ agent/ mcp/

# Static type check with mypy
typecheck:
    uv run mypy src/

# Run pytest test suite
test *args="":
    uv run pytest -v {{args}}

# Run test suite with coverage report
test-cov:
    uv run pytest -v --cov=src/sepa_pipeline --cov-report=term-missing

# ---------------------------------------------------------------------------
# Environment & Lakehouse Infrastructure
# ---------------------------------------------------------------------------

# Synchronize all dependency groups with uv
sync:
    uv sync --all-groups

# Start local infrastructure (RustFS, Polaris, PostgreSQL)
up:
    docker compose up -d

# Stop local infrastructure
down:
    docker compose down

# View logs for local infrastructure services
logs *services="":
    docker compose logs -f {{services}}

# Bootstrap local lakehouse bucket and Polaris catalog
bootstrap:
    uv run python -m sepa_pipeline.utils.bootstrap_lakehouse

# Setup BigQuery datasets and BigLake tables
setup-bq:
    uv run python -m sepa_pipeline.utils.setup_bigquery

# ---------------------------------------------------------------------------
# Pipeline & Operations
# ---------------------------------------------------------------------------

# Run SEPA ingestion pipeline
run *args="":
    uv run python -m sepa_pipeline.pipeline {{args}}

# Run pipeline scraper only
scrape *args="":
    uv run python -m sepa_pipeline.pipeline --scrape-only {{args}}

# Verify Silver Iceberg tables
verify *args="":
    uv run python -m sepa_pipeline.utils.verify_silver {{args}}

# Run dbt commands (defaults to `dbt run`)
dbt *args="run":
    uv run dbt {{args}} --project-dir dbt/sepa_analytics --profiles-dir dbt
