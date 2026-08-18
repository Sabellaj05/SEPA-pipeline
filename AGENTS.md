# AGENTS.md — SEPA Pipeline

## Project

Async pipeline scraping SEPA (Argentine govt price data) into a Medallion Lakehouse: RustFS (Bronze) → Iceberg/Polaris (Silver) → dbt + DuckDB/BigQuery (Gold). Process ~15M rows/day in ~1.5 GB peak memory.

## Setup

```bash
uv sync --all-groups          # install everything (core + dev + test)
docker compose up -d          # starts RustFS + Polaris + Postgres (creates bucket automatically)
just bootstrap                # first-time only (or: uv run python -m sepa_pipeline.utils.bootstrap_lakehouse)
uv run python -m sepa_pipeline.utils.setup_bigquery        # if using BigQuery
```

## Quality

```bash
just pre-commit               # ruff format → ruff check --fix → mypy src/ → pytest -v
just test                     # tests only
uv run pytest tests/test_schema.py -v   # single test file
```

- **Python**: `>=3.12, <3.14` (dbt compatibility cap)
- **mypy**: very strict — `disallow_untyped_defs`, `strict_equality`, `warn_unreachable`, etc.
- **ruff**: line-length 88 (E501 ignored), rules E/W/F only, `isort` first-party: `agent,lakehouse_mcp,sepa_pipeline,serving_mcp`
- **Coverage**: `--cov-fail-under=60`, but `loaders/*`, `utils/*`, `pipeline.py`, `manage_iceberg.py` are omitted from coverage
- **No CI**: no GitHub Actions — testing is manual

## Task Runner (`just`)

Use `just` as the canonical task runner for development and quality checks:

- `just pre-commit`: Full quality gate (`fmt` -> `fix` -> `lint` -> `typecheck` -> `test`).
- `just fmt` / `just fix` / `just lint`: Ruff formatting and rule checks (`E501` is ignored).
- `just typecheck`: Strict MyPy static type checking across `src/`.
- `just test [args]`: PyTest suite runner with optional argument forwarding (e.g. `just test tests/test_env.py`).
- `just up` / `just down` / `just bootstrap`: Local Docker stack (RustFS + Polaris + Postgres) management.
- `just run [args]` / `just scrape [args]` / `just verify [args]`: Pipeline execution and verification.
- `just dbt [args]`: Run dbt commands (defaults to `dbt run`).

## Git & Branching Workflow

- **Never commit directly to `main`** for new features, bugfixes, or refactors.
- **Linear Issue**: Every branch must correspond to a Linear issue (`SEP-XXX`). If none exists, create one in the `SEPA` team under the active project.
- **Branch Naming**: `sep-<id>-<short-description>` (e.g. `sep-295-rustfs-cleanup-justfile`).
- **Commit Format**: `[SEP-XXX]: <Imperative summary description>` (e.g. `[SEP-295]: Finalize RustFS renaming cleanup`).
- **PR Description**: Generate standardized PR descriptions using the `generate-pr-desc` skill and write to `sep-<id>-PR-desc.md`.

## Gotchas

- **`pyiceberg` pinned locally**: `[tool.uv.sources]` overrides it to `../../../../opensource-contribution/iceberg-python` (editable). Clone/path must exist or override must be removed before `uv sync` works elsewhere.
- **Polaris REST Catalog**: Uses Apache Polaris (`http://localhost:8181/api/catalog/v1`) with standard OAuth2 / client credentials.
- **No `main.py`** anymore — entry point is `uv run python -m sepa_pipeline.pipeline` (or `just run`)
- **CSV quirk**: SEPA files are pipe-delimited (`|`), utf8, with a footer line `Ultima actualización: ...` that must be stripped. Validator handles this.
- **Fecha uses AR timezone** (UTC-3) — weekday names are Spanish (`lunes`, `martes`, etc.). Used for scraper URL matching.

## Architecture

| Layer | Location | Format |
|-------|----------|--------|
| Bronze Raw | `s3://sepa-lakehouse/bronze/raw/year=/month=/day=/` | ZIP |
| Bronze Parquet | `s3://sepa-lakehouse/bronze/parquet/year=/month=/day=/` | Zstd Parquet |
| Silver | `sepa.precios` (partitioned Day(fecha_vigencia)) + `sepa.dim_*` | Iceberg via Polaris |
| Gold | `dbt/sepa_analytics/models/{staging,intermediate,marts,gold}/` | dbt + DuckDB/BQ |

- **Productos**: bronze staged CSV-by-CSV (no full-day concat); silver reads bronze in 500k-row batches
- **Referential integrity**: validated in-memory by `validator.py`; orphans dropped and audited
- **Pipeline CLI**: `uv run python -m sepa_pipeline.pipeline [--date YYYY-MM-DD] [--target iceberg,bigquery] [--date-from X --date-to Y] [--scrape-only] [--maintain-iceberg] [--force-rebuild-bronze]`
- **Verify**: `uv run python -m sepa_pipeline.utils.verify_silver [--date YYYY-MM-DD] [--catalog bigquery]`

## Loaders

- `IcebergLoader` → local RustFS/Polaris
- `BigQueryLoader` → GCS/BigLake (requires GCP credentials in `.secret/`)
- `ParquetLoader` → Bronze Parquet (always runs first)
  - Writes under `bronze/parquet/.../.staging/`, then commits all three tables and a `_SUCCESS` marker
  - Productos (and dims) staged via streaming `ParquetWriter` (one child CSV at a time → one unified parquet)
  - `exists()` requires `_SUCCESS` + `comercio`/`sucursales`/`productos` parquet (incomplete days are never cache hits)
  - `--force-rebuild-bronze` ignores cache and rebuilds from raw ZIP (scrapes only if raw missing)
  - Days committed before `_SUCCESS` existed look like cache misses once; rebuild from raw ZIP once
- Silver `precios` appends are buffered to ~2M rows (`PRECIOS_APPEND_TARGET_ROWS`) before each Iceberg/BQ append, then `flush()` at end of day — fewer/larger data files than per-500k appends
- Export `PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID=True` is set in `BigQueryLoader` (workaround for BQ catalog)

## Packages built by hatchling

- `src/sepa_pipeline` (main)
- `agent/`
- `mcp/lakehouse_mcp`
- `mcp/serving_mcp`

## dbt

```bash
uv run dbt run --project-dir dbt/sepa_analytics --profiles-dir dbt
```

- Local target: DuckDB (with `httpfs` + `iceberg` extensions, reads RustFS directly)
- Prod target: BigQuery (service account key in `.secret/`)
- Staging models are views that `SELECT *` from Iceberg source via `iceberg_source()` macro
- Intermediate models materialize deduplicated dimension snapshots as tables
- Marathon variable `start_date: '2025-01-01'` limits BigQuery full scan

## Tests

- `pytest-asyncio` for async scraper tests
- `factories.py` builds realistic nested ZIPs for integration testing
- Fixtures in `conftest.py`: `tmp_path` for temp dirs, `mock_httpx_response`, `sample_url`

## Names not to create

- `AGENT.md` (singular - old file, replace/delete)
- Files under `docs/`, `data/`, `logs/`, `.secret/`, `.agent/` are gitignored
- Any `*.duckdb` files are gitignored
