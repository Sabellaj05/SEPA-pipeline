"""
verify_silver.py — Silver layer health check

Connects to an Iceberg catalog, loads each Silver table filtered to a
target date, and reports:

  - Row count and schema match against Silver schema definitions
  - Partition pruning effectiveness (files scanned vs total)
  - Null audit on key columns (grain keys, required metrics)
  - 3-row sample

Usage:
    uv run python -m sepa_pipeline.utils.verify_silver
    uv run python -m sepa_pipeline.utils.verify_silver --date 2026-04-01
    uv run python -m sepa_pipeline.utils.verify_silver --catalog bigquery
"""

import argparse
import sys
from datetime import date, datetime
from typing import Dict

import polars as pl
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.schema import (
    SILVER_DIM_COMERCIOS_SCHEMA,
    SILVER_DIM_PRODUCTOS_SCHEMA,
    SILVER_DIM_SUCURSALES_SCHEMA,
    SILVER_PRECIOS_SCHEMA,
)

# ── Columns that must be non-null for the table to be considered valid ────────
NULL_CHECKS: Dict[str, list[str]] = {
    "sepa.precios":        ["id_comercio", "id_bandera", "id_sucursal", "id_producto",
                            "precio_lista", "descripcion", "marca"],
    "sepa.dim_sucursales": ["id_comercio", "id_bandera", "id_sucursal", "provincia"],
    "sepa.dim_comercios":  ["id_comercio", "id_bandera", "cuit", "razon_social"],
    "sepa.dim_productos":  ["id_producto", "descripcion", "marca"],
}

SILVER_SCHEMAS = {
    "sepa.precios":        SILVER_PRECIOS_SCHEMA,
    "sepa.dim_sucursales": SILVER_DIM_SUCURSALES_SCHEMA,
    "sepa.dim_comercios":  SILVER_DIM_COMERCIOS_SCHEMA,
    "sepa.dim_productos":  SILVER_DIM_PRODUCTOS_SCHEMA,
}

SEP = "─" * 60


def _hdr(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def _ok(msg: str) -> None:
    print(f"  ✓  {msg}")


def _warn(msg: str) -> None:
    print(f"  ⚠  {msg}")


def _fail(msg: str) -> None:
    print(f"  ✗  {msg}")


def check_schema(identifier: str, actual_cols: list[str]) -> bool:
    expected = list(SILVER_SCHEMAS[identifier].keys())
    missing = [c for c in expected if c not in actual_cols]
    extra = [c for c in actual_cols if c not in expected]
    ok = True
    if missing:
        _fail(f"Missing columns: {missing}")
        ok = False
    if extra:
        _warn(f"Extra columns (not in Silver schema): {extra}")
    if not missing:
        _ok(f"Schema OK — {len(actual_cols)} columns match Silver definition")
    return ok


def check_partition_pruning(catalog: Catalog, identifier: str, target_date: date) -> None:
    """
    Compare files scanned with vs without a date filter.
    For a partitioned table, a filtered scan should touch fewer files.
    """
    table = catalog.load_table(identifier)
    total_files = len(list(table.scan().plan_files()))
    pruned_files = len(list(table.scan(row_filter=EqualTo("fecha_vigencia", target_date)).plan_files()))

    if total_files == 0:
        _warn("No data files found in table")
        return

    pruning_pct = 100 * (1 - pruned_files / total_files)
    msg = f"Partition pruning: {pruned_files}/{total_files} files scanned ({pruning_pct:.0f}% pruned)"

    if identifier == "sepa.precios" and pruned_files == total_files:
        _warn(f"{msg}  ← expected pruning for partitioned table; check partition spec")
    else:
        _ok(msg)


def check_nulls(df: pl.DataFrame, identifier: str) -> bool:
    required = NULL_CHECKS.get(identifier, [])
    all_ok = True
    for col in required:
        if col not in df.columns:
            _fail(f"NULL check skipped — column '{col}' not in DataFrame")
            all_ok = False
            continue
        null_count = df[col].null_count()
        if null_count > 0:
            pct = 100 * null_count / len(df)
            _fail(f"'{col}': {null_count:,} nulls ({pct:.1f}%)")
            all_ok = False
        else:
            _ok(f"'{col}': 0 nulls")
    return all_ok


def verify_table(catalog: Catalog, identifier: str, target_date: date) -> bool:
    _hdr(identifier)

    # Normalize to "sepa.<table>" for schema/null-check lookups regardless of catalog namespace
    lookup_key = "sepa." + identifier.split(".")[-1]

    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        _fail(f"Table not found in catalog")
        return False

    # Schema check (against Iceberg table metadata, not a scan)
    actual_cols = [f.name for f in table.schema().fields]
    check_schema(lookup_key, actual_cols)

    # Partition pruning (uses plan_files — no data read yet)
    if identifier == "sepa.precios":
        check_partition_pruning(catalog, identifier, target_date)

    # Scan filtered to target date — exercises partition pruning on read
    print(f"\n  Scanning {identifier} for {target_date} ...")
    scan = table.scan(row_filter=EqualTo("fecha_vigencia", target_date))
    try:
        arrow = scan.to_arrow()
    except Exception as e:
        _fail(f"Scan failed: {e}")
        return False

    df = pl.from_arrow(arrow)

    if df.is_empty():
        _warn(f"No rows found for {target_date}")
        return False

    _ok(f"Row count: {len(df):,}")

    # Null audit
    print()
    all_ok = check_nulls(df, lookup_key)

    # Extra checks per table
    if lookup_key == "sepa.dim_productos":
        unique_ids = df["id_producto"].n_unique() if "id_producto" in df.columns else 0
        duplicates = len(df) - unique_ids
        if duplicates > 0:
            _warn(f"id_producto: {duplicates:,} duplicate rows (expected 0 — dedup not applied?)")
        else:
            _ok(f"id_producto: {unique_ids:,} unique values (no duplicates)")

    if lookup_key == "sepa.precios" and "precio_lista" in df.columns:
        low = df.filter(pl.col("precio_lista") < 10.0)
        if len(low) > 0:
            _warn(f"precio_lista < 10.0: {len(low):,} rows (gold filter threshold)")

    if lookup_key == "sepa.dim_sucursales" and "provincia" in df.columns:
        null_prov = df["provincia"].null_count()
        provinces = df["provincia"].drop_nulls().n_unique()
        _ok(f"provincia: {provinces} distinct values, {null_prov} nulls")
        # Flag any values that are not ISO 3166-2 format (AR-X)
        prov = df["provincia"].drop_nulls()
        non_iso = prov.filter(~prov.str.contains(r"^AR-[A-Z]$"))
        if len(non_iso) > 0:
            bad = non_iso.unique().to_list()
            _fail(f"provincia: non-ISO values found — {bad} (expected AR-X format)")

    # Sample
    print(f"\n  Sample (3 rows):")
    sample_cols = [c for c in actual_cols if c in df.columns][:8]
    print(df.select(sample_cols).head(3).to_pandas().to_string(index=False))

    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Silver Iceberg tables for a given date.")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--catalog",
        choices=["nessie", "bigquery"],
        default="nessie",
        help="Catalog to verify against (default: nessie)",
    )
    args = parser.parse_args()

    target_date = (
        datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    )

    config = SEPAConfig()

    print(f"\n{'═' * 60}")
    print(f"  SEPA Silver Verification — {target_date}  [{args.catalog}]")
    print(f"{'═' * 60}")

    if args.catalog == "bigquery":
        catalog = load_catalog("default", **config.bigquery_catalog_config)
        tables = [
            f"{config.gcp_dataset}.precios",
            f"{config.gcp_dataset}.dim_sucursales",
            f"{config.gcp_dataset}.dim_comercios",
            f"{config.gcp_dataset}.dim_productos",
        ]
    else:
        catalog = load_catalog("default", **config.iceberg_catalog_config)
        tables = list(NULL_CHECKS.keys())  # sepa.precios, sepa.dim_*

    results = {}
    for identifier in tables:
        results[identifier] = verify_table(catalog, identifier, target_date)

    # Summary
    print(f"\n{'═' * 60}")
    print("  Summary")
    print(f"{'═' * 60}")
    all_passed = True
    for identifier, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}  {identifier}")
        if not passed:
            all_passed = False

    print()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
