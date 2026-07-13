"""Tests for bronze ParquetLoader: exists, staging commit, force-rebuild semantics."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
from pyarrow import fs

from sepa_pipeline.loaders.parquet_loader import (
    SUCCESS_MARKER,
    TABLE_TYPES,
    ParquetLoader,
)


FECHA = date(2026, 4, 4)


@pytest.fixture
def local_fs(tmp_path: Path) -> fs.SubTreeFileSystem:
    """Filesystem rooted at tmp_path so S3-style keys map to local files."""
    return fs.SubTreeFileSystem(str(tmp_path), fs.LocalFileSystem())


@pytest.fixture
def config() -> SimpleNamespace:
    return SimpleNamespace(minio_bucket="sepa-lakehouse")


@pytest.fixture
def loader(config: SimpleNamespace, local_fs: fs.SubTreeFileSystem) -> ParquetLoader:
    return ParquetLoader(config, filesystem=local_fs)  # type: ignore[arg-type]


def _write_csv(path: Path, header: str, *rows: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def _sample_csv_paths(tmp_path: Path) -> list[dict[str, Path]]:
    """One child-ZIP worth of minimal CSVs for all three table types."""
    base = tmp_path / "extract" / "child1"
    comercio = base / "comercio.csv"
    sucursales = base / "sucursales.csv"
    productos = base / "productos.csv"

    _write_csv(
        comercio,
        "id_comercio|comercio_cuit|comercio_razon_social",
        "1|20123456789|Test SA",
    )
    _write_csv(
        sucursales,
        "id_comercio|id_bandera|id_sucursal|sucursales_nombre",
        "1|1|10|Sucursal Centro",
    )
    _write_csv(
        productos,
        "id_comercio|id_bandera|id_sucursal|id_producto|productos_ean|"
        "productos_descripcion|productos_precio_lista",
        "1|1|10|SKU1|7790001|Leche 1L|1500.50",
        "1|1|10|SKU2|7790002|Pan|800.00",
    )
    return [
        {
            "comercio": comercio,
            "sucursales": sucursales,
            "productos": productos,
        }
    ]


def test_exists_false_when_empty(loader: ParquetLoader) -> None:
    assert loader.exists(FECHA) is False


def _multi_child_csv_paths(tmp_path: Path) -> list[dict[str, Path]]:
    """Two child ZIPs so streamed write covers multi-batch ParquetWriter path."""
    paths = _sample_csv_paths(tmp_path)
    base = tmp_path / "extract" / "child2"
    comercio = base / "comercio.csv"
    sucursales = base / "sucursales.csv"
    productos = base / "productos.csv"
    _write_csv(
        comercio,
        "id_comercio|comercio_cuit|comercio_razon_social",
        "2|20999888777|Other SA",
    )
    _write_csv(
        sucursales,
        "id_comercio|id_bandera|id_sucursal|sucursales_nombre",
        "2|1|20|Sucursal Norte",
    )
    _write_csv(
        productos,
        "id_comercio|id_bandera|id_sucursal|id_producto|productos_ean|"
        "productos_descripcion|productos_precio_lista",
        "2|1|20|SKU9|7790009|Yerba|2100.00",
    )
    paths.append(
        {
            "comercio": comercio,
            "sucursales": sucursales,
            "productos": productos,
        }
    )
    return paths


def test_build_commits_and_exists(
    loader: ParquetLoader,
    local_fs: fs.SubTreeFileSystem,
    tmp_path: Path,
) -> None:
    audit = loader.build(_sample_csv_paths(tmp_path), FECHA)

    assert loader.exists(FECHA) is True
    assert audit["comercio"]["parquet_rows"] == 1
    assert audit["sucursales"]["parquet_rows"] == 1
    assert audit["productos"]["parquet_rows"] == 2

    # Final files + marker present; staging cleaned.
    prefix = loader._parquet_prefix(FECHA)
    for table in TABLE_TYPES:
        assert loader._file_exists(f"{prefix}/{table}.parquet")
        assert not loader._file_exists(f"{prefix}/.staging/{table}.parquet")
    assert loader._file_exists(f"{prefix}/{SUCCESS_MARKER}")

    dims = loader.read_dimensions(FECHA)
    assert dims["comercio"].height == 1
    assert dims["sucursales"].height == 1

    productos = list(loader.read_productos_batched(FECHA, batch_size=1))
    assert sum(df.height for df in productos) == 2


def test_streamed_build_merges_multiple_child_zips(
    loader: ParquetLoader,
    tmp_path: Path,
) -> None:
    """Streaming writer still produces one unified parquet per table."""
    audit = loader.build(_multi_child_csv_paths(tmp_path), FECHA)

    assert loader.exists(FECHA) is True
    assert audit["comercio"]["parquet_rows"] == 2
    assert audit["sucursales"]["parquet_rows"] == 2
    assert audit["productos"]["parquet_rows"] == 3

    dims = loader.read_dimensions(FECHA)
    assert dims["comercio"].height == 2
    assert sum(df.height for df in loader.read_productos_batched(FECHA)) == 3


def test_exists_requires_success_marker(
    loader: ParquetLoader,
    local_fs: fs.SubTreeFileSystem,
    tmp_path: Path,
) -> None:
    loader.build(_sample_csv_paths(tmp_path), FECHA)
    assert loader.exists(FECHA) is True

    loader._delete_if_exists(loader._success_path(FECHA))
    assert loader.exists(FECHA) is False


def test_exists_false_if_one_table_missing(
    loader: ParquetLoader,
    tmp_path: Path,
) -> None:
    loader.build(_sample_csv_paths(tmp_path), FECHA)
    loader._delete_if_exists(loader._parquet_path(FECHA, "productos"))
    # Marker still present but incomplete → not a cache hit.
    assert loader.exists(FECHA) is False


def test_partial_build_does_not_commit(
    loader: ParquetLoader,
    tmp_path: Path,
) -> None:
    """Missing one table type → no _SUCCESS, no final files."""
    paths = _sample_csv_paths(tmp_path)
    # Only comercio + sucursales (simulate missing productos CSVs).
    paths[0].pop("productos")

    audit = loader.build(paths, FECHA)

    assert audit["productos"]["parquet_rows"] == 0
    assert loader.exists(FECHA) is False
    for table in TABLE_TYPES:
        assert not loader._file_exists(loader._parquet_path(FECHA, table))
    assert not loader._file_exists(loader._success_path(FECHA))
    # Staging cleaned after incomplete build.
    for table in ("comercio", "sucursales"):
        assert not loader._file_exists(loader._staging_path(FECHA, table))


def test_failed_rebuild_preserves_committed_day(
    loader: ParquetLoader,
    tmp_path: Path,
) -> None:
    """After a good commit, a failed rebuild leaves the prior day intact."""
    loader.build(_sample_csv_paths(tmp_path), FECHA)
    assert loader.exists(FECHA) is True

    original = {
        t: pq.read_table(loader._parquet_path(FECHA, t), filesystem=loader._s3).num_rows
        for t in TABLE_TYPES
    }

    # Force a failure while staging productos (pyarrow FS methods are read-only).
    real_open = loader._open_output

    def boom(path: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if path.endswith("productos.parquet") and ".staging" in path:
            raise OSError("simulated staging failure")
        return real_open(path, *args, **kwargs)

    with patch.object(loader, "_open_output", side_effect=boom):
        with pytest.raises(OSError, match="simulated staging failure"):
            loader.build(_sample_csv_paths(tmp_path), FECHA)

    # Prior committed day still a cache hit.
    assert loader.exists(FECHA) is True
    for table, rows in original.items():
        table_data = pq.read_table(
            loader._parquet_path(FECHA, table), filesystem=loader._s3
        )
        assert table_data.num_rows == rows

    # Staging cleaned on failure.
    for table in TABLE_TYPES:
        assert not loader._file_exists(loader._staging_path(FECHA, table))


def test_rebuild_overwrites_committed_day(
    loader: ParquetLoader,
    tmp_path: Path,
) -> None:
    loader.build(_sample_csv_paths(tmp_path), FECHA)

    # Second build with extra productos row.
    paths = _sample_csv_paths(tmp_path)
    extra = paths[0]["productos"]
    content = extra.read_text(encoding="utf-8").rstrip("\n")
    extra.write_text(
        content + "\n1|1|10|SKU3|7790003|Yerba|2000.00\n",
        encoding="utf-8",
    )

    audit = loader.build(paths, FECHA)
    assert loader.exists(FECHA) is True
    assert audit["productos"]["parquet_rows"] == 3

    rows = sum(df.height for df in loader.read_productos_batched(FECHA, batch_size=10))
    assert rows == 3


def test_force_rebuild_pipeline_skips_cache(
    config: SimpleNamespace,
    local_fs: fs.SubTreeFileSystem,
    tmp_path: Path,
) -> None:
    """process_daily_data with force_rebuild_bronze ignores exists() hit."""
    from sepa_pipeline import pipeline as pipeline_mod

    loader = ParquetLoader(config, filesystem=local_fs)  # type: ignore[arg-type]
    loader.build(_sample_csv_paths(tmp_path), FECHA)
    assert loader.exists(FECHA) is True

    build_calls: list[date] = []

    def fake_build(self, all_csv_paths, fecha_vigencia):  # type: ignore[no-untyped-def]
        build_calls.append(fecha_vigencia)
        return {
            t: {"csv_rows": 0, "csv_cols": 0, "parquet_rows": 0} for t in TABLE_TYPES
        }

    class FakeExtractor:
        def fetch_from_bronze(self, target_date, config):  # type: ignore[no-untyped-def]
            return tmp_path / "raw"

        def extract_all_zips(self, raw_zip_dir, target_date):  # type: ignore[no-untyped-def]
            return _sample_csv_paths(tmp_path), 0, 0, 0

    (tmp_path / "raw").mkdir(exist_ok=True)

    with (
        patch.object(pipeline_mod, "ParquetLoader", return_value=loader),
        patch.object(pipeline_mod, "SEPAExtractor", FakeExtractor),
        patch.object(pipeline_mod, "_raw_zip_exists", return_value=True),
        patch.object(ParquetLoader, "build", fake_build),
        patch.object(pipeline_mod, "SEPAAuditWriter") as mock_audit_cls,
        patch.object(pipeline_mod, "SEPAValidator") as mock_validator_cls,
        patch.object(pipeline_mod, "IcebergLoader", return_value=None),
        patch.object(pipeline_mod, "BigQueryLoader", return_value=None),
    ):
        mock_audit = mock_audit_cls.return_value
        mock_validator = mock_validator_cls.return_value
        mock_validator.validate_comercio.side_effect = lambda df: df
        mock_validator.validate_sucursales.side_effect = lambda df: df
        mock_validator.validate_referential_integrity.side_effect = lambda c, s, p: (
            s,
            p,
        )
        mock_validator.validate_productos.side_effect = lambda df: df
        mock_validator.get_drop_stats.return_value = {
            "validation_dropped": 0,
            "integrity_dropped": 0,
            "negative_price_count": 0,
        }

        # Without force: cache hit → build not called.
        pipeline_mod.process_daily_data(
            FECHA,
            config,
            targets=[],
            force_rebuild_bronze=False,  # type: ignore[arg-type]
        )
        assert build_calls == []

        # With force: rebuild path runs build.
        pipeline_mod.process_daily_data(
            FECHA,
            config,
            targets=[],
            force_rebuild_bronze=True,  # type: ignore[arg-type]
        )
        assert build_calls == [FECHA]
        mock_audit.write_bronze.assert_called_once()


def test_cli_force_rebuild_bronze_flag() -> None:
    from sepa_pipeline.pipeline import parse_args

    with patch(
        "sys.argv",
        ["pipeline", "--force-rebuild-bronze", "--date", "2026-04-04"],
    ):
        args = parse_args()
    assert args.force_rebuild_bronze is True
    assert args.date == "2026-04-04"
