"""Tests for silver precios append buffering (fewer, larger Iceberg data files)."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl

from sepa_pipeline.loaders.bigquery_loader import BigQueryLoader
from sepa_pipeline.loaders.iceberg_loader import IcebergLoader


FECHA = date(2026, 7, 10)


def _precios_df(n: int, start: int = 0) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_comercio": [str(i) for i in range(start, start + n)],
            "id_bandera": ["1"] * n,
            "id_sucursal": ["1"] * n,
            "id_producto": [f"SKU{i}" for i in range(start, start + n)],
            "precio_lista": [100.0] * n,
        }
    )


def _make_iceberg_loader(target_rows: int = 1000) -> IcebergLoader:
    loader = IcebergLoader.__new__(IcebergLoader)
    loader.config = SimpleNamespace()
    loader.stage_name = "IcebergLoader"
    loader.catalog = object()
    loader._iceberg_table = MagicMock()
    loader._dim_tables = {}
    loader._seen_productos = set()
    loader._precios_buffer = []
    loader._precios_buffer_rows = 0
    loader._precios_append_target_rows = target_rows
    loader._productos_buffer = []
    return loader


def _make_bq_loader(target_rows: int = 1000) -> BigQueryLoader:
    loader = BigQueryLoader.__new__(BigQueryLoader)
    loader.config = SimpleNamespace()
    loader.stage_name = "BigQueryLoader"
    loader.catalog = object()
    loader._iceberg_table = MagicMock()
    loader._dim_tables = {}
    loader._seen_productos = set()
    loader._namespace = "silver"
    loader._precios_buffer = []
    loader._precios_buffer_rows = 0
    loader._precios_append_target_rows = target_rows
    loader._productos_buffer = []
    return loader


def test_iceberg_buffers_until_target_then_appends() -> None:
    loader = _make_iceberg_loader(target_rows=1000)
    append_sizes: list[int] = []

    def capture(df: pl.DataFrame, fecha_vigencia: date) -> None:
        append_sizes.append(len(df))

    loader._append_precios = capture  # type: ignore[method-assign]

    loader.load(_precios_df(600), FECHA)
    assert append_sizes == []
    assert loader._precios_buffer_rows == 600

    loader.load(_precios_df(500, start=600), FECHA)
    # 600 + 500 >= 1000 → one flush
    assert append_sizes == [1100]
    assert loader._precios_buffer_rows == 0


def test_iceberg_flush_writes_remainder() -> None:
    loader = _make_iceberg_loader(target_rows=10_000)
    append_sizes: list[int] = []
    loader._append_precios = (  # type: ignore[method-assign]
        lambda df, fecha: append_sizes.append(len(df))
    )

    loader.load(_precios_df(100), FECHA)
    loader.load(_precios_df(50, start=100), FECHA)
    assert append_sizes == []

    loader.flush(FECHA)
    assert append_sizes == [150]
    assert loader._precios_buffer_rows == 0

    # Second flush is a no-op
    loader.flush(FECHA)
    assert append_sizes == [150]


def test_bigquery_buffer_mirrors_iceberg() -> None:
    loader = _make_bq_loader(target_rows=100)
    append_sizes: list[int] = []
    loader._append_precios = (  # type: ignore[method-assign]
        lambda df, fecha: append_sizes.append(len(df))
    )

    loader.load(_precios_df(40), FECHA)
    loader.load(_precios_df(40, start=40), FECHA)
    assert append_sizes == []
    loader.load(_precios_df(40, start=80), FECHA)
    assert append_sizes == [120]
    loader.flush(FECHA)
    assert append_sizes == [120]
