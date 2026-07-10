from typing import Any

import polars as pl

from lakehouse_mcp.tools import query


class _FakeResult:
    def pl(self) -> pl.DataFrame:
        return pl.DataFrame({"ok": [True]})


class _FakeConnection:
    def __init__(self) -> None:
        self.executed_sql = ""

    def execute(self, sql: str) -> _FakeResult:
        self.executed_sql = sql
        return _FakeResult()


def test_run_query_normalizes_sepa_prefix_for_registered_views(
    monkeypatch: Any,
) -> None:
    conn = _FakeConnection()
    monkeypatch.setattr(query, "get_connection", lambda: conn)
    monkeypatch.setattr(
        query,
        "get_registered_views",
        lambda: frozenset({"precios", "dim_comercios"}),
    )

    result = query.run_query(
        """
        SELECT p.descripcion, c.nombre
        FROM sepa.precios p
        JOIN sepa.dim_comercios c ON p.id_comercio = c.id_comercio
        """,
    )

    assert result == [{"ok": True}]
    assert "sepa.precios" not in conn.executed_sql
    assert "sepa.dim_comercios" not in conn.executed_sql
    assert "FROM precios p" in conn.executed_sql
    assert "JOIN dim_comercios c" in conn.executed_sql

