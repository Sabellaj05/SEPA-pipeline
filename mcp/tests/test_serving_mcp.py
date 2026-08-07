import asyncio
from typing import Any

import polars as pl
import pytest


class _FakeResult:
    def pl(self) -> pl.DataFrame:
        return pl.DataFrame({"ok": [True]})


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, list[Any]]] = []

    def execute(self, sql: str, params: list[Any]) -> _FakeResult:
        self.executed.append((sql, params))
        return _FakeResult()


def _tool_names(mcp) -> list[str]:
    return [t.name for t in asyncio.run(mcp.list_tools())]


def test_serving_server_exposes_only_search_tool() -> None:
    from serving_mcp.server import mcp

    assert _tool_names(mcp) == ["search_products_tool"]


def test_lakehouse_server_no_longer_exposes_search_tool() -> None:
    from lakehouse_mcp.server import mcp

    names = _tool_names(mcp)
    assert "search_products_tool" not in names
    assert "run_query" in names


def test_search_products_builds_fts_query(monkeypatch: Any) -> None:
    from serving_mcp.tools import search

    conn = _FakeConnection()
    monkeypatch.setattr(search, "get_serving_connection", lambda: conn)

    result = search.search_products("fideos", limit=5)

    assert result == [{"ok": True}]
    sql, params = conn.executed[0]
    assert "fts_main_current_prices.match_bm25" in sql
    assert "FROM current_prices" in sql
    assert params == ["fideos", 5]


def test_search_products_propagates_missing_db(monkeypatch: Any) -> None:
    from serving_mcp.tools import search

    def boom() -> None:
        raise RuntimeError("serving db missing")

    monkeypatch.setattr(search, "get_serving_connection", boom)

    with pytest.raises(RuntimeError, match="serving db missing"):
        search.search_products("fideos")


def test_serving_app_is_mounted_at_public_mcp_endpoint() -> None:
    from serving_mcp.main import app, streamable_app

    mount_paths = [getattr(route, "path", None) for route in app.routes]
    mcp_paths = [getattr(route, "path", None) for route in streamable_app.routes]

    assert "" in mount_paths
    assert "/mcp" in mcp_paths
