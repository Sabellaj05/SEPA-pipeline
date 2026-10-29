import asyncio
from typing import Any

import pytest


class _FakeCursor:
    def __init__(self, executed: list[tuple[str, Any]]) -> None:
        self.executed = executed

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[dict[str, Any]]:
        return [
            {
                "id_producto": "123",
                "descripcion": "Fideos Tallarines",
                "precio_lista": 1000.0,
                "score": 0.8,
            }
        ]

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    def cursor(self, *args: Any, **kwargs: Any) -> _FakeCursor:
        return _FakeCursor(self.executed)


def _tool_names(mcp: Any) -> list[str]:
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
    monkeypatch.setattr(search, "get_pg_connection", lambda: conn)
    monkeypatch.setattr(search, "_get_typesafe_client", lambda: None)

    result = search.search_products("fideos", limit=5, use_jev=False)

    assert len(result) == 1
    assert result[0]["descripcion"] == "Fideos Tallarines"
    sql, params = conn.executed[0]
    assert "serving.products" in sql
    assert "similarity" in sql
    assert params[0] == "fideos"


def test_search_products_propagates_missing_db(monkeypatch: Any) -> None:
    from serving_mcp.tools import search

    def boom() -> None:
        raise RuntimeError("serving db missing")

    monkeypatch.setattr(search, "get_pg_connection", boom)

    with pytest.raises(RuntimeError, match="serving db missing"):
        search.search_products("fideos")


def test_serving_app_is_mounted_at_public_mcp_endpoint() -> None:
    from serving_mcp.main import app, streamable_app

    mount_paths = [getattr(route, "path", None) for route in app.routes]
    mcp_paths = [getattr(route, "path", None) for route in streamable_app.routes]

    assert "" in mount_paths
    assert "/mcp" in mcp_paths
