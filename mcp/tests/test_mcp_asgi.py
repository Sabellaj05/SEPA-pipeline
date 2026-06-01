from contextlib import asynccontextmanager

import pytest


def test_mcp_app_is_mounted_at_public_mcp_endpoint() -> None:
    from lakehouse_mcp.main import app, streamable_app

    mount_paths = [getattr(route, "path", None) for route in app.routes]
    mcp_paths = [getattr(route, "path", None) for route in streamable_app.routes]

    assert "" in mount_paths
    assert "/mcp" in mcp_paths


@pytest.mark.asyncio
async def test_fastapi_lifespan_runs_mcp_lifespan_and_closes_duckdb(
    monkeypatch,
) -> None:
    from lakehouse_mcp import main

    calls = []

    @asynccontextmanager
    async def fake_mcp_lifespan(app):
        calls.append("mcp_start")
        try:
            yield
        finally:
            calls.append("mcp_stop")

    monkeypatch.setattr(main, "init_duckdb", lambda: calls.append("duckdb_start"))
    monkeypatch.setattr(main, "close_duckdb", lambda: calls.append("duckdb_stop"))
    monkeypatch.setattr(
        main.streamable_app.router,
        "lifespan_context",
        fake_mcp_lifespan,
    )

    async with main.lifespan(main.app):
        calls.append("app_running")

    assert calls == [
        "duckdb_start",
        "mcp_start",
        "app_running",
        "mcp_stop",
        "duckdb_stop",
    ]
