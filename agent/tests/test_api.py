"""Tests for the Agent HTTP API (FastAPI).

These tests mock the ADK runner so they can run without MCP, MinIO, or
a running LLM server.  They verify request/response contracts, SSE
streaming format, session management, and error handling.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from agent.api.schemas import ChatRequest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch build_runtime so the app starts without real MCP/LLM."""
    from agent import sepa_agent

    mock_agent = MagicMock()
    mock_agent.name = "test_agent"

    mock_session_svc = MagicMock()
    # Make get_session and create_session proper async mocks
    mock_session_svc.get_session = AsyncMock(return_value=None)
    mock_session_svc.create_session = AsyncMock(return_value=MagicMock())

    mock_runner = MagicMock()

    monkeypatch.setattr(sepa_agent, "root_agent", mock_agent)
    monkeypatch.setattr(sepa_agent, "_session_service", mock_session_svc)
    monkeypatch.setattr(sepa_agent, "_runner", mock_runner)
    monkeypatch.setattr(
        sepa_agent,
        "build_runtime",
        lambda **kwargs: mock_runner,
    )
    monkeypatch.setattr(sepa_agent, "_load_agent_env", lambda: None)


@pytest.fixture()
def client(_mock_runtime: None) -> TestClient:
    """TestClient with mocked runtime — no real agent needed."""
    from agent.api.app import app

    return TestClient(app)


def _make_fake_events() -> list[dict[str, Any]]:
    """Return a list of event dicts simulating a typical agent interaction."""
    return [
        {"type": "progress", "content": "Agent is thinking..."},
        {
            "type": "tool_call",
            "content": "Calling tool: list_tables",
            "tool": "list_tables",
            "args": {"namespace": "sepa"},
        },
        {
            "type": "tool_result",
            "content": "['dim_productos', 'dim_sucursales']",
            "tool": "list_tables",
        },
        {"type": "final", "content": "There are 2 tables in the sepa namespace."},
    ]


async def _fake_arun_prompt_stream(
    prompt: str,
    *,
    user_id: str = "web-user",
    session_id: str = "test-session",
) -> AsyncGenerator[dict[str, Any], None]:
    """Async generator yielding fake events."""
    for event in _make_fake_events():
        yield event


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_returns_ok(self, client: TestClient) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["agent_name"] == "test_agent"
        assert "uptime_seconds" in data

    def test_health_has_model_field(self, client: TestClient) -> None:
        resp = client.get("/health")
        data = resp.json()
        assert "model" in data


# ---------------------------------------------------------------------------
# Non-streaming chat
# ---------------------------------------------------------------------------


class TestChat:
    @patch(
        "agent.api.routes.arun_prompt_stream",
        side_effect=_fake_arun_prompt_stream,
        create=True,
    )
    def test_chat_returns_final_response(
        self, mock_stream: MagicMock, client: TestClient
    ) -> None:
        # We need to patch the import inside routes.py
        with patch(
            "agent.sepa_agent.arun_prompt_stream",
            side_effect=_fake_arun_prompt_stream,
        ):
            resp = client.post(
                "/api/agent/chat",
                json={"prompt": "How many tables are there?"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["response"] == "There are 2 tables in the sepa namespace."
        assert "session_id" in data
        assert data["user_id"] == "web-user"

    def test_chat_rejects_empty_prompt(self, client: TestClient) -> None:
        resp = client.post(
            "/api/agent/chat",
            json={"prompt": ""},
        )
        assert resp.status_code == 422  # Pydantic validation

    def test_chat_uses_custom_user_id(self, client: TestClient) -> None:
        with patch(
            "agent.sepa_agent.arun_prompt_stream",
            side_effect=_fake_arun_prompt_stream,
        ):
            resp = client.post(
                "/api/agent/chat",
                json={
                    "prompt": "hello",
                    "user_id": "custom-user",
                    "session_id": "custom-session",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_id"] == "custom-user"
        assert data["session_id"] == "custom-session"


# ---------------------------------------------------------------------------
# SSE streaming
# ---------------------------------------------------------------------------


class TestStreaming:
    def test_stream_returns_sse_content_type(self, client: TestClient) -> None:
        with patch(
            "agent.sepa_agent.arun_prompt_stream",
            side_effect=_fake_arun_prompt_stream,
        ):
            resp = client.post(
                "/api/agent/stream",
                json={"prompt": "list tables"},
            )
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers["content-type"]

    def test_stream_yields_valid_sse_events(self, client: TestClient) -> None:
        with patch(
            "agent.sepa_agent.arun_prompt_stream",
            side_effect=_fake_arun_prompt_stream,
        ):
            resp = client.post(
                "/api/agent/stream",
                json={"prompt": "list tables"},
            )
        lines = resp.text.strip().split("\n")
        data_lines = [
            line for line in lines if line.startswith("data: ")
        ]

        # Last data line should be [DONE]
        assert data_lines[-1] == "data: [DONE]"

        # All other data lines should be valid JSON
        event_lines = data_lines[:-1]
        assert len(event_lines) >= 1

        for line in event_lines:
            payload = line.removeprefix("data: ")
            parsed = json.loads(payload)
            assert "type" in parsed
            assert "session_id" in parsed

    def test_stream_includes_final_event(self, client: TestClient) -> None:
        with patch(
            "agent.sepa_agent.arun_prompt_stream",
            side_effect=_fake_arun_prompt_stream,
        ):
            resp = client.post(
                "/api/agent/stream",
                json={"prompt": "list tables"},
            )
        lines = resp.text.strip().split("\n")
        data_lines = [
            line for line in lines
            if line.startswith("data: ") and line != "data: [DONE]"
        ]

        events = [
            json.loads(line.removeprefix("data: "))
            for line in data_lines
        ]
        types = [e["type"] for e in events]
        assert "final" in types

    def test_stream_handles_errors_gracefully(self, client: TestClient) -> None:
        async def _exploding_stream(
            prompt: str, **kwargs: Any
        ) -> AsyncGenerator[dict[str, Any], None]:
            yield {"type": "progress", "content": "starting"}
            raise RuntimeError("LLM connection failed")

        with patch(
            "agent.sepa_agent.arun_prompt_stream",
            side_effect=_exploding_stream,
        ):
            resp = client.post(
                "/api/agent/stream",
                json={"prompt": "will fail"},
            )
        assert resp.status_code == 200  # SSE always returns 200

        lines = resp.text.strip().split("\n")
        data_lines = [
            line for line in lines
            if line.startswith("data: ") and line != "data: [DONE]"
        ]

        events = [
            json.loads(line.removeprefix("data: "))
            for line in data_lines
        ]
        error_events = [e for e in events if e["type"] == "error"]
        assert len(error_events) == 1
        assert "LLM connection failed" in error_events[0]["content"]


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


class TestSessions:
    def test_list_sessions_empty(self, client: TestClient) -> None:
        resp = client.get("/api/agent/sessions/some-user")
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_id"] == "some-user"
        assert data["sessions"] == []

    def test_delete_session_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/agent/sessions/user1/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestSchemas:
    def test_chat_request_defaults(self) -> None:
        req = ChatRequest(prompt="hello")
        assert req.user_id == "web-user"
        assert req.session_id is None

    def test_chat_request_max_length(self) -> None:
        with pytest.raises(Exception):
            ChatRequest(prompt="x" * 4001)

    def test_chat_request_custom_fields(self) -> None:
        req = ChatRequest(
            prompt="query",
            user_id="u1",
            session_id="s1",
        )
        assert req.user_id == "u1"
        assert req.session_id == "s1"
