"""
SEPA Operations Agent — Built with Google ADK.

Importing this module has no side effects.  All network I/O (MCP transport
negotiation, Langfuse setup) is deferred to build_runtime(), which is called
automatically by run_prompt() on first use.

Typical usage
-------------
Programmatic / CLI::

    from agent.sepa_agent import run_prompt
    print(run_prompt("How many rows were loaded yesterday?"))

ADK web UI / adk run::

    adk run agent   # discovers root_agent via agent/__init__.__getattr__

"""

import os
import socket
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any


from dotenv import load_dotenv
from google.adk.agents import Agent
from google.adk.models import LiteLlm
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import McpToolset
from google.adk.tools.mcp_tool import (
    StdioConnectionParams,
    StreamableHTTPConnectionParams,
)
from google.genai import types

from agent.observability import configure_langfuse_tracing
from agent.prompt import SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Constants — safe at module level (no I/O)
# ---------------------------------------------------------------------------

APP_NAME = "sepa-agent"
DEFAULT_USER_ID = "local-user"
DEFAULT_SESSION_ID = "local-session-01"
_HTTP_PORT = 19121

# Toggle via SEPA_AGENT_LOCAL_MODEL=false to force the cloud model.
LOCAL_ENABLED: bool = os.getenv("SEPA_AGENT_LOCAL_MODEL", "true").lower() not in {
    "0",
    "false",
    "no",
}

SYSTEM_INSTRUCTIONS: str = SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Lazily-initialized runtime state
# ---------------------------------------------------------------------------
# All three are None until build_runtime() is first called.  Type checkers
# and readers should treat them as Optional; call build_runtime() (or
# run_prompt, which calls it internally) before accessing them.

root_agent: Agent | None = None
_session_service: InMemorySessionService | None = None
_runner: Runner | None = None


# ---------------------------------------------------------------------------
# Pure helpers (no I/O, safe to call in tests without mocking)
# ---------------------------------------------------------------------------


def is_port_open(host: str, port: int) -> bool:
    """Return True if host:port accepts a TCP connection within 0.5 s."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            return s.connect_ex((host, port)) == 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Agent env loader
# ---------------------------------------------------------------------------


def _load_agent_env() -> None:
    """Load ``agent/.env.agent`` if it exists.

    Uses ``override=False`` so values already set in the process environment
    (e.g. from a CI system or a parent shell export) are never overwritten.
    The file is optional — the agent runs fine without it as long as the
    required variables are present by other means.
    """
    env_path = Path(__file__).resolve().parent / ".env.agent"
    if env_path.exists():
        load_dotenv(env_path, override=False)


# ---------------------------------------------------------------------------
# Runtime factory
# ---------------------------------------------------------------------------


def _resolve_connection_params() -> (
    StreamableHTTPConnectionParams | StdioConnectionParams
):
    """Probe for the HTTP MCP server; fall back to a stdio subprocess."""
    if is_port_open("127.0.0.1", _HTTP_PORT):
        print(
            f"Connecting to running Streamable HTTP MCP server on port {_HTTP_PORT}..."
        )
        return StreamableHTTPConnectionParams(url=f"http://127.0.0.1:{_HTTP_PORT}/mcp")
    print("HTTP server offline. Spawning local MCP server via Stdio...")
    from mcp import StdioServerParameters

    return StdioConnectionParams(
        server_params=StdioServerParameters(
            command="uv",
            args=["run", "python", "-m", "lakehouse_mcp.stdio"],
        )
    )


def build_runtime(*, local: bool | None = None) -> Runner:
    """Initialize the agent, session service, and runner (idempotent).

    All network I/O — port probing, MCP connection negotiation, Langfuse
    setup — is confined to this function.  Subsequent calls return the
    existing runner without repeating initialization.

    Args:
        local: Override the LOCAL_ENABLED flag.  Pass ``False`` to force the
               cloud Gemini model even when SEPA_AGENT_LOCAL_MODEL is set.

    Returns:
        The initialized ADK Runner.
    """
    global root_agent, _session_service, _runner

    if _runner is not None:
        return _runner

    _load_agent_env()
    configure_langfuse_tracing()

    use_local = local if local is not None else LOCAL_ENABLED
    connection_params = _resolve_connection_params()
    audit_tools = McpToolset(connection_params=connection_params)

    model: LiteLlm | str = (
        LiteLlm(
            model=f"openai/{os.getenv('LOCAL_THINKING_MODEL')}",
            api_base=os.getenv("LOCAL_ENDPOINT", "http://127.0.0.1:8091/v1"),
            api_key=os.getenv("LOCAL_LLM_API_KEY", "sk-no-key"),
        )
        if use_local
        else "gemini-2.5-flash"
    )

    root_agent = Agent(
        name="sepa_ops_assistant",
        model=model,
        instruction=SYSTEM_INSTRUCTIONS,
        tools=[audit_tools],
    )

    _session_service = InMemorySessionService()
    _runner = Runner(
        agent=root_agent, app_name=APP_NAME, session_service=_session_service
    )
    return _runner


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _run_sync(coro: Any) -> Any:
    import asyncio
    import concurrent.futures

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor() as executor:
            return executor.submit(asyncio.run, coro).result()
    else:
        return asyncio.run(coro)


def run_prompt(
    prompt: str,
    *,
    user_id: str = DEFAULT_USER_ID,
    session_id: str = DEFAULT_SESSION_ID,
) -> str:
    """Run one prompt through the ADK runner so session context is traceable.

    Initializes the runtime on first call (no-op on subsequent calls).
    Sessions are created on first use and reused on subsequent calls, so
    multi-turn conversational context accumulates correctly within a
    (user_id, session_id) pair.

    Args:
        prompt: The user's natural-language query.
        user_id: Identifies the caller; used to scope session state.
        session_id: Identifies the conversation thread within user_id.

    Returns:
        The agent's final response text.
    """
    runner = build_runtime()

    svc = _session_service
    if svc is None:
        # Should never happen: build_runtime() always sets _session_service.
        raise RuntimeError("Internal error: session service was not initialized.")

    if (
        _run_sync(
            svc.get_session(app_name=APP_NAME, user_id=user_id, session_id=session_id)
        )
        is None
    ):
        _run_sync(
            svc.create_session(
                app_name=APP_NAME, user_id=user_id, session_id=session_id
            )
        )

    user_msg = types.Content(role="user", parts=[types.Part(text=prompt)])
    final_text = ""
    for event in runner.run(
        user_id=user_id,
        session_id=session_id,
        new_message=user_msg,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            final_text = event.content.parts[0].text or ""
    return final_text


async def _ensure_session(
    user_id: str, session_id: str
) -> None:
    """Create the session if it does not already exist (async)."""
    svc = _session_service
    if svc is None:
        raise RuntimeError("Internal error: session service was not initialized.")
    existing = await svc.get_session(
        app_name=APP_NAME, user_id=user_id, session_id=session_id
    )
    if existing is None:
        await svc.create_session(
            app_name=APP_NAME, user_id=user_id, session_id=session_id
        )


def _event_to_dict(event: Any) -> dict[str, Any]:
    """Convert an ADK Event into a plain dict suitable for SSE serialization."""
    from google.adk.events.event import Event as _Event

    assert isinstance(event, _Event)

    # Tool calls (function_call parts)
    func_calls = event.get_function_calls()
    if func_calls:
        fc = func_calls[0]
        return {
            "type": "tool_call",
            "content": f"Calling tool: {fc.name}",
            "tool": fc.name,
            "args": dict(fc.args) if fc.args else {},
        }

    # Tool results (function_response parts)
    func_responses = event.get_function_responses()
    if func_responses:
        fr = func_responses[0]
        # Truncate large tool results to keep SSE payloads manageable.
        response_str = str(fr.response) if fr.response else ""
        if len(response_str) > 2000:
            response_str = response_str[:2000] + "... (truncated)"
        return {
            "type": "tool_result",
            "content": response_str,
            "tool": fr.name,
        }

    # Final response
    if event.is_final_response():
        text = ""
        if event.content and event.content.parts:
            text = event.content.parts[0].text or ""
        return {"type": "final", "content": text}

    # Intermediate / progress
    text = ""
    if event.content and event.content.parts:
        text = event.content.parts[0].text or ""
    return {"type": "progress", "content": text or "Agent is thinking..."}


async def arun_prompt_stream(
    prompt: str,
    *,
    user_id: str = DEFAULT_USER_ID,
    session_id: str = DEFAULT_SESSION_ID,
) -> AsyncGenerator[dict[str, Any], None]:
    """Async generator that yields structured event dicts from the ADK runner.

    Each yielded dict has at minimum ``{"type": ..., "content": ...}`` and
    matches the ``SSEEvent`` schema defined in ``agent.api.schemas``.

    This is the primary interface used by the FastAPI streaming endpoint.
    The existing sync ``run_prompt()`` remains available for CLI / testing.

    Args:
        prompt: The user's natural-language query.
        user_id: Identifies the caller; used to scope session state.
        session_id: Identifies the conversation thread within user_id.

    Yields:
        Dicts representing agent events (progress, tool_call, tool_result,
        final, or error).
    """
    runner = build_runtime()
    await _ensure_session(user_id, session_id)

    user_msg = types.Content(role="user", parts=[types.Part(text=prompt)])

    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=user_msg,
    ):
        yield _event_to_dict(event)


if __name__ == "__main__":
    user_text = (
        " ".join(sys.argv[1:]) if len(sys.argv) > 1 else input("What do you need?: ")
    )
    print(run_prompt(user_text))
