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

import json
import logging
import os
import re
import socket
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.adk.agents import Agent, SequentialAgent
from google.adk.agents.base_agent import BaseAgent
from google.adk.models import LiteLlm
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import McpToolset
from google.adk.tools.mcp_tool import (
    StdioConnectionParams,
    StreamableHTTPConnectionParams,
)
from google.genai import types
from pydantic import ValidationError

from agent.api.schemas import ShoppingList
from agent.intent_router import (
    classify_intent,
    extract_search_term,
    format_single_product_shopping_list,
    parse_ingredient_lines,
)
from agent.observability import configure_langfuse_tracing, load_langfuse_environment
from agent.shopping_prompts import (
    LAKEHOUSE_ANALYTICS_PROMPT,
    SHOPPING_CONVERSATION_PROMPT,
    SHOPPING_FORMATTER_PROMPT,
    SHOPPING_PLANNER_PROMPT,
    SHOPPING_RESEARCH_PROMPT,
)

logger = logging.getLogger(__name__)

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
    root_env = Path(__file__).resolve().parent.parent / ".env"
    if root_env.exists():
        load_dotenv(root_env, override=False)
    env_path = Path(__file__).resolve().parent / ".env.agent"
    if env_path.exists():
        load_dotenv(env_path, override=False)
    load_langfuse_environment()


# Eagerly load agent environment at import time so module-level constants
# reflect .env and .env.agent.
_load_agent_env()


def is_local_enabled() -> bool:
    """Return True if local LLM is enabled in environment."""
    return os.getenv("SEPA_AGENT_LOCAL_MODEL", "false").lower() not in {
        "0",
        "false",
        "no",
    }


def get_gemini_model() -> str:
    """Return the configured Gemini model name."""
    return os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")


# ---------------------------------------------------------------------------
# Constants — safe at module level (no I/O)
# ---------------------------------------------------------------------------


APP_NAME = "sepa-agent"
DEFAULT_USER_ID = "local-user"
DEFAULT_SESSION_ID = "local-session-01"
DEFAULT_GEMINI_MODEL: str = get_gemini_model()
_HTTP_PORT = 19121
_SERVING_HTTP_PORT = 19122
RESEARCH_AGENT_NAME = "sepa_shopping_researcher"
FORMATTER_AGENT_NAME = "sepa_shopping_formatter"

# Toggle via SEPA_AGENT_LOCAL_MODEL=false to force the cloud model.
LOCAL_ENABLED: bool = is_local_enabled()

SYSTEM_INSTRUCTIONS: str = SHOPPING_RESEARCH_PROMPT

# ---------------------------------------------------------------------------
# Lazily-initialized runtime state
# ---------------------------------------------------------------------------
# All three are None until build_runtime() is first called.  Type checkers
# and readers should treat them as Optional; call build_runtime() (or
# run_prompt, which calls it internally) before accessing them.

root_agent: BaseAgent | None = None
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
# Runtime factory
# ---------------------------------------------------------------------------


def _resolve_serving_connection_params() -> (
    StreamableHTTPConnectionParams | StdioConnectionParams
):
    """Probe for the serving (product search) HTTP MCP server; fall back to stdio."""
    if is_port_open("127.0.0.1", _SERVING_HTTP_PORT):
        print(f"Connecting to serving MCP server on port {_SERVING_HTTP_PORT}...")
        return StreamableHTTPConnectionParams(
            url=f"http://127.0.0.1:{_SERVING_HTTP_PORT}/mcp"
        )
    print("Serving HTTP server offline. Spawning local MCP server via Stdio...")
    from mcp import StdioServerParameters

    return StdioConnectionParams(
        server_params=StdioServerParameters(
            command="uv",
            args=["run", "python", "-m", "serving_mcp.stdio"],
        )
    )


def _resolve_lakehouse_connection_params() -> (
    StreamableHTTPConnectionParams | StdioConnectionParams
):
    """Probe for the lakehouse (ops/analytics) HTTP MCP server; fall back to stdio."""
    if is_port_open("127.0.0.1", _HTTP_PORT):
        print(f"Connecting to lakehouse MCP server on port {_HTTP_PORT}...")
        return StreamableHTTPConnectionParams(url=f"http://127.0.0.1:{_HTTP_PORT}/mcp")
    print("Lakehouse HTTP server offline. Spawning local MCP server via Stdio...")
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

    use_local = local if local is not None else is_local_enabled()
    serving_tools = McpToolset(
        connection_params=_resolve_serving_connection_params(),
        tool_filter=["search_products_tool"],
    )

    model: LiteLlm | str = (
        LiteLlm(
            model=f"openai/{os.getenv('LOCAL_THINKING_MODEL')}",
            api_base=os.getenv("LOCAL_ENDPOINT", "http://127.0.0.1:8091/v1"),
            api_key=os.getenv("LOCAL_LLM_API_KEY", "sk-no-key"),
        )
        if use_local
        else get_gemini_model()
    )

    planner_agent = Agent(
        name="sepa_recipe_planner",
        model=DEFAULT_GEMINI_MODEL,
        instruction=SHOPPING_PLANNER_PROMPT,
        output_key="canonical_shopping_list",
    )

    research_agent = Agent(
        name=RESEARCH_AGENT_NAME,
        model=model,
        instruction=SHOPPING_RESEARCH_PROMPT,
        tools=[serving_tools],
        output_key="shopping_research",
    )
    formatter_agent = Agent(
        name=FORMATTER_AGENT_NAME,
        model=DEFAULT_GEMINI_MODEL,
        instruction=SHOPPING_FORMATTER_PROMPT,
        output_schema=ShoppingList,
        output_key="shopping_list",
    )

    root_agent = SequentialAgent(
        name="sepa_shopping_assistant",
        sub_agents=[planner_agent, research_agent, formatter_agent],
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


async def _ensure_session(user_id: str, session_id: str) -> None:
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


_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


def _extract_json_payload(text: str) -> str:
    """Extract a JSON object from a raw LLM response."""
    block_match = _JSON_BLOCK_RE.search(text)
    if block_match:
        return block_match.group(1).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]

    return text.strip()


try:
    from langfuse import observe, propagate_attributes
except ImportError:
    from contextlib import nullcontext

    def observe(*args: Any, **kwargs: Any) -> Any:
        def decorator(fn: Any) -> Any:
            return fn

        return decorator

    def propagate_attributes(*args: Any, **kwargs: Any) -> Any:
        return nullcontext()


def _validated_final_event(text: str) -> dict[str, Any]:
    """Validate and normalize the final shopping-list response."""
    try:
        payload = _extract_json_payload(text)
        shopping_list = ShoppingList.model_validate_json(payload)
    except (json.JSONDecodeError, ValidationError, ValueError) as exc:
        return {
            "type": "error",
            "content": f"Agent returned invalid ShoppingList JSON: {exc}",
        }

    # Ensure conversational message is never empty so the UI always has text to display
    if not shopping_list.message or len(shopping_list.message.strip()) < 5:
        shopping_list.message = (
            f"¡Listo! Preparé la lista de compras para '{shopping_list.project_name}' "
            f"con los mejores precios encontrados en los supermercados de SEPA."
        )

    data = shopping_list.model_dump(mode="json")
    return {
        "type": "final",
        "content": shopping_list.model_dump_json(),
        "data": data,
    }


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
        if event.author != FORMATTER_AGENT_NAME:
            return {
                "type": "progress",
                "content": "Research complete. Structuring response...",
            }
        return _validated_final_event(text)

    # Intermediate / progress
    text = ""
    if event.content and event.content.parts:
        text = event.content.parts[0].text or ""
    return {"type": "progress", "content": text or "Agent is thinking..."}


@observe(name="sepa_recipe_planner", as_type="agent")
async def _run_planner_agent(
    prompt: str,
    user_id: str,
    session_id: str,
) -> str:
    """Run the recipe planner agent with Langfuse agent span."""
    global _session_service
    if _session_service is None:
        _session_service = InMemorySessionService()

    planner_agent = Agent(
        name="sepa_recipe_planner",
        model=DEFAULT_GEMINI_MODEL,
        instruction=SHOPPING_PLANNER_PROMPT,
    )
    planner_session_id = f"{session_id}__planner"
    await _ensure_session(user_id, planner_session_id)
    planner_runner = Runner(
        agent=planner_agent,
        app_name=APP_NAME,
        session_service=_session_service,
    )

    user_msg = types.Content(role="user", parts=[types.Part(text=prompt)])
    planner_text = ""
    async for event in planner_runner.run_async(
        user_id=user_id,
        session_id=planner_session_id,
        new_message=user_msg,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            planner_text = event.content.parts[0].text or ""
    return planner_text


@observe(name="sepa_shopping_formatter", as_type="agent")
async def _run_formatter_agent(
    prompt: str,
    research_brief: str,
    user_id: str,
    session_id: str,
) -> str:
    """Run the shopping formatter agent with Langfuse agent span."""
    global _session_service
    if _session_service is None:
        _session_service = InMemorySessionService()

    formatter_instruction = SHOPPING_FORMATTER_PROMPT.format(
        user_prompt=prompt,
        shopping_research=research_brief,
    )
    formatter_agent = Agent(
        name=FORMATTER_AGENT_NAME,
        model=DEFAULT_GEMINI_MODEL,
        instruction=formatter_instruction,
        output_schema=ShoppingList,
    )
    formatter_session_id = f"{session_id}__formatter"
    await _ensure_session(user_id, formatter_session_id)
    formatter_runner = Runner(
        agent=formatter_agent,
        app_name=APP_NAME,
        session_service=_session_service,
    )

    fmt_msg = types.Content(
        role="user",
        parts=[
            types.Part(
                text=f"Generar lista de compras estructurada y mensaje conversacional para: {prompt}"
            )
        ],
    )
    final_text = ""
    async for event in formatter_runner.run_async(
        user_id=user_id,
        session_id=formatter_session_id,
        new_message=fmt_msg,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            final_text = event.content.parts[0].text or ""
    return final_text


@observe(name="sepa_conversational_agent", as_type="agent")
async def _run_conversational_agent(
    prompt: str,
    user_id: str,
    session_id: str,
) -> str:
    """Run the conversational agent with Langfuse agent span."""
    global _session_service
    if _session_service is None:
        _session_service = InMemorySessionService()

    conv_agent = Agent(
        name="sepa_conversational_agent",
        model=get_gemini_model(),
        instruction=SHOPPING_CONVERSATION_PROMPT,
    )
    conv_session_id = f"{session_id}__conv"
    await _ensure_session(user_id, conv_session_id)
    conv_runner = Runner(
        agent=conv_agent,
        app_name=APP_NAME,
        session_service=_session_service,
    )

    user_msg = types.Content(role="user", parts=[types.Part(text=prompt)])
    conv_text = ""
    async for event in conv_runner.run_async(
        user_id=user_id,
        session_id=conv_session_id,
        new_message=user_msg,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            conv_text = event.content.parts[0].text or ""
    return conv_text


@observe(name="sepa_lakehouse_analyst", as_type="agent")
async def _run_lakehouse_agent(
    prompt: str,
    user_id: str,
    session_id: str,
) -> str:
    """Run the lakehouse analytics agent with Langfuse agent span."""
    global _session_service
    if _session_service is None:
        _session_service = InMemorySessionService()

    tools: list[Any] = []
    try:
        lakehouse_tools = McpToolset(
            connection_params=_resolve_lakehouse_connection_params(),
        )
        tools.append(lakehouse_tools)
    except Exception as exc:
        logger.warning(f"Lakehouse MCP toolset initialization skipped: {exc}")

    analyst_agent = Agent(
        name="sepa_lakehouse_analyst",
        model=get_gemini_model(),
        instruction=LAKEHOUSE_ANALYTICS_PROMPT,
        tools=tools,
    )
    analyst_session_id = f"{session_id}__analytics"
    await _ensure_session(user_id, analyst_session_id)
    analyst_runner = Runner(
        agent=analyst_agent,
        app_name=APP_NAME,
        session_service=_session_service,
    )

    user_msg = types.Content(role="user", parts=[types.Part(text=prompt)])
    analyst_text = ""
    async for event in analyst_runner.run_async(
        user_id=user_id,
        session_id=analyst_session_id,
        new_message=user_msg,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            analyst_text = event.content.parts[0].text or ""
    return analyst_text


@observe(name="sepa_shopping_assistant")
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
        final, or error)
    """
    _load_agent_env()
    with propagate_attributes(user_id=user_id, session_id=session_id):
        intent, conf = await classify_intent(prompt)

        if intent == "single_product_price":
            clean_query = extract_search_term(prompt)
            yield {
                "type": "progress",
                "content": f"Buscando precios para '{clean_query}' en SEPA...",
            }
            yield {
                "type": "tool_call",
                "content": "Calling tool: search_products_tool",
                "tool": "search_products_tool",
                "args": {"query": clean_query},
            }

            import asyncio
            from serving_mcp.server import search_products_tool

            try:
                results = await asyncio.to_thread(
                    search_products_tool,
                    search_query=clean_query,
                    limit=5,
                    use_jev=True,
                )
            except Exception as exc:
                yield {
                    "type": "error",
                    "content": f"Error consultando precios: {exc}",
                }
                return

            yield {
                "type": "tool_result",
                "content": f"Se encontraron {len(results)} productos en SEPA.",
                "tool": "search_products_tool",
            }

            yield {
                "type": "progress",
                "content": "Generando recomendación y análisis de precios...",
            }

            # Build research brief for Formatter Agent
            if results and not results[0].get("error"):
                top = results[0]
                desc = top.get("descripcion", clean_query)
                marca = top.get("marca") or ""
                precio = float(top.get("precio_lista", 0.0))
                sucursales = top.get("sucursales_count", 1)
                quotes = top.get("store_quotes", [])
                store_str = ", ".join(
                    [
                        f"{q.get('cadena_nombre')}: ${q.get('precio_lista')}"
                        for q in quotes[:4]
                    ]
                )
                research_brief = (
                    f"- Producto: {clean_query} -> Encontrado: {desc} ({marca}), "
                    f"Precio promedio: ${precio:.2f} (en {sucursales} sucursales). "
                    f"Cotizaciones por supermercado: {store_str or 'Promedio SEPA'}"
                )
            else:
                research_brief = f"- Producto: {clean_query} -> No se encontraron precios en SEPA (precio: $0.0)"

            final_text = ""
            try:
                final_text = await _run_formatter_agent(
                    prompt=prompt,
                    research_brief=research_brief,
                    user_id=user_id,
                    session_id=session_id,
                )
            except Exception as exc:
                logger.warning(
                    f"Formatter agent error on single product: {exc}, using deterministic fallback"
                )

            if final_text:
                final_event = _validated_final_event(final_text)
                if final_event.get("type") == "error":
                    payload = format_single_product_shopping_list(clean_query, results)
                    final_event = {
                        "type": "final",
                        "content": json.dumps(payload, ensure_ascii=False),
                        "data": payload,
                    }
            else:
                payload = format_single_product_shopping_list(clean_query, results)
                final_event = {
                    "type": "final",
                    "content": json.dumps(payload, ensure_ascii=False),
                    "data": payload,
                }

            yield {"type": "progress", "content": "Structuring response..."}
            yield final_event
            return

        if intent == "recipe_or_shopping_list":
            yield {
                "type": "progress",
                "content": "Planificando lista canónica de ingredientes y porciones...",
            }

            # 1. Planner Agent
            planner_text = await _run_planner_agent(
                prompt=prompt,
                user_id=user_id,
                session_id=session_id,
            )

            # 2. Programmatic Hybrid Search via Serving MCP (Postgres + TypeSafe Jev)
            yield {
                "type": "progress",
                "content": "Buscando y cotizando ingredientes en SEPA con TypeSafe Jev...",
            }
            items = parse_ingredient_lines(planner_text)
            if not items:
                items = [(prompt, "")]

            research_brief_lines: list[str] = []
            import asyncio
            from serving_mcp.server import search_products_tool

            for name, qty in items:
                yield {
                    "type": "tool_call",
                    "content": "Calling tool: search_products_tool",
                    "tool": "search_products_tool",
                    "args": {"query": name},
                }

                try:
                    results = await asyncio.to_thread(
                        search_products_tool,
                        search_query=name,
                        limit=3,
                        use_jev=True,
                    )
                except Exception:
                    results = []

                if results and not results[0].get("error"):
                    top = results[0]
                    desc = top.get("descripcion", name)
                    marca = top.get("marca") or ""
                    precio = float(top.get("precio_lista", 0.0))
                    sucursales = top.get("sucursales_count", 1)
                    quotes = top.get("store_quotes", [])
                    store_str = ", ".join(
                        [
                            f"{q.get('cadena_nombre')}: ${q.get('precio_lista')}"
                            for q in quotes[:3]
                        ]
                    )
                    line = (
                        f"- Item: {name} ({qty}) -> Producto encontrado: {desc} ({marca}), "
                        f"Precio: ${precio:.2f} (en {sucursales} sucursales). Cotizaciones: {store_str or 'Promedio SEPA'}"
                    )
                    yield {
                        "type": "tool_result",
                        "content": f"Encontrado: {desc} (${precio:.2f}) en {sucursales} sucursales",
                        "tool": "search_products_tool",
                    }
                else:
                    line = f"- Item: {name} ({qty}) -> No se encontraron precios en SEPA (precio: $0.0)"
                    yield {
                        "type": "tool_result",
                        "content": f"No se encontraron precios para '{name}'",
                        "tool": "search_products_tool",
                    }

                research_brief_lines.append(line)

            research_brief = "\n".join(research_brief_lines)

            # 3. Formatter Agent
            yield {
                "type": "progress",
                "content": "Generando lista de compras optimizada...",
            }
            final_text = await _run_formatter_agent(
                prompt=prompt,
                research_brief=research_brief,
                user_id=user_id,
                session_id=session_id,
            )

            yield {"type": "progress", "content": "Structuring response..."}
            final_event = _validated_final_event(final_text)
            yield final_event
            return

        if intent == "general_conversation":
            yield {
                "type": "progress",
                "content": "Pensando respuesta...",
            }
            conv_text = ""
            try:
                conv_text = await _run_conversational_agent(
                    prompt=prompt,
                    user_id=user_id,
                    session_id=session_id,
                )
            except Exception as exc:
                logger.warning(f"Conversational agent failed: {exc}")

            if not conv_text:
                conv_text = (
                    "¡Hola! Soy tu asistente de compras inteligentes de SEPA (Sistema Electrónico de Publicidad de Precios Argentinos).\n\n"
                    "Puedo ayudarte a:\n"
                    "- **Consultar y comparar precios** de productos específicos en supermercados (Coto, Carrefour, Dia, ChangoMás).\n"
                    "- **Planificar recetas** y armar una lista de compras con los mejores precios y cálculo de porciones.\n"
                    "- **Calcular ahorros** para saber en qué cadena te conviene comprar.\n\n"
                    "¿Qué producto o receta te gustaría consultar hoy?"
                )

            yield {
                "type": "final",
                "content": conv_text,
            }
            return

        if intent == "lakehouse_analytics":
            yield {
                "type": "progress",
                "content": "Consultando metadatos y analítica del Lakehouse...",
            }
            analytics_text = ""
            try:
                analytics_text = await _run_lakehouse_agent(
                    prompt=prompt,
                    user_id=user_id,
                    session_id=session_id,
                )
            except Exception as exc:
                logger.warning(f"Lakehouse analyst failed: {exc}")

            if not analytics_text:
                analytics_text = (
                    "El Lakehouse de SEPA procesa diariamente ~15M de precios usando una arquitectura Medallion:\n"
                    "- **Bronze**: Ingesta cruda en RustFS (S3) convertida a Parquet con compresión zstd.\n"
                    "- **Silver**: Tablas Apache Iceberg gestionadas por Apache Polaris REST Catalog (`sepa.precios`, `sepa.dim_*`, `sepa.audit_*`).\n"
                    "- **Gold**: Modelos analíticos dbt con DuckDB local y BigQuery.\n"
                    "- **Serving**: PostgreSQL 16 con índices GIN trigram para búsquedas en submilisegundos."
                )

            yield {
                "type": "final",
                "content": analytics_text,
            }
            return

        runner = build_runtime()
        await _ensure_session(user_id, session_id)

        user_msg = types.Content(role="user", parts=[types.Part(text=prompt)])

        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=user_msg,
        ):
            event_dict = _event_to_dict(event)
            if event_dict["type"] == "final":
                yield {"type": "progress", "content": "Structuring response..."}
            yield event_dict


if __name__ == "__main__":
    user_text = (
        " ".join(sys.argv[1:]) if len(sys.argv) > 1 else input("What do you need?: ")
    )
    print(run_prompt(user_text))
