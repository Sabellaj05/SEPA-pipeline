"""Agent API routes — streaming SSE, synchronous chat, and session management."""

import json
import logging
import uuid
from collections.abc import AsyncGenerator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from agent.api.schemas import ChatRequest, ChatResponse, HealthResponse

router = APIRouter()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Basic health-check. Returns agent status and uptime."""
    from agent.api.app import get_uptime
    from agent.sepa_agent import LOCAL_ENABLED, root_agent

    model_name = "local" if LOCAL_ENABLED else "gemini-2.5-flash"
    agent_name = root_agent.name if root_agent else "not initialized"

    return HealthResponse(
        status="ok" if root_agent else "initializing",
        uptime_seconds=round(get_uptime(), 2),
        agent_name=agent_name,
        model=model_name,
    )


# ---------------------------------------------------------------------------
# SSE streaming
# ---------------------------------------------------------------------------


@router.post("/api/agent/stream")
async def stream_agent_response(req: ChatRequest) -> StreamingResponse:
    """Stream agent events as Server-Sent Events (SSE).

    Each event is a JSON object on a ``data:`` line.  The stream ends with
    ``data: [DONE]``.

    Event types: progress, tool_call, tool_result, final, error.
    """
    from agent.sepa_agent import arun_prompt_stream

    session_id = req.session_id or str(uuid.uuid4())

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            async for event_dict in arun_prompt_stream(
                req.prompt,
                user_id=req.user_id,
                session_id=session_id,
            ):
                # Include session_id so the frontend can track it.
                event_dict["session_id"] = session_id
                payload = json.dumps(event_dict, ensure_ascii=False)
                yield f"data: {payload}\n\n"
        except Exception as exc:
            logger.exception("Error during agent streaming")
            error_payload = json.dumps(
                {
                    "type": "error",
                    "content": str(exc),
                    "session_id": session_id,
                }
            )
            yield f"data: {error_payload}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Non-streaming chat (convenience / testing)
# ---------------------------------------------------------------------------


@router.post("/api/agent/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    """Non-streaming chat endpoint. Returns the agent's final answer."""
    from agent.sepa_agent import arun_prompt_stream

    session_id = req.session_id or str(uuid.uuid4())
    final_text = ""

    try:
        async for event_dict in arun_prompt_stream(
            req.prompt,
            user_id=req.user_id,
            session_id=session_id,
        ):
            if event_dict.get("type") == "final":
                final_text = event_dict.get("content", "")
    except Exception as exc:
        logger.exception("Error during agent chat")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ChatResponse(
        response=final_text,
        session_id=session_id,
        user_id=req.user_id,
    )


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


@router.get("/api/agent/sessions/{user_id}")
async def list_sessions(user_id: str) -> dict:
    """List known session IDs for a user.

    Note: InMemorySessionService does not expose a ``list_sessions`` method,
    so this is a best-effort stub that returns the sessions the service
    knows about.  A DB-backed service will provide full support.
    """
    from agent.sepa_agent import APP_NAME, _session_service

    if _session_service is None:
        raise HTTPException(status_code=503, detail="Session service not ready")

    # InMemorySessionService stores sessions in a nested dict:
    #   _sessions[app_name][user_id] -> {session_id: Session}
    sessions_map = getattr(_session_service, "_sessions", {})
    user_sessions = sessions_map.get(APP_NAME, {}).get(user_id, {})
    session_ids = list(user_sessions.keys())

    return {"user_id": user_id, "sessions": session_ids}


@router.delete("/api/agent/sessions/{user_id}/{session_id}")
async def delete_session(user_id: str, session_id: str) -> dict:
    """Delete / reset a specific session.

    InMemorySessionService does not have a delete method, so we remove
    the session dict entry directly.
    """
    from agent.sepa_agent import APP_NAME, _session_service

    if _session_service is None:
        raise HTTPException(status_code=503, detail="Session service not ready")

    sessions_map = getattr(_session_service, "_sessions", {})
    user_sessions = sessions_map.get(APP_NAME, {}).get(user_id, {})

    if session_id not in user_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    del user_sessions[session_id]
    return {"deleted": True, "session_id": session_id}
