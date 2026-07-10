"""Pydantic request/response models for the Agent API."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """Incoming chat message from the frontend."""

    prompt: str = Field(..., min_length=1, max_length=4000)
    user_id: str = Field(default="web-user")
    session_id: str | None = Field(
        default=None,
        description="If None, auto-generates a UUID on the server side.",
    )


class ChatResponse(BaseModel):
    """Non-streaming response returned by POST /api/agent/chat."""

    response: str
    session_id: str
    user_id: str


class HealthResponse(BaseModel):
    """Response for GET /health."""

    status: str
    uptime_seconds: float
    agent_name: str
    model: str


class SSEEvent(BaseModel):
    """Single event emitted over the SSE stream.

    Types:
        progress   – agent is working / intermediate status
        tool_call  – agent invoked an MCP tool
        tool_result – tool returned a result
        final      – agent's final answer
        error      – something went wrong
    """

    type: Literal["progress", "tool_call", "tool_result", "final", "error"]
    content: str = ""
    tool: str | None = None
    args: dict[str, Any] | None = None
    data: dict[str, Any] | None = None


# Smart List feature
class Item(BaseModel):
    name: str = Field(..., min_length=1)
    price: float = Field(..., ge=0)
    description: str = Field(default="")
    quantity: int = Field(..., ge=1)


class Stores(BaseModel):
    name: str = Field(..., min_length=1)
    items: list[Item] = Field(..., min_length=1)


class ShoppingList(BaseModel):
    project_name: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1, max_length=4096)
    total_estimate: float = Field(..., ge=0)
    savings: float = Field(default=0, ge=0)
    stores: list[Stores] = Field(..., min_length=1)
