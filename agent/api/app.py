"""FastAPI application for the SEPA Agent API.

Start the server::

    uv run uvicorn agent.api.app:app --host 0.0.0.0 --port 8000 --reload
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from agent.api.routes import router

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

_start_time: float = 0.0


def get_uptime() -> float:
    """Seconds since the server started."""
    return time.monotonic() - _start_time


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize the ADK runtime on startup; clean up on shutdown."""
    global _start_time
    _start_time = time.monotonic()

    # Load agent env before build_runtime so env vars are available.
    from agent.sepa_agent import _load_agent_env, build_runtime

    _load_agent_env()
    load_dotenv()  # also load root .env for MINIO_* etc.

    logger.info("Initializing ADK agent runtime...")
    build_runtime()
    logger.info("Agent runtime ready.")

    yield

    logger.info("Shutting down Agent API.")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SEPA Agent API",
    description="HTTP API for the SEPA Operations Agent (Google ADK).",
    version="0.1.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

_default_origins = [
    "*",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:4321",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
]

_cors_origins_raw = os.getenv("API_CORS_ORIGINS", "")
cors_origins: list[str] = (
    [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]
    if _cors_origins_raw
    else _default_origins
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[] if "*" in cors_origins else cors_origins,
    allow_origin_regex=r".*" if "*" in cors_origins else None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

app.include_router(router)
