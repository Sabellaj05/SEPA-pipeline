import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from serving_mcp.server import mcp
from serving_mcp.clients.serving import close_serving_connection, get_serving_connection

logger = logging.getLogger(__name__)

# The MCP SDK's FastMCP implementation exposes /mcp inside this Starlette app.
# Mounting it at "/" keeps the public MCP endpoint at /mcp.
streamable_app = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Warming up serving DuckDB connection...")
    try:
        get_serving_connection()
    except Exception as exc:
        logger.warning("Serving DB not available at startup: %s", exc)

    # FastAPI does not run mounted sub-application lifespans automatically.
    # Run the MCP ASGI lifespan explicitly so its session manager starts.
    try:
        async with streamable_app.router.lifespan_context(streamable_app):
            yield
    finally:
        logger.info("Closing serving DuckDB connection...")
        close_serving_connection()


app = FastAPI(lifespan=lifespan)

app.mount("/", streamable_app)
