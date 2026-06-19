import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from lakehouse_mcp.server import mcp
from lakehouse_mcp.clients.duckdb import init_duckdb, close_duckdb

logger = logging.getLogger(__name__)

# The MCP SDK's FastMCP implementation exposes /mcp inside this Starlette app.
# Mounting it at "/" keeps the public MCP endpoint at /mcp.
streamable_app = mcp.streamable_http_app()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing DuckDB and registering views...")
    init_duckdb()

    # FastAPI does not run mounted sub-application lifespans automatically.
    # Run the MCP ASGI lifespan explicitly so its session manager starts.
    try:
        async with streamable_app.router.lifespan_context(streamable_app):
            yield
    finally:
        logger.info("Closing DuckDB connection...")
        close_duckdb()

app = FastAPI(lifespan=lifespan)

app.mount("/", streamable_app)
