import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from agent.tools.mcp.lakehouse_mcp.server import mcp
from agent.tools.mcp.lakehouse_mcp.clients.duckdb import init_duckdb, close_duckdb

logger = logging.getLogger(__name__)

# Get the streamable HTTP app first so the session manager is initialized internally
streamable_app = mcp.streamable_http_app()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing DuckDB and registering views...")
    init_duckdb()
    
    # FastAPI does not automatically run lifespans of mounted sub-applications.
    # We must explicitly run the FastMCP session manager's lifespan here.
    async with mcp.session_manager.run():
        yield
        
    logger.info("Closing DuckDB connection...")
    close_duckdb()

app = FastAPI(lifespan=lifespan)

# Mount the FastMCP Streamable HTTP app
# FastMCP sets up the /mcp route by default for bidirectional Streamable HTTP
app.mount("/", streamable_app)
