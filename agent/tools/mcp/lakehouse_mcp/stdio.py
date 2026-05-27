import asyncio
import logging

from agent.tools.mcp.lakehouse_mcp.server import mcp
from agent.tools.mcp.lakehouse_mcp.clients.duckdb import init_duckdb, close_duckdb

# Suppress logging to stderr/stdout so it doesn't interfere with stdio transport
logging.basicConfig(level=logging.ERROR)

async def main():
    init_duckdb()
    try:
        await mcp.run_stdio_async()
    finally:
        close_duckdb()

if __name__ == "__main__":
    asyncio.run(main())
