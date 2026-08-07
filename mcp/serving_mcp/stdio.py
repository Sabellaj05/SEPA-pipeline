import asyncio
import logging

from serving_mcp.server import mcp
from serving_mcp.clients.serving import close_serving_connection, get_serving_connection

# Suppress logging to stderr/stdout so it doesn't interfere with stdio transport
logging.basicConfig(level=logging.ERROR)


async def main() -> None:
    get_serving_connection()
    try:
        await mcp.run_stdio_async()
    finally:
        close_serving_connection()


if __name__ == "__main__":
    asyncio.run(main())
