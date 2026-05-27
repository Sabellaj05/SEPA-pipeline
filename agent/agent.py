"""
SEPA Operations Agent — Built with Google ADK
Orchestrates the MCP server to provide operational insights and data querying.
"""

import os
import socket
import sys

from google.adk import Agent
from google.adk.tools import McpToolset
from google.adk.tools.mcp_tool import (
    StdioConnectionParams,
    StreamableHTTPConnectionParams,
)

from agent.prompt import SYSTEM_PROMPT

# Configure paths
current_dir = os.path.dirname(os.path.abspath(__file__))
server_path = os.path.join(current_dir, "tools", "mcp", "lakehouse_mcp", "stdio.py")

# system prompt
SYSTEM_INSTRUCTIONS = SYSTEM_PROMPT


# Helper to check if the SSE server is already listening
def is_port_open(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            return s.connect_ex((host, port)) == 0
    except Exception:
        return False


# Toggle transport dynamically based on port availability
http_port = 19121
if is_port_open("127.0.0.1", http_port):
    print(f"Connecting to running Streamable HTTP MCP server on port {http_port}...")
    connection_params = StreamableHTTPConnectionParams(
        url=f"http://127.0.0.1:{http_port}/mcp"
    )
else:
    print("HTTP server offline. Spawning local MCP server via Stdio...")
    from mcp import StdioServerParameters

    connection_params = StdioConnectionParams(
        server_params=StdioServerParameters(
            command="uv",
            args=["run", "python", "-m", "agent.tools.mcp.lakehouse_mcp.stdio"],
        )
    )

audit_tools = McpToolset(connection_params=connection_params)

# Main Agent definition
root_agent = Agent(
    name="sepa_ops_assistant",
    model="gemini-2.5-flash",
    instruction=SYSTEM_INSTRUCTIONS,
    tools=[audit_tools],
)

if __name__ == "__main__":
    # This allows running the agent directly for testing
    # Note: Requires GOOGLE_API_KEY or Application Default Credentials
    if len(sys.argv) > 1:
        # Simple CLI interaction if prompt provided
        response = root_agent.run(input=sys.argv[1])
        print(response.text)
    else:
        print(
            "SEPA Ops Assistant initialized. Use 'adk run agent/agent.py' or provide a prompt."
        )
