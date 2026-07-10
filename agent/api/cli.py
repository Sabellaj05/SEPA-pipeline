"""CLI entry point for ``sepa-api`` (see pyproject.toml [project.scripts]).

Usage::

    sepa-api          # starts on 0.0.0.0:8000
    sepa-api --port 9000
"""

import os


def main() -> None:
    """Launch the Agent API via uvicorn."""
    import uvicorn

    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run(
        "agent.api.app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
