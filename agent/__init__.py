"""SEPA Operations Agent package.

Importing this package is side-effect-free.  The agent runtime (MCP transport,
model, session service) is initialized lazily the first time ``root_agent`` is
accessed or ``run_prompt()`` is called.

Typical usage
-------------
Programmatic::

    from agent.sepa_agent import run_prompt
    answer = run_prompt("Show me last week's silver audit stats.")

ADK runner (``adk run agent``)::

    # ADK accesses agent.root_agent, which triggers build_runtime() here.
    # No I/O occurs on bare import.

"""
from __future__ import annotations

from typing import Any


def __getattr__(name: str) -> Any:
    """Lazily expose ``root_agent`` so that importing this package has no I/O.

    Python only calls module ``__getattr__`` when ``name`` is *not* already in
    the module's ``__dict__``.  Tests that import ``agent.sepa_agent`` directly
    never trigger this function.  ``adk run agent`` accesses ``agent.root_agent``
    at runtime and gets the correctly-initialized Agent instance.
    """
    if name == "root_agent":
        from agent import sepa_agent

        sepa_agent.build_runtime()
        return sepa_agent.root_agent
    raise AttributeError(f"module 'agent' has no attribute {name!r}")
