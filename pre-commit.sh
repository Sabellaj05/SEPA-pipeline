#!/bin/bash
set -euo pipefail

if command -v just >/dev/null 2>&1; then
    exec just pre-commit "$@"
fi

echo "Running ruff formatter..."
uv run ruff format src/ tests/ agent/ mcp/
echo "Running ruff linter..."
uv run ruff check --fix src/ tests/ agent/ mcp/
echo "Running mypy for type checking..."
uv run mypy src/
echo "Running pytest..."
uv run pytest -v "$@"

