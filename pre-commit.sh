#!/bin/bash
echo "Running ruff formatter..."
uv run ruff format src/ tests/
echo "Running ruff linter..."
uv run ruff check --fix src/ tests/
echo "Running mypy for type cheking..."
uv run mypy src/
echo "Running pytest for cooking pasta..."
uv run pytest
