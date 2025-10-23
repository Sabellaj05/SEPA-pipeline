#!/bin/bash
echo "Running black formatter..."
uv run black src/ tests/
echo "Running isort to sort imports..."
uv run isort src/ tests/
echo "Running flake8 for linting..."
uv run flake8 src/ tests/
echo "Running mypy for type cheking..."
uv run mypy src/
echo "Running pytest for cooking pasta..."
uv run pytest
