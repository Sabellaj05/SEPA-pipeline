#!/bin/bash
echo "Running black"
uv run black src/ tests/          # Format code
echo "Running isort"
uv run isort src/ tests/          # Sort imports
echo "Running flake8"
uv run flake8 src/ tests/         # Check linting
echo "Running mypy"
uv run mypy src/                  # Type check
echo "Running pytest"
uv run pytest                     # Run tests
