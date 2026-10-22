.PHONY: install test lint typecheck build
install:
	uv sync --all-extras
test:
	uv run --all-extras pytest
lint:
	uv run ruff check .
	uv run ruff format --check .
typecheck:
	uv run --all-extras basedpyright
build:
	uv build
