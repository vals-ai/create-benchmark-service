FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends curl git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for faster package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies first so this layer caches unless pyproject changes;
# --no-install-project skips the service itself, which needs no version yet.
COPY pyproject.toml README.md ./
RUN uv sync --no-install-project

# Version is injected at build (CI passes the git tag); hatch-vcs reads
# SETUPTOOLS_SCM_PRETEND_VERSION rather than .git, so the image needs no .git.
ARG SERVICE_VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SERVICE_VERSION}

COPY . .
RUN uv sync

# Expose port
EXPOSE 8001

# Run the application. The --ws-ping-* values are the server half of the keepalive
# contract; BenchmarkServiceClient derives its own from them (_WS_PING_* in client.py).
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--ws-ping-interval", "30", "--ws-ping-timeout", "10"]
