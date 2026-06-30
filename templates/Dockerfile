FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends curl git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for faster package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy the full source, including .git — hatch-vcs reads it at build to resolve
# the service version. Copy before `uv sync` or the version is the 0.0.0 fallback.
COPY . .

# Install dependencies and the service
RUN uv sync

# Expose port
EXPOSE 8001

# Run the application
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--ws-ping-interval", "30", "--ws-ping-timeout", "10"]
