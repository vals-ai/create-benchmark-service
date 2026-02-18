FROM python:3.12-slim

WORKDIR /app

# Install uv for faster package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies
RUN uv sync

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Run the application
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--ws-ping-interval", "30", "--ws-ping-timeout", "10"]
