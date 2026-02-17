# Create Benchmark Service

CLI tool to scaffold benchmark services.

> **TODO**: When this repository becomes public, update `templates/pyproject.toml.jinja` to use HTTPS instead of SSH:
> ```toml
> "create-benchmark-service @ git+https://github.com/vals-ai/create-benchmark-service.git@main"
> ```

## Installation

```bash
pip install git+ssh://git@github.com/vals-ai/create-benchmark-service.git@main
```

## Usage

```bash
create-benchmark-service <benchmark-name>
```

Creates a new service in `./<benchmark-name>-service/` in your current directory.

## What Gets Generated

```
<benchmark-name>-service/
├── main.py                    # Service implementation
├── src/
│   └── {benchmark_package}/   # Benchmark-specific utilities
├── tests/                     # Tests
├── .github/workflows/         # CI/CD (test, lint, typecheck)
├── pyproject.toml             # Dependencies
├── Dockerfile                 # Container image
├── Makefile                   # Commands
├── README.md                  # Documentation
├── .gitignore
├── .dockerignore
└── .python-version
```

## Repository Structure

```
.
├── cli/                       # CLI tool
│   ├── cli.py                 # Entry point
│   └── generator.py           # Project generator
├── src/benchmark_service/     # Framework code
│   ├── __init__.py
│   ├── app.py                 # FastAPI application
│   ├── base.py                # BenchmarkService base class
│   ├── schemas.py             # Pydantic models
│   └── utils.py               # Utilities
├── templates/                 # Templates for generated projects
│   ├── pyproject.toml
│   └── README.md
├── main.py                    # Example implementation
├── pyproject.toml             # CLI + framework config
└── README.md                  # This file
```
