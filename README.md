# Create Benchmark Service

CLI tool to scaffold benchmark services.

## Installation

```bash
uv tool install git+ssh://git@github.com/vals-ai/create-benchmark-service.git@main
```

## Usage

```bash
create-benchmark-service <benchmark-name>
```

Creates a new service in `./<benchmark-name>-benchmark-service/` in your current directory.

## What Gets Generated

```
<benchmark-name>-benchmark-service/
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

## Framework: `src/benchmark_service`

The `benchmark_service` package is the core framework that generated services build on. It provides the FastAPI application, abstract base class, data models, and sandbox utilities — so you only need to implement benchmark-specific logic.

### `BenchmarkService` base class (`base.py`)

Subclass `BenchmarkService` and implement its abstract methods. On instantiation, the `create()` factory method calls `load_datasets()` and stores the result as `self.datasets`.

**Abstract methods to implement:**

| Method | Description |
|--------|-------------|
| `load_datasets()` | Load all tasks from your source; return `dict[dataset_name, dict[task_id, task_object]]` |
| `retrieve_task(task_id, skip_validation, dataset)` | Return task metadata: docker image, problem statement, resources, etc. |
| `setup_task(task_id, sandbox, dataset)` | Async generator — set up the task in a Daytona sandbox, yielding `StreamChunk`s |
| `evaluate_response(request, dataset)` | Evaluate a text response directly (no sandbox needed) |
| `evaluate_instance(task_id, sandbox, dataset)` | Async generator — run evaluation in a Daytona sandbox, yielding `StreamChunk`s |
| `calculate_final_score(evaluation_results, dataset)` | Aggregate per-task results into a final `FinalScoreResult` |

**Built-in methods:**

- `get_dataset(dataset)` — return the task dictionary for a given dataset name (defaults to `"default"`)
- `filter_tasks(task_filter, dataset)` — return task IDs matching a list or Python slice notation (e.g. `"0:10:2"`)
- `validate_task_ids(task_ids, dataset)` — raise `ValueError` if any ID is not in the dataset

### FastAPI application factory (`app.py`)

`BenchmarkServiceApp(service_cls)` wraps your `BenchmarkService` subclass in a fully configured FastAPI app. Pass your subclass and run the result with any ASGI server.

**HTTP endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Returns `{"status": "ok"}` |
| `GET` | `/verify-task-ids` | Return task IDs filtered by `?task_ids=…` or `?slice=start:stop:step` (optional `?dataset=…`) |
| `GET` | `/retrieve-task/?task_id=…` | Return task metadata for a given task ID (optional `?dataset=…`) |
| `POST` | `/evaluate-response/` | Evaluate a text response: `{"task_id": "…", "response": "…", "dataset": "…"}` |
| `POST` | `/final-score/` | Aggregate results: `{"evaluation_results": {task_id: result, …}, "dataset": "…"}` |

**WebSocket endpoints** (stream `StreamChunk` JSON objects):

| Path | Description |
|------|-------------|
| `/ws/setup-task` | Set up a task in a sandbox; streams progress, errors, and a final result |
| `/ws/evaluate-instance` | Evaluate a solution in a sandbox; streams progress, errors, and a final result |

Both WebSocket endpoints require three headers — `x-api-key`, `x-api-url`, `x-target` — used to connect to the Daytona sandbox, and accept a JSON body of `{"task_id": "…", "instance_id": "…", "dataset": "…"}` (dataset is optional, defaults to `"default"`).

### Streaming protocol

The WebSocket endpoints and the `setup_task` / `evaluate_instance` generators communicate via three chunk types:

```python
StreamMessageChunk(type="message", data="log line")     # progress / log output
StreamErrorChunk(type="error",   data="error text")     # non-fatal errors
StreamResultChunk(type="result", data=<any>)            # final result payload
```

Yield these from your generator methods; the framework serialises and forwards them to the WebSocket client.

### Schemas (`schemas.py`)

Pydantic models used across requests and responses:

- **`RetrieveTaskResponse`** — `docker_image`, `problem_statement`, `request_setup`, `cwd`, `Resources`
- **`Resources`** — `vcpu`, `memory` (GB), `disk` (GB)
- **`EvaluateResponseRequest`** — `task_id`, `response`, `dataset`
- **`FinalScoreResult`** / **`FinalScoreResponse`** — `score` (float), `metadata`, `tasks_evaluated`
- **`TaskFilter`** — `task_ids` list or `slice_str`; `parse_slice()` converts `"start:stop:step"` to a Python `slice`

### Utilities (`utils.py`)

**`stream_command(sandbox, command, cwd, ignore_error=False)`**

Runs a shell command inside a Daytona sandbox and yields stdout/stderr lines in real time. Creates a unique session per invocation, streams output via an async queue, checks the exit code, and cleans up the session on completion. Use it inside `setup_task` and `evaluate_instance` to run commands and forward their output as `StreamMessageChunk`s.

### Reverse Tunnel setup

You may want to test the benchmark service using the hosted agentic harness service instead of hosting it locally. We offer a simple way to do this through ngrok (although you can use any reverse tunnel tool)

Setup

1. [Signup / login to ngrok](https://dashboard.ngrok.com/login)
2. [Follow the setup and installation steps](https://dashboard.ngrok.com/get-started/setup/macos)
3. Start the project using either `make dev` or `make docker-build && make docker-run`
4. Run ngrok on the matching port that is exposed: `ngrok http 8001` (forwards the traffic from the tunnel to the FastAPI server running on your machine)
   - Copy the forwarding address on the left. Example: `https://hemagglutinative-vonnie-fungic.ngrok-free.dev`
5. Using the agentic harness CLI, run `harness config service add <benchmark-name> <forwarding-address>`

If the forwarding address changes you will need to run step 5 again.
