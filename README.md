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
│   ├── auth.py                # Auth, Descope tenant resolution, allowlist loading
│   ├── base.py                # BenchmarkService base class
│   ├── client.py              # HTTP/WebSocket client
│   ├── schemas.py             # Pydantic models
│   └── utils.py               # Utilities
├── templates/                 # Templates for generated projects
│   ├── pyproject.toml
│   └── README.md.jinja
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
| `retrieve_task(task_id, skip_validation, dataset)` | Return task metadata: docker image, problem path, resources, etc. |
| `setup_task(task_id, sandbox, dataset)` | Async generator — set up the task in a Daytona sandbox, yielding `StreamChunk`s |
| `evaluate_response(request, dataset)` | Evaluate a text response directly (no sandbox needed) |
| `evaluate_instance(task_id, sandbox, dataset)` | Async generator — run evaluation in a Daytona sandbox, yielding `StreamChunk`s |
| `calculate_final_score(evaluation_results, dataset)` | Aggregate per-task results into a final `FinalScoreResult` |

**Built-in methods:**

- `get_dataset(dataset)` — return the task dictionary for a given dataset name (defaults to `"default"`)
- `filter_tasks(task_filter, dataset)` — return task IDs matching a list or Python slice notation (e.g. `"0:10:2"`)
- `validate_task_ids(task_ids, dataset)` — raise `ValueError` if any ID is not in the dataset
- `check_auth(headers)` — legacy boolean auth hook. Override for custom auth that does not need tenant or dataset awareness.
- `resolve_tenant(headers)` — validate request authorization and return a tenant ID, `"_legacy"` for legacy auth, or `None` to reject.
- `check_dataset_access(tenant, dataset)` — return whether a resolved tenant may access a dataset.

### FastAPI application factory (`app.py`)

`BenchmarkServiceApp(service_cls)` wraps your `BenchmarkService` subclass in a fully configured FastAPI app. Pass your subclass and run the result with any ASGI server.

**HTTP endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Returns `{"status": "ok"}` |
| `GET` | `/verify-task-ids` | Return task IDs filtered by `?task_ids=…` or `?slice=start:stop:step` (optional `?dataset=…`) |
| `GET` | `/retrieve-task/?task_id=…` | Return task metadata for a given task ID (optional `?dataset=…`) |
| `POST` | `/evaluate-response/` | Evaluate without a sandbox and return one final response |
| `POST` | `/final-score/` | Aggregate results: `{"evaluation_results": {task_id: result, …}, "dataset": "…"}` |
| `POST` | `/v1/evaluate` | Lab-facing per-task evaluation (see [v1 Eval API](#v1-eval-api-lab-facing) below) |
| `POST` | `/v1/score` | Lab-facing run aggregation (see [v1 Eval API](#v1-eval-api-lab-facing) below) |

**WebSocket endpoints** (stream `StreamChunk` JSON objects):

| Path | Description |
|------|-------------|
| `/ws/setup-task` | Set up a task in a sandbox; streams progress, errors, and a final result |
| `/ws/evaluate-response` | Evaluate without a sandbox; streams progress, checkpoint state, errors, and a final result |
| `/ws/evaluate-instance` | Evaluate a live sandbox solution; streams progress, errors, and a final result |

Sandbox setup and live sandbox evaluation require three headers — `x-api-key`, `x-api-url`, `x-target` — used to connect to Daytona. Live evaluation accepts `{"task_id": "…", "instance_id": "…", "dataset": "…"}`. Eval-only retry uses `/ws/evaluate-response` with `{"task_id": "…", "eval_resume_state": {...}, "dataset": "…"}` and does not require Daytona headers.

Benchmark services can send `eval_resume_state` updates to the tracker while evaluation is running. The tracker stores the latest value and sends it back on eval-only retry, so the benchmark service can continue evaluation without recreating the original agent sandbox.

Eval-only retry flow:

1. A benchmark service yields `StreamEvalResumeStateChunk` before starting failure-prone evaluation work.
2. The tracker stores the latest `eval_resume_state` on the task row.
3. If evaluation fails after that point, retry calls `/ws/evaluate-response` with the saved state.
4. The benchmark service decides what the state means and streams a new result, plus any newer checkpoint state.

### Streaming protocol

The WebSocket endpoints and generators communicate via four chunk types:

```python
StreamMessageChunk(type="message", data="log line")              # progress / log output
StreamErrorChunk(type="error", data="error text")                # non-fatal errors
StreamEvalResumeStateChunk(type="eval_resume_state", data={...}) # evaluation progress state
StreamResultChunk(type="result", data=<any>)                     # final result payload
```

Yield these from your generator methods; the framework serialises and forwards them to the WebSocket client.

### v1 Eval API (lab-facing)

`/v1/evaluate` and `/v1/score` are the lab-facing surface. They share scoring handlers with the internal `/evaluate-response/` and `/final-score/` endpoints — a benchmark implements `evaluate_response` and `calculate_final_score` once and inherits both surfaces.

**`POST /v1/evaluate`** — grade one task. Request:

```json
{
  "run_id": "external-run-123",
  "task_id": "01-011",
  "dataset": "validation",
  "payload": {"type": "text", "schema": "fabv2.text.v1", "data": "..."},
  "versions": {"runner": "0.0.10-a1b2c3d"}
}
```

Response:

```json
{
  "run_id": "external-run-123",
  "task_id": "01-011",
  "status": "evaluated",
  "evaluator_version": "0.0.2",
  "result": {"pass_percentage": 0.83, "...": "..."},
  "errors": []
}
```

`status` is one of `evaluated`, `did_not_complete`, `generation_error`, `error`. `result` is the benchmark-specific JSON-compatible value your `evaluate_response` returns, passed through. Only `payload.type == "text"` is implemented today; artifact rehydration is a follow-on.

**`POST /v1/score`** — aggregate across a run. Request `{run_id, dataset, evaluation_results: {task_id: {"status": "evaluated", "result": {...}} | {"status": "did_not_complete"} | null}}`. Before calling `calculate_final_score`, the framework unwraps evaluated task envelopes to their `result` value and treats non-evaluated or null tasks as `null`. Response `{run_id, tasks_evaluated, final_score, metadata}`.

**Auth.** `/v1/*` is Descope-only. Callers using the legacy `BENCHMARK_API_KEY` bearer (i.e. those that resolve to the `_legacy` tenant sentinel) get 403. Migrate the deploy to Descope (`AUTH_REQUIRED=true` + `DESCOPE_PROJECT_ID` + allowlist) before opening `/v1/` to external traffic.

**Deferred to follow-on plans.** `GET /v1/schema`, `GET /v1/tasks/{task_id}`, `/ws/v1/evaluate` (streamed judges), artifact payloads, async/`poll_url` response shape, idempotency on `(run_id, task_id)`.

### Schemas (`schemas.py`)

Pydantic models used across requests and responses:

- **`RetrieveTaskResponse`** — `docker_image`, `problem_path`, `cwd`, `agent_timeout`, `Resources`
- **`Resources`** — `vcpu`, `memory` (GB), `disk` (GB)
- **`EvaluateResponseRequest`** — `task_id`, `response` or `eval_resume_state`, `dataset`
- **`FinalScoreResult`** / **`FinalScoreResponse`** — `score` (float), `metadata`, `tasks_evaluated`
- **`TaskFilter`** — `task_ids` list or `slice_str`; `parse_slice()` converts `"start:stop:step"` to a Python `slice`

### Utilities (`utils.py`)

**`stream_command(sandbox, command, cwd, ignore_error=False)`**

Runs a shell command inside a Daytona sandbox and yields stdout/stderr lines in real time. Creates a unique session per invocation, streams output via an async queue, checks the exit code, and cleans up the session on completion. Use it inside `setup_task` and `evaluate_instance` to run commands and forward their output as `StreamMessageChunk`s.

### Authentication

The framework authenticates every HTTP request except `/health`, and every WebSocket route before Daytona headers are used.

For hosted Valkyrie benchmark services, set `AUTH_REQUIRED=true`, `DESCOPE_PROJECT_ID`, and a tenant + dataset allowlist. Requests must include a valid Descope access key in `X-Descope-Api-Key`. The key must be scoped to exactly one Descope tenant, and that tenant must appear in the service allowlist.

The allowlist is loaded in this order:

1. `DESCOPE_TENANT_ALLOWLIST_JSON` — JSON payload injected by production deployment.
2. `DESCOPE_ALLOWLIST_PATH` — path to a local YAML file, useful for development.
3. Empty config — allowed at startup, but Descope-authenticated requests fail closed when `AUTH_REQUIRED=true`.

Example allowlist:

```yaml
tenants:
  acme-corp:
    datasets:
      - validation
  vals-internal:
    datasets:
      - default
      - validation
      - test
```

Malformed configured allowlists raise at app startup when `AUTH_REQUIRED=true`. Unknown tenants receive `401 Unauthorized`. Known tenants requesting a dataset outside their allowlist receive `403 Dataset not allowed`; WebSocket routes close with code `1008`. The tenant ID `"_legacy"` is reserved for compatibility mode and is rejected as a Descope tenant.

For local development or legacy custom services, leave `AUTH_REQUIRED` unset or `false`. In that mode, `BENCHMARK_API_KEY` preserves the previous static-key behavior by requiring `Authorization: Bearer <key>`. If `BENCHMARK_API_KEY` is not set, requests are allowed. Legacy auth uses the `"_legacy"` sentinel and bypasses dataset-level allowlist enforcement because no tenant identity is available.

Override `check_auth()` in your `BenchmarkService` subclass to keep using custom boolean authentication:

```python
from benchmark_service import BenchmarkService

class MyBenchmarkService(BenchmarkService):
    async def check_auth(self, headers: dict[str, str]) -> bool:
        return headers.get("authorization") == "my-secret-credential"

    # ... other abstract methods
```

For tenant-aware custom authentication, override `resolve_tenant()` directly and return a tenant ID. Override `check_dataset_access()` if your service needs dataset rules that differ from the configured allowlist:

```python
from benchmark_service import BenchmarkService

class MyBenchmarkService(BenchmarkService):
    async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
        token = headers.get("authorization")
        if token == "Bearer internal-token":
            return "internal"
        return None

    async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
        return tenant == "internal" and (dataset or "default") in {"default", "validation"}

    # ... other abstract methods
```

Header names are lowercase per HTTP convention. Requests that fail auth receive a `401 Unauthorized` response automatically.

Valkyrie users normally configure their Descope credential once via the CLI. Legacy/custom service credentials can still be configured separately:

```bash
valkyrie config auth set <benchmark-name> <credential>
```

The credential is stored under `benchmark_auth` and sent as the `Authorization` header on every request. Users can also pass arbitrary headers at runtime with `-H`:

```bash
valkyrie run start --benchmark my-benchmark --agent my-agent -H X-Custom value
```

### Reverse Tunnel setup

You may want to test the benchmark service using valkyrie instead of hosting it locally. We offer a simple way to do this through ngrok (although you can use any reverse tunnel tool)

Setup

1. [Signup / login to ngrok](https://dashboard.ngrok.com/login)
2. [Follow the setup and installation steps](https://dashboard.ngrok.com/get-started/setup/macos)
3. Start the project using either `make dev` or `make docker-build && make docker-run`
4. Run ngrok on the matching port that is exposed: `ngrok http 8001` (forwards the traffic from the tunnel to the FastAPI server running on your machine)
   - Copy the forwarding address on the left. Example: `https://hemagglutinative-vonnie-fungic.ngrok-free.dev`
5. Using Valkyrie, run `valkyrie config service add <benchmark-name> <forwarding-address>`

If the forwarding address changes you will need to run step 5 again.
