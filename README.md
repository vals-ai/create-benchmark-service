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
| `retrieve_task(task_id, skip_validation, dataset)` | Return task metadata: sandbox source, problem path, resources, etc. |
| `setup_task(task_id, sandbox, dataset)` | Async generator — set up the task in a sandbox, yielding `StreamChunk`s |
| `evaluate_response(request, dataset)` | Evaluate a text response directly (no sandbox needed) |
| `evaluate_instance(task_id, sandbox, dataset)` | Async generator — run evaluation in a sandbox, yielding `StreamChunk`s |
| `calculate_final_score(evaluation_results, dataset)` | Aggregate per-task results into a final `FinalScoreResult` |

**Built-in methods:**

- `get_dataset(dataset)` — return the task dictionary for a given dataset name (defaults to `"default"`)
- `filter_tasks(task_filter, dataset)` — return task IDs matching a list or Python slice notation (e.g. `"0:10:2"`)
- `validate_task_ids(task_ids, dataset)` — raise `ValueError` if any ID is not in the dataset
- `list_tasks(dataset)` — return `list[V1Task]` (id, question, timeout) for the lab-facing `GET /v1/datasets/{dataset}/tasks` endpoint. Must be overridden before exposing task listing; the base implementation fails closed to avoid leaking evaluator-only data.
- `check_auth(headers)` — legacy boolean auth hook. Override for custom auth that does not need tenant or dataset awareness.
- `resolve_tenant(headers)` — validate request authorization and return a tenant ID, `"_legacy"` for legacy auth, or `None` to reject.
- `check_dataset_access(tenant, dataset)` — return whether a resolved tenant may access a dataset.
- `get_service_version()` — optional benchmark-owned service version override. If it returns `None`, `/version` falls back to the installed benchmark package version.
- `get_dataset_version(dataset)` — optional dataset release/version hook. The value is returned on the dataset task-list response after auth and dataset access checks.

### FastAPI application factory (`app.py`)

`BenchmarkServiceApp(service_cls)` wraps your `BenchmarkService` subclass in a fully configured FastAPI app. Pass your subclass and run the result with any ASGI server.

**HTTP endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Returns `{"status": "ok"}` |
| `GET` | `/version` | Returns `{"framework_version": "...", "service_name": "...", "service_version": "..."}` |
| `GET` | `/verify-task-ids` | Return task IDs filtered by `?task_ids=…` or `?slice=start:stop:step` (optional `?dataset=…`) |
| `GET` | `/retrieve-task/?task_id=…` | Return task metadata for a given task ID (optional `?dataset=…`) |
| `POST` | `/evaluate-response/` | Evaluate without a sandbox and return one final response |
| `POST` | `/final-score/` | Aggregate results: `{"evaluation_results": {task_id: result, …}, "dataset": "…"}` |
| `POST` | `/v1/evaluate` | Lab-facing per-task evaluation (see [v1 Eval API](#v1-eval-api-lab-facing) below) |
| `POST` | `/v1/score` | Lab-facing run aggregation (see [v1 Eval API](#v1-eval-api-lab-facing) below) |
| `GET` | `/v1/datasets/{dataset}/tasks` | Lab-facing dataset task list (see [v1 Eval API](#v1-eval-api-lab-facing) below) |

**WebSocket endpoints** (stream `StreamChunk` JSON objects):

| Path | Description |
|------|-------------|
| `/ws/setup-task` | Set up a task in a sandbox; streams progress, errors, and a final result |
| `/ws/evaluate-response` | Evaluate without a sandbox; streams progress, checkpoint state, errors, and a final result |
| `/ws/evaluate-instance` | Evaluate a live sandbox solution; streams progress, errors, and a final result |

Sandbox setup and live sandbox evaluation use request-scoped `sandbox_provider` config so one hosted service can use different sandbox providers per request. Eval-only retry uses `/ws/evaluate-response` with `{"task_id": "…", "eval_resume_state": {...}, "sandbox_provider": {...}, "dataset": "…"}`. The provider config is optional because many benchmarks resume without a sandbox; include it when the benchmark must create one for evaluation. It is request-only and must not be embedded in persisted `eval_resume_state`.

#### Sandbox providers

`sandbox_provider` is selected per setup/evaluate-instance request:

```json
{"type": "daytona", "api_key": "...", "api_url": "...", "target": "..."}
```

or:

```json
{"type": "modal", "MODAL_TOKEN_ID": "...", "MODAL_TOKEN_SECRET": "...", "MODAL_ENVIRONMENT": "..."}
```

Modal credentials are required and resolved per request, exactly like Daytona's `DAYTONA_API_KEY`: the caller carries them in the `sandbox_provider` config so the process that creates and talks to the sandbox (the Valkyrie tracker) has them. The tracker resolves this config from AWS Secrets Manager via `sandbox_provider_secret_name`, so the Modal secret holds `MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET`, and optional `MODAL_ENVIRONMENT`. Only set `MODAL_ENVIRONMENT` when that Modal environment exists; otherwise Modal uses the active/default environment for the token profile.

Provider compatibility notes:

- Modal supports both `ImageSource` (registry pull) and `SnapshotSource` (a Modal filesystem snapshot created via `Sandbox.snapshot_filesystem()`, restored by image id).
- Modal sandboxes do not expose a disk-size parameter; `Resources.disk` is accepted for schema compatibility but not enforced.
- Nested Docker (Docker-in-Docker) capability is granted on every sandbox for both providers — Daytona supports it natively and the Modal adapter requests it unconditionally — so benchmarks never configure it. The benchmark service still owns the Docker-capable image, dockerd startup flags, compose workflow, and cleanup.
- Transient Modal connection errors are retried up to three attempts, matching the Daytona adapter's provider-level retry shape. Non-transient command failures still surface as `SandboxCommandError` with the command exit code.

Benchmark services can send `eval_resume_state` updates to the tracker while evaluation is running. The tracker stores the latest value and sends it back on eval-only retry, so the benchmark service can continue evaluation without recreating the original agent sandbox.

Eval-only retry flow:

1. A benchmark service yields `StreamEvalResumeStateChunk` before starting failure-prone evaluation work.
2. The tracker stores the latest `eval_resume_state` on the task row.
3. If evaluation fails after that point, retry calls `/ws/evaluate-response` with the saved state and, when needed, the request-scoped sandbox provider config.
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

**`POST /v1/score`** — aggregate across a run. Request `{run_id, dataset, evaluation_results: {task_id: {"status": "evaluated", "result": {...}} | {"status": "did_not_complete"} | null}}`. Before calling `calculate_final_score`, the framework converts every non-null item to the same eval-result envelope shape used by the runner and internal `/final-score/` path: `{"task_id": task_id, "status": status, "result": result}` plus `error` when the v1 item carries errors. Multiple v1 errors are collapsed into that single `error` string; callers should leave `errors` empty for successful `evaluated` items. `null` still represents a missing task and is passed through as `null`. Response `{run_id, tasks_evaluated, final_score, metadata}`.

**`POST /v1/submissions/upload-url`** — mint a presigned S3 PUT URL for a submission artifact (e.g. an agent workspace tarball the eval side later rehydrates). Request `{run_id, task_id, dataset?, filename}`; every field must be a plain key segment (`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`) and `task_id` must exist in the dataset. Response `{key, url, expires_in}`: the caller PUTs the artifact bytes to `url`, then reports `key` as the task's generation output. Deployments serving uploads must set `SUBMISSION_ARTIFACT_BUCKET` (the receiving S3 bucket) and `AWS_REGION` (the bucket's region — presigned URLs are signed per region). Without the bucket the endpoint returns 503; a bucket without a region fails at startup. Not available to trial tenants.

**Auth.** `/v1/*` is Descope-only. Callers using the legacy `BENCHMARK_API_KEY` bearer (i.e. those that resolve to the `_legacy` tenant sentinel) get 403. Migrate the deploy to Descope (`AUTH_REQUIRED=true` + `DESCOPE_PROJECT_ID` + allowlist) before opening `/v1/` to external traffic.

**`GET /v1/datasets/{dataset}/tasks`** — return the dataset's task list. Same Descope-only auth + tenant/dataset allowlist gate as the rest of `/v1/*`. Response:

```json
{
  "dataset": "validation",
  "dataset_version": "2026-06-10",
  "tasks": [
    {"id": "01-011", "question": "...", "timeout": null},
    {"id": "01-012", "question": "...", "timeout": 600}
  ]
}
```

Status codes: 403 if the tenant isn't allowed the dataset *or* if the caller uses legacy bearer; 404 if the dataset is in the tenant's allowlist but the service's `load_datasets()` doesn't load it; 501 if the benchmark has not implemented `list_tasks`. The base `BenchmarkService.list_tasks` does not infer a public task shape from internal task objects because those objects often include evaluator-only data (`answer`, `checks`, rubrics, grader config). Benchmarks must explicitly map their loaded tasks to `V1Task(id, question, timeout, ...)`. `V1Task` allows benchmark-specific extras, so benchmarks that need to surface additional per-task fields (e.g. SWE-bench's `repo`/`base_commit`) can include them when constructing the `V1Task` — document them in your benchmark's README and validate on the runner side with a typed `Task` subclass.

**Trial mode.** A tenant with `trial_mode: true` in its allowlist entry receives score-only responses on `/v1/evaluate` and `/v1/score`. Benchmarks enabling trial mode must implement `project_trial_result(result)` to return the audited per-task fields trial callers may see and resubmit to `/v1/score`; include any field `calculate_final_score` needs for aggregation. The framework still removes `evaluator_version` and error text from `/v1/evaluate` (the error *count* survives as generic `"error"` entries), and `/v1/score` `metadata` is emptied while `final_score` and `tasks_evaluated` remain. The sanitizer builds fresh response objects from allowlisted fields, so fields added later do not leak to trial callers by default. Unhandled server errors return a generic `{"detail": "Internal server error"}` 500 (no traceback) for every caller, not just trial tenants. Trial tenants may access only `/v1/evaluate`, `/v1/score`, and `GET /v1/datasets/{dataset}/tasks`; other `/v1/*`, internal `/evaluate-response/`, `/final-score/`, and `/ws/*` endpoints are denied (403). For trial tenants, the dataset task list is projected to `id`, `question`, and `timeout` even if the benchmark's normal `V1Task` includes extras.

**Deferred to follow-on plans.** `GET /v1/schema`, `GET /v1/tasks/{task_id}` (single-task lookup), `/ws/v1/evaluate` (streamed judges), artifact payloads, async/`poll_url` response shape, idempotency on `(run_id, task_id)`.

### Schemas (`schemas.py`)

Pydantic models used across requests and responses:

- **`RetrieveTaskResponse`** — `source`, `problem_path`, `cwd`, `agent_timeout`, `Resources`
- **`SandboxSource`** — `ImageSource(type="image", image=...)` or `SnapshotSource(type="snapshot", snapshot=...)`
- **`SandboxProviderConfig`** — request-scoped provider config selected by `type`; currently `DaytonaProviderConfig(type="daytona", DAYTONA_API_KEY, DAYTONA_API_URL, DAYTONA_TARGET)` or `ModalProviderConfig(type="modal", MODAL_TOKEN_ID, MODAL_TOKEN_SECRET, MODAL_ENVIRONMENT?)`
- **`Resources`** — `vcpu`, `memory`, `disk`
- **`SetupTaskRequest`** / **`EvaluateInstanceRequest`** — `task_id`, `instance_id`, optional `sandbox_provider` with Daytona header fallback, `dataset`
- **`EvaluateResponseRequest`** — `task_id`, `response` or `eval_resume_state`, optional `sandbox_provider`, `dataset`
- **`FinalScoreResult`** / **`FinalScoreResponse`** — `score` (float), `metadata`, `tasks_evaluated`
- **`TaskFilter`** — `task_ids` list or `slice_str`; `parse_slice()` converts `"start:stop:step"` to a Python `slice`

### Utilities (`utils.py`)

**`stream_command(sandbox, command, cwd, ignore_error=False)`**

Runs a shell command inside a sandbox and yields output in real time. Checks the exit code after the command finishes. Use it inside `setup_task` and `evaluate_instance` to run commands and forward output as `StreamMessageChunk`s.

### Authentication

The framework authenticates every HTTP request except `/health`, and every WebSocket route before sandbox provider config is used.

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
