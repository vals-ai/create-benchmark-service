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
| `GET` | `/version` | Returns framework, service, and dataset versions plus the benchmark's `eval_mode` |
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

`sandbox_provider` is selected per setup/evaluate-instance request and carries the provider credentials. The tracker resolves it from AWS Secrets Manager (`sandbox_provider_secret_name`); the secret's keys are the config fields:

```json
{"type": "daytona", "api_key": "...", "api_url": "...", "target": "..."}
{"type": "modal", "MODAL_TOKEN_ID": "...", "MODAL_TOKEN_SECRET": "..."}
```

Provider compatibility notes:

- `Sandbox.labels` and `Sandbox.created_at` expose provider inventory metadata when available; unsupported metadata is `None`, and creation times are timezone-aware UTC. `SandboxQuery.created_at_lte` is an inclusive creation-time bound. Daytona supports it and always limits listing to the provider's configured target, which may be a Daytona region name or ID. Modal rejects creation-time-bounded listing.
- Modal supports both `ImageSource` (registry pull) and `SnapshotSource` (a Modal filesystem snapshot created via `Sandbox.snapshot_filesystem()`, restored by image id). `TargetedSnapshotSource` is Daytona-only.
- Daytona uses `TargetedSnapshotSource(snapshot=..., target=...)` to select a target only when creating that sandbox. Its legacy `docker_image` value is intentionally invalid because that field cannot preserve the target.
- Modal sandboxes do not expose a disk-size parameter; `Resources.disk` is accepted for schema compatibility but not enforced.
- GPUs are requested via `Resources.gpu` (count) and `Resources.gpu_type`. Modal requires `gpu_type` (any Modal GPU name, e.g. `H100`, `A100-80GB`, `T4`) and passes `"<type>:<count>"` to the sandbox. Daytona accepts a count with an optional type restricted to its `GpuType` enum (`H100`, `H200`, `RTX-PRO-6000`, `RTX-4090`, `RTX-5090`); GPU requests are rejected for Daytona snapshot sandboxes because snapshot resources are fixed at snapshot creation.
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

`/v1/evaluate` and `/v1/score` are the lab-facing surface. Text-mode evaluation reuses `evaluate_response`; byte-backed and materialized artifact modes call their corresponding in-process hooks; and sandbox-mode evaluation runs `evaluate_instance`. Scoring reuses `calculate_final_score` for every mode.

### Byte-backed artifact evaluation (`eval_mode = IN_PROCESS_ARTIFACT`)

Benchmarks that grade an uploaded file inside the service process declare
`eval_mode = EvalMode.IN_PROCESS_ARTIFACT`, declare only the artifact schema
IDs they accept, and implement
`evaluate_artifact(run_id, task_id, schema_id, artifact, dataset)`. The framework
validates the authenticated tenant, dataset, task, schema, and upload key,
captures the artifact's size and ETag, and downloads that admitted version
before invoking benchmark code. The hook receives bytes and never receives an
object key, bucket, or tenant credential. Use this mode only when the configured
maximum artifact size is safe to hold in service memory.

### Materialized artifact evaluation (`eval_mode = IN_PROCESS_MATERIALIZED_ARTIFACT`)

Benchmarks that need file-backed intake declare
`eval_mode = EvalMode.IN_PROCESS_MATERIALIZED_ARTIFACT`, declare only the
artifact schema IDs they accept, and implement
`evaluate_materialized_artifact(tenant, run_id, task_id, schema_id, artifact, dataset)`.
The framework streams the admitted object to a temporary file. The hook receives
the authenticated tenant and a `MaterializedSubmissionArtifact` containing the
read-only local `path` and immutable `reference`; it never receives a bucket or
tenant credential. The path remains valid only while the hook's stream is being
consumed and must not be retained after the hook finishes.

These deployments require `SUBMISSION_ARTIFACT_BUCKET` and `AWS_REGION`.
Set `SUBMISSION_ARTIFACT_MAX_DOWNLOAD_BYTES` to the largest artifact the
deployment accepts and provision temporary disk for both that file and any
expanded or copied form the benchmark creates. The framework checks the size
captured at admission, the download response, and the bytes written to disk.
Artifact admission and evaluation share the normal grading concurrency and
duplicate-request limits. Text evaluation and the websocket resume endpoints
remain unchanged.

### Sandbox-based evaluation (`eval_mode = SANDBOX`)

Benchmarks that must execute the submitted artifact to grade it (run a test
suite, compile a proof) declare `eval_mode = EvalMode.SANDBOX` on the service
class, declare the exact payload schemas they accept, and implement
`prepare_grading_sandbox(sandbox, submission)`. On
`POST /v1/evaluate` the service then provisions a fresh, network-isolated
sandbox, materializes the submission, runs `evaluate_instance`, and deletes
the sandbox. Text benchmarks (`eval_mode == TEXT`, the default) are
unaffected, and the websocket endpoints (including `/ws/evaluate-response`
resume) keep their sandbox-less service-owned paths.

Submissions arrive either inline (`payload.type == "text"`) or by reference
(`payload.type == "artifact"`, where `payload.data` is the object key returned
by `/v1/submissions/upload-url`). For artifact submissions the endpoint
reserves grading capacity, then captures an immutable `artifact_reference`
containing the key, size, and ETag before any sandbox is provisioned. The same
reference binds the download to that admitted object version, which is placed
at `submission.sandbox_path` before the hook runs. Admission remains held
through bounded sandbox cleanup. The hook receives `TextGradingSubmission` or
`ArtifactGradingSubmission`, including the payload schema and semantic
`text`/`artifact_reference` field, so it never has to infer the submission type.

Daytona is the default grading provider and reads `DAYTONA_API_KEY`,
`DAYTONA_API_URL`, and `DAYTONA_TARGET`. Set
`GRADING_SANDBOX_PROVIDER=modal` to use Modal with `MODAL_TOKEN_ID` and
`MODAL_TOKEN_SECRET`. Artifact-capable benchmarks also require
`SUBMISSION_ARTIFACT_BUCKET` and `AWS_REGION`; missing server configuration
fails at startup. The task's `eval_sandbox` spec can override the grading
source, image-backed resources, timeout, and network policy. Evaluator secrets
must stay in private deployment configuration or benchmark-owned server code;
they are never returned in `RetrieveTaskResponse` or persisted with a
submission.

Grading sandboxes can use an image or snapshot, not `ComposeSource`. A task
whose generation environment uses `ComposeSource` must set
`eval_sandbox.source` to an image or snapshot because the grading service does
not start Docker and Compose services in a fresh outer sandbox.

Task retrieval, sandbox creation, submission materialization, and the setup
hook share a 600-second preparation bound. `eval_sandbox.timeout_s` (1800
seconds by default) starts fresh immediately before `evaluate_instance`, and
sandbox teardown has its own bound. Benchmark-owned generator cleanup must not
catch and suppress `asyncio.CancelledError`; Python cannot forcibly stop
in-process code that ignores cancellation. Each service process caps active grades
with `GRADING_MAX_CONCURRENCY` (default 4), queued grades with
`GRADING_MAX_QUEUED` (default equal to concurrency), and admitted work per
tenant with `GRADING_MAX_ADMITTED_PER_TENANT` (also default equal to
concurrency). Queue waits longer than `GRADING_QUEUE_TIMEOUT_S` (default 30)
return 429; duplicate `(tenant, run_id, task_id)` requests return 409.

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

`status` is one of `evaluated`, `did_not_complete`, `generation_error`, `error`. `result` is the benchmark-specific JSON-compatible value returned by `evaluate_response` in text mode or by the terminal result chunk from `evaluate_instance` in sandbox mode. `payload.type` is `"text"` for inline submissions; `"artifact"` (sandbox-grading benchmarks only) carries the uploaded submission's object key in `data`.

**`POST /v1/score`** — aggregate across a run. Request `{run_id, dataset, evaluation_results: {task_id: {"status": "evaluated", "result": {...}} | {"status": "did_not_complete"} | null}}`. Before calling `calculate_final_score`, the framework converts every non-null item to the same eval-result envelope shape used by the runner and internal `/final-score/` path: `{"task_id": task_id, "status": status, "result": result}` plus `error` when the v1 item carries errors. Multiple v1 errors are collapsed into that single `error` string; callers should leave `errors` empty for successful `evaluated` items. `null` still represents a missing task and is passed through as `null`. Response `{run_id, tasks_evaluated, final_score, metadata}`.

**`POST /v1/submissions/upload-url`** — mint a presigned S3 PUT URL for a submission artifact (e.g. an agent workspace tarball the eval side later rehydrates). Request `{run_id, task_id, dataset?, filename}`; every field must be a plain key segment (`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`) and `task_id` must exist in the dataset. Response `{key, url, expires_in}`: the caller PUTs the artifact bytes to `url`, then reports `key` as the task's generation output. Deployments serving uploads must set `SUBMISSION_ARTIFACT_BUCKET` (the receiving S3 bucket) and `AWS_REGION` (the bucket's region — presigned URLs are signed per region). Without the bucket the endpoint returns 503; a bucket without a region fails at startup. Server-side reads default to a 64 MiB limit; set `SUBMISSION_ARTIFACT_MAX_DOWNLOAD_BYTES` to the deployment's positive maximum accepted size. Invalid configured limits fail at startup. Not available to trial tenants.

The service role needs `s3:PutObject` and `s3:GetObject` on `arn:aws:s3:::BUCKET/submission-artifacts/*`, plus `s3:ListBucket` on `arn:aws:s3:::BUCKET` limited to the `submission-artifacts/*` namespace in the deployment policy. The list permission is part of the missing-object contract: [S3 returns 404 for a missing object only when the caller has `s3:ListBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_Permissions); without it S3 returns 403, which the framework leaves as a permission failure rather than misreporting the object as missing.

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

**Deferred to follow-on plans.** `GET /v1/schema`, `GET /v1/tasks/{task_id}` (single-task lookup), `/ws/v1/evaluate` (streamed judges), async/`poll_url` response shape, idempotency on `(run_id, task_id)`.

### Client-owned sandbox recovery

`BenchmarkServiceClient.run_with_sandbox_recovery(...)` owns the bounded loop for tasks that opt in with `SandboxRecoveryPolicy`. It loads the task once, retries only provider-confirmed `SandboxNotFoundError` losses, and counts every fresh-sandbox attempt against one overall cap. Callers can explicitly include setup errors that also require a fresh sandbox; `default_max_attempts` retains the legacy fallback cap for benchmarks without a recovery policy and bounds consecutive setup attempts when a policy is present.

The operation receives a `SandboxRecoveryAttempt`. Merge its `environment` into the sandbox environment, then call `mark_replacement_ready()` only after benchmark setup has durably recorded the outage. A failed replacement setup therefore carries the same outage ID into the next sandbox, while a later provider loss gets a globally unique ID that cannot collide after a runner restart. `sandbox_loss_retry_available` lets a runner persist its normal terminal task error without duplicating the policy calculation.

```python
async def run_attempt(attempt):
    task = await attempt.retrieve_task()
    async with create_sandbox(
        source=task.source,
        env_vars={**base_environment, **attempt.environment},
    ) as sandbox:
        await client.setup_task(task_id, sandbox.id)
        attempt.mark_replacement_ready()
        return await run_agent(sandbox)


result = await client.run_with_sandbox_recovery(
    task_id,
    run_id,
    run_attempt,
    retryable_attempt_errors=(TransientSandboxSetupError,),
    default_max_attempts=2,
)
```

### Schemas (`schemas.py`)

Pydantic models used across requests and responses:

- **`RetrieveTaskResponse`** — `source`, `problem_path`, `cwd`, `agent_timeout`, `resources`, optional persistent `volumes`, optional bounded `sandbox_recovery`, optional non-secret `eval_sandbox`
- **`SandboxRecoveryPolicy`** — explicit opt-in to recreate a lost generation sandbox with the same run identity and volumes; `max_sandbox_attempts` (2–20, inclusive) includes the initial sandbox
- **`SandboxSource`** — `ImageSource`, `SnapshotSource`, top-level Daytona-only `TargetedSnapshotSource`, or `ComposeSource` with an outer image/snapshot
- **`EvalSandboxSpec`** — grading overrides whose optional `source` is an image or snapshot, never `ComposeSource`
- **`GradingSubmission`** — typed `TextGradingSubmission` or `ArtifactGradingSubmission` passed only to the sandbox-grading hook
- **`SandboxProviderConfig`** — request-scoped provider config selected by `type`; currently `DaytonaProviderConfig(type="daytona", DAYTONA_API_KEY, DAYTONA_API_URL, DAYTONA_TARGET)` or `ModalProviderConfig(type="modal", MODAL_TOKEN_ID, MODAL_TOKEN_SECRET)`
- **`Resources`** — `vcpu`, `memory`, `disk`, optional `gpu` (count, default 0) and `gpu_type` (requires `gpu >= 1`)
- **`VolumeMount`** — named persistent volume, absolute `mount_path`, optional `read_only`, `create_if_missing`, and relative `subpath`; `{run_id}` in a subpath resolves from the sandbox's `run-id` or `run_id` label and fails when that label is absent
- **`SetupTaskRequest`** / **`EvaluateInstanceRequest`** — `task_id`, `instance_id`, optional `sandbox_provider` with Daytona header fallback, `dataset`
- **`EvaluateResponseRequest`** — `task_id`, `response` or `eval_resume_state`, optional `sandbox_provider`, `dataset`
- **`FinalScoreResult`** / **`FinalScoreResponse`** — `score` (float), `metadata`, `tasks_evaluated`
- **`TaskFilter`** — `task_ids` list or `slice_str`; `parse_slice()` converts `"start:stop:step"` to a Python `slice`

### Utilities (`utils.py`)

**`stream_command(sandbox, command, cwd, ignore_error=False)`**

Runs a shell command inside a sandbox and yields output in real time. Checks the exit code after the command finishes. Use it inside `setup_task` and `evaluate_instance` to run commands and forward output as `StreamMessageChunk`s.

For process-scoped credentials, call `sandbox.command(..., env_vars={...})`. Providers pass these values through their native process environment channel; Compose forwards only variable names in its command line, so values are not shell-interpolated.

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
    evaluation_quota:
      limit: 2500
      period: week
  vals-internal:
    datasets:
      - default
      - validation
      - test
```

Malformed configured allowlists raise at app startup. Unknown tenants receive `401 Unauthorized`. Known tenants requesting a dataset outside their allowlist receive `403 Dataset not allowed`; WebSocket routes close with code `1008`. The tenant ID `"_legacy"` is reserved for compatibility mode and is rejected as a Descope tenant.

**Evaluation quotas.** Add `evaluation_quota` to a tenant entry to cap that tenant's evaluation requests. Set `period` to `day`, `week`, `month`, or `year`. Periods use UTC calendar boundaries: days start at 00:00, weeks start Monday at 00:00, months start on the first day at 00:00, and years start January 1 at 00:00. Changing a tenant's period selects a separate counter namespace. `POST /v1/evaluate`, `POST /evaluate-response/`, `/ws/evaluate-response`, and `/ws/evaluate-instance` consume the same quota; task setup, task retrieval, and score aggregation do not. Authentication, request parsing, dataset authorization, and payload compatibility checks happen before the request is counted. Duplicate and immediate-capacity checks for admitted grading requests also happen first. Accepted requests then consume quota before waiting for an active grading slot or accessing submission storage. Once counted, a request still consumes quota if evaluation or another later step fails.

Quota-enabled deployments must set a stable, non-empty `SERVICE_NAME` and set `EVALUATION_QUOTA_TABLE_NAME` to a DynamoDB table whose string partition key is `quota_key` and whose TTL attribute is `expires_at`. `SERVICE_NAME` separates each benchmark service's counters when the table is shared and must not change during a quota period. The task role needs `dynamodb:UpdateItem` on the table.

Counter updates are atomic across service processes and are not retried because increments are not idempotent. A write that reaches DynamoDB counts even if its response is lost, so an ambiguous storage failure can return HTTP 503 or WebSocket close code `1011` after consuming one request. This fail-closed behavior prevents one incoming request from consuming multiple units and prevents an unavailable counter from bypassing the limit. Internal AWS errors are logged but not returned to the caller.

After the configured limit is reached, HTTP routes return 429 with `Retry-After` and WebSocket routes close with code `1008`. Missing counter configuration fails service startup instead of silently disabling enforcement.

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
