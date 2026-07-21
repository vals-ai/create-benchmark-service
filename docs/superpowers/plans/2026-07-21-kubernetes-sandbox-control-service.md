# Kubernetes Sandbox Control Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the private in-cluster service that turns the Kubernetes provider protocol into isolated EKS Jobs with lifecycle, command/file streaming, Docker/Compose support, GPU/resource mapping, egress controls, and cleanup.

**Architecture:** A small FastAPI app authenticates the provider client and delegates to a `SandboxControlBackend`. `KubernetesSandboxBackend` reconciles Jobs and policies through a narrow Kubernetes API adapter, while a separate remote-exec adapter handles WebSocket channels and streaming base64 file transfer. The app and backend are stateless; Kubernetes metadata stores identity, fingerprints, and activity timestamps.

**Tech Stack:** Python 3.12, FastAPI, HTTPX/httpx-ws, kubernetes-asyncio 36.1, Kubernetes batch/core/custom-object APIs, CiliumNetworkPolicy, pytest, Ruff, BasedPyright.

## Global Constraints

- Do not deploy or create AWS resources from this branch.
- Load in-cluster Kubernetes credentials by default; local kubeconfig use must be explicitly enabled for development.
- Restrict the service account to the configured sandbox namespace.
- Use the configured `RuntimeClass`; requests cannot override it.
- `ImageSource` is supported; `SnapshotSource` fails explicitly until image plus workspace restoration exists.
- `ComposeSource` remains handled by `ComposeSandbox` around the outer image.
- Baseline egress is unrestricted to match existing tracker semantics; `modify_egress_rules` temporarily applies a fail-closed allowlist and `clear_egress_rules` removes it.
- Baseline ingress is denied.
- Stream command and file data incrementally; never accumulate it in the control app.
- Map GPU count to `nvidia.com/gpu` and require `gpu_type` so placement can use the configured GPU-type label.
- Images and sidecars are deployment-configured GovCloud ECR references; examples never hardcode credentials.
- Preserve `tests/__init__.py` without staging it.
- Commit and push each task separately after pulling the remote feature branch; never force push.

## File structure

- `src/benchmark_service/sandbox/kubernetes/control/__init__.py`: control-service exports.
- `src/benchmark_service/sandbox/kubernetes/control/settings.py`: validated in-cluster deployment settings.
- `src/benchmark_service/sandbox/kubernetes/control/backend.py`: backend protocol and control-layer result types.
- `src/benchmark_service/sandbox/kubernetes/control/app.py`: private HTTP/WebSocket protocol, authentication, and streaming responses.
- `src/benchmark_service/sandbox/kubernetes/control/resources.py`: deterministic Kubernetes names, request fingerprints, Job, NetworkPolicy, and Cilium policy manifests.
- `src/benchmark_service/sandbox/kubernetes/control/api.py`: narrow `kubernetes_asyncio` API adapter.
- `src/benchmark_service/sandbox/kubernetes/control/egress.py`: replaceable egress-policy interface and first Cilium implementation.
- `src/benchmark_service/sandbox/kubernetes/control/remote_exec.py`: Kubernetes exec session adapter, command events, and streaming base64 codecs.
- `src/benchmark_service/sandbox/kubernetes/control/kubernetes.py`: lifecycle reconciliation and operation implementation.
- `src/benchmark_service/sandbox/kubernetes/control/main.py`: environment-based process entrypoint.
- `tests/test_kubernetes_control_app.py`: ASGI protocol and auth tests.
- `tests/test_kubernetes_resources.py`: manifest/security/resource mapping tests.
- `tests/test_kubernetes_backend.py`: fake-API reconciliation, streaming, egress, and cleanup tests.
- `tests/integration/test_kubernetes_control_service.py`: opt-in disposable-cluster contract test.

---

### Task 1: Add the authenticated control API around a backend protocol

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/control/__init__.py`
- Create: `src/benchmark_service/sandbox/kubernetes/control/settings.py`
- Create: `src/benchmark_service/sandbox/kubernetes/control/backend.py`
- Create: `src/benchmark_service/sandbox/kubernetes/control/app.py`
- Create: `tests/test_kubernetes_control_app.py`

**Interfaces:**
- Consumes: protocol models from `benchmark_service.sandbox.kubernetes.protocol`.
- Produces: `SandboxControlBackend`, `KubernetesControlSettings`, and `create_kubernetes_control_app(settings, backend) -> FastAPI`.

- [ ] **Step 1: Write failing ASGI contract tests**

Create a `RecordingControlBackend` implementing lifecycle, command, file, and egress methods. Use HTTPX `ASGITransport` and `ASGIWebSocketTransport` to cover:

- missing or incorrect bearer tokens return HTTP 401 and WebSocket close code 1008;
- create/get/list/delete preserve payloads and pagination;
- exec returns one `ExecResponse`;
- command forwards two output chunks and one exit event in order;
- upload consumes multiple request chunks;
- download returns two response chunks without joining;
- egress PUT and DELETE reach distinct backend methods; and
- `/health` returns `{"status": "ok"}` without exposing configuration.
- every HTTP response has `X-Request-ID`, and WebSocket error events carry the same request ID used in logs.

The backend protocol used by the test is:

```python
class SandboxControlBackend(Protocol):
    async def create_sandbox(self, request: SandboxCreateRequest) -> SandboxRecord: ...
    async def get_sandbox(self, instance_id: str) -> SandboxRecord: ...
    async def list_sandboxes(
        self,
        labels: dict[str, str],
        limit: int,
        continue_token: str | None,
    ) -> SandboxListPage: ...
    async def delete_sandbox(self, instance_id: str) -> None: ...
    async def exec(self, instance_id: str, request: CommandRequest) -> ExecResponse: ...
    def command(self, instance_id: str, request: CommandRequest) -> AsyncGenerator[CommandEvent, None]: ...
    async def upload_file(self, instance_id: str, remote_path: str, chunks: AsyncIterable[bytes]) -> None: ...
    def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]: ...
    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None: ...
    async def clear_egress_rules(self, instance_id: str) -> None: ...
    async def close(self) -> None: ...
```

Add an internal `SandboxConflictError(SandboxError)` for same-name/different-fingerprint create conflicts. It is not exported from `benchmark_service.sandbox`.

- [ ] **Step 2: Run the ASGI tests and confirm missing modules**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_control_app.py -q`

Expected: collection fails because the control package does not exist.

- [ ] **Step 3: Add validated settings and backend protocol**

Implement `KubernetesControlSettings` as a Pydantic model with:

```python
namespace: str = "benchmark-sandboxes"
api_token: str = Field(min_length=1)
runtime_class_name: str = "kata-qemu"
sandbox_container_name: str = "sandbox"
docker_image: str
docker_enabled: bool = True
hard_lifetime_seconds: int = Field(default=86400, gt=0)
finished_ttl_seconds: int = Field(default=300, ge=0)
exec_output_limit_bytes: int = Field(default=16 * 1024 * 1024, gt=0)
janitor_interval_seconds: int = Field(default=60, gt=0)
gpu_resource_name: str = "nvidia.com/gpu"
gpu_type_label: str = "sandbox.vals.ai/gpu-type"
allowed_image_prefixes: tuple[str, ...] = ()
require_image_digest: bool = False
allow_local_kubeconfig: bool = False
```

Reject empty namespace/runtime/container/image values. Keep the backend protocol signatures exactly as shown in Step 1.

- [ ] **Step 4: Implement HTTP/WebSocket routes and error envelopes**

`create_kubernetes_control_app` must:

- compare bearer tokens with `secrets.compare_digest`;
- accept or generate one request ID, return it as `X-Request-ID`, and include it in every error envelope without logging bearer tokens;
- parse repeated `label=name=value` parameters and reject malformed labels with 422;
- pass `request.stream()` directly to upload;
- return `StreamingResponse(backend.stream_download(...), media_type="application/octet-stream")`;
- accept a WebSocket, authenticate before command execution, receive one `CommandRequest`, and send `event.model_dump(mode="json")` for every backend event;
- map shared sandbox errors to `ControlErrorResponse`, with 404 for not found, 503 for connection errors, 409 for `SandboxConflictError`, and 500 for other sandbox errors; send the equivalent `CommandErrorEvent` after an accepted WebSocket fails; and
- close the backend during FastAPI lifespan shutdown.

- [ ] **Step 5: Verify the private API contract**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_control_app.py -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_control_app.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_control_app.py
```

Expected: all ASGI protocol and auth cases pass.

- [ ] **Step 6: Commit and push**

```bash
git add src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_control_app.py
git commit -m "Add Kubernetes sandbox control API"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 2: Build deterministic and secure Kubernetes resources

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/control/resources.py`
- Create: `tests/test_kubernetes_resources.py`

**Interfaces:**
- Consumes: `SandboxCreateRequest`, `ImageSource`, `KubernetesControlSettings`, and `resolve_allowed_addresses`.
- Produces: `sandbox_name`, `safe_label_value`, `request_fingerprint`, `build_job`, `build_ingress_policy`, and `build_egress_policy`.

- [ ] **Step 1: Write failing table-driven resource tests**

Cover these cases in one focused test:

- unsafe and long names become DNS labels no longer than 63 characters with an eight-character SHA-256 suffix;
- fingerprints are stable across label/env insertion order and change when image or resources change;
- CPU is rendered as a whole number string, memory/disk as `Gi`, GPU as the configured extended resource;
- GPU requests without `gpu_type` raise `SandboxError`;
- `SnapshotSource` raises an explicit unsupported-source error;
- the Job uses `backoffLimit: 0`, active deadline, finished TTL, Kata runtime class, `automountServiceAccountToken: false`, RuntimeDefault seccomp, no host namespace/path fields, and required owner labels;
- the workspace `emptyDir.sizeLimit` equals requested disk;
- the Docker sidecar uses the configured image, a shared socket volume, and `privileged: true` only when enabled;
- the main container has a shell exec readiness probe, and images outside configured prefixes or lacking a required digest are rejected before Job creation;
- baseline ingress policy denies ingress and does not restrict egress;
- egress policy includes DNS plus exact Cilium `toCIDR` and `toFQDNs` rules; and
- an empty allowlist is rejected by `resolve_allowed_addresses`.

- [ ] **Step 2: Run resource tests and confirm missing builders**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_resources.py -q`

Expected: collection fails because `resources.py` does not exist.

- [ ] **Step 3: Implement names and fingerprints**

Use lowercase DNS normalization, collapse invalid runs to `-`, trim separators, default to `sandbox`, and append `-{sha256(original)[:8]}` when normalization changes the name or exceeds 63 characters. Compute the request fingerprint from canonical JSON:

```python
payload = request.model_dump(mode="json")
encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
return hashlib.sha256(encoded).hexdigest()
```

- [ ] **Step 4: Implement Job and policy manifests**

Build a `batch/v1` Job with labels:

```python
{
    "app.kubernetes.io/managed-by": "benchmark-sandbox-control",
    "sandbox.vals.ai/id": resource_name,
    "sandbox.vals.ai/request-name": safe_label_value(request.name),
    "sandbox.vals.ai/fingerprint": request_fingerprint(request),
}
```

Store the original name, auto-stop minutes, and last-activity UTC timestamp in annotations. The primary container runs `sh -lc 'trap : TERM INT; while :; do sleep 3600; done'`, has an exec readiness probe using the same shell, mounts `/workspace` and `/var/run`, and receives `DOCKER_HOST=unix:///var/run/docker.sock` when Docker is enabled. Add a dind sidecar with readiness probe `docker info`, the same socket volume, and no host mounts. Map GPU count to both requests and limits and add `{gpu_type_label: gpu_type}` to the node selector. Validate the main and Docker images against `allowed_image_prefixes`; when `require_image_digest` is true, require an `@sha256:` reference.

`build_ingress_policy` returns a standard `networking.k8s.io/v1` policy with only `policyTypes: [Ingress]`. `build_egress_policy` returns one `cilium.io/v2` policy selecting the sandbox ID, allowing kube-dns on UDP/TCP 53 plus resolved CIDRs/domains.

- [ ] **Step 5: Verify the design and manifest semantics remain aligned**

Compare the manifest assertions to the design: unrestricted baseline egress, temporary fail-closed allowlists, unrestricted clear, denied ingress, and GPU placement must use the same terms and behavior. Fix either document or implementation if they differ.

- [ ] **Step 6: Verify resource generation**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_resources.py -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes/control/resources.py tests/test_kubernetes_resources.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes/control/resources.py tests/test_kubernetes_resources.py
```

Expected: all manifest assertions pass and no request can override runtime or namespace.

- [ ] **Step 7: Commit and push**

```bash
git add src/benchmark_service/sandbox/kubernetes/control/resources.py tests/test_kubernetes_resources.py
git commit -m "Define EKS sandbox resources"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 3: Reconcile lifecycle through kubernetes-asyncio

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `src/benchmark_service/sandbox/kubernetes/control/api.py`
- Create: `src/benchmark_service/sandbox/kubernetes/control/kubernetes.py`
- Create: `tests/test_kubernetes_backend.py`

**Interfaces:**
- Consumes: resource builders, settings, backend protocol, and protocol records.
- Produces: `KubernetesApi`, `KubernetesAsyncioApi`, and `KubernetesSandboxBackend` lifecycle methods.

- [ ] **Step 1: Add the Kubernetes async client**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv add 'kubernetes-asyncio>=36.1.0'`

Expected: the direct dependency and lock entries are added.

- [ ] **Step 2: Write failing fake-API lifecycle tests**

Create a fake `KubernetesApi` recording creates, reads, lists, patches, and deletes. Cover:

- create writes ingress policy before Job and waits until the Pod is ready;
- same-name/same-fingerprint create reuses the running Job;
- same-name/different-fingerprint raises `SandboxError` with “conflicting specification”;
- pending image pull, unschedulable, quota, and timeout conditions have distinct messages;
- get derives running, pending, failed, and stopped states from Job/Pod conditions;
- list passes a label selector, one page limit, and continuation token;
- delete removes egress policy, Job, and ingress policy and treats 404 as success; and
- Kubernetes 401/403/429/5xx errors map to stable shared errors.

- [ ] **Step 3: Run backend tests and confirm missing implementation**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_backend.py -k lifecycle -q`

Expected: collection fails because the API and backend modules do not exist.

- [ ] **Step 4: Implement the narrow Kubernetes API adapter**

Define `KubernetesApi` as a protocol over dictionaries rather than exposing generated model types. Include only these operations:

```python
async def create_job(namespace: str, body: dict[str, object]) -> dict[str, object]: ...
async def get_job(namespace: str, name: str) -> dict[str, object] | None: ...
async def list_jobs(namespace: str, label_selector: str, limit: int, continue_token: str | None) -> dict[str, object]: ...
async def patch_job(namespace: str, name: str, body: dict[str, object]) -> None: ...
async def delete_job(namespace: str, name: str) -> None: ...
async def list_pods(namespace: str, label_selector: str) -> list[dict[str, object]]: ...
async def create_network_policy(namespace: str, body: dict[str, object]) -> None: ...
async def delete_network_policy(namespace: str, name: str) -> None: ...
async def replace_custom_object(namespace: str, plural: str, name: str, body: dict[str, object]) -> None: ...
async def delete_custom_object(namespace: str, plural: str, name: str) -> None: ...
async def close() -> None: ...
```

`KubernetesAsyncioApi.create(settings)` first tries in-cluster config. It may load kubeconfig only when `allow_local_kubeconfig` is true. Configure `BatchV1Api`, `CoreV1Api`, `NetworkingV1Api`, and `CustomObjectsApi` from one `ApiClient` and normalize generated objects with `ApiClient.sanitize_for_serialization`.

- [ ] **Step 5: Implement lifecycle reconciliation**

`KubernetesSandboxBackend` creates the ingress policy first, then reconciles the Job by deterministic name/fingerprint. Poll Pod/Job state with a monotonic deadline and a 0.5-second test-injectable wait. Convert metadata to `SandboxRecord`, implement label-selector escaping, and preserve Kubernetes continuation tokens in `SandboxListPage`.

Wrap safe lifecycle API operations in three bounded attempts with jitter for HTTP 429 and 5xx responses. On partial create failure, delete only resources created by that request. Do not delete a reused Job. `close()` closes the API adapter.

- [ ] **Step 6: Verify lifecycle behavior**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_backend.py -k lifecycle -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_backend.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_backend.py
```

Expected: all lifecycle and mapping cases pass.

- [ ] **Step 7: Commit and push**

```bash
git add pyproject.toml uv.lock src/benchmark_service/sandbox/kubernetes/control/api.py src/benchmark_service/sandbox/kubernetes/control/kubernetes.py tests/test_kubernetes_backend.py
git commit -m "Reconcile EKS sandbox lifecycle"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 4: Relay commands and files through Kubernetes exec streams

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/control/remote_exec.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/kubernetes.py`
- Modify: `tests/test_kubernetes_backend.py`

**Interfaces:**
- Consumes: ready Pod lookup, `CommandRequest`, and configured output cap.
- Produces: `RemoteExec`, `RemoteExecSession`, incremental command events, and streaming file codecs.

- [ ] **Step 1: Write failing remote-exec and codec tests**

Use a fake exec session whose stdout chunks split `"€"` across UTF-8 boundaries and whose status channel returns exit code 7. Cover:

- output chunks appear before exit;
- stderr is relayed and the client can merge it;
- buffered exec enforces `exec_output_limit_bytes` and returns the exit code;
- cwd, timeout, and environment values are shell quoted and environment names are validated;
- upload base64 encoding handles input split at every one- and two-byte boundary;
- download decoding handles whitespace and base64 quartets split across arbitrary frames;
- file paths containing spaces and shell metacharacters are quoted; and
- cancellation closes the session and invokes best-effort termination without replay.

- [ ] **Step 2: Run streaming tests and confirm missing implementation**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_backend.py -k 'command or exec or file or base64' -q`

Expected: failures because remote exec is not implemented.

- [ ] **Step 3: Implement streaming codecs and exec adapter**

Define `RemoteExecSession` with `read_stdout`, `read_stderr`, `write_stdin`, `close_stdin`, `update`, `is_open`, `return_code`, and `close`. `KubernetesRemoteExec.open` calls `kubernetes_asyncio.stream.stream(..., _preload_content=False)` for the configured container.

Implement base64 helpers with carry buffers:

```python
async def encode_base64_chunks(chunks: AsyncIterable[bytes]) -> AsyncGenerator[str, None]:
    carry = b""
    async for chunk in chunks:
        data = carry + chunk
        boundary = len(data) - (len(data) % 3)
        if boundary:
            yield base64.b64encode(data[:boundary]).decode("ascii")
        carry = data[boundary:]
    if carry:
        yield base64.b64encode(carry).decode("ascii")
```

The decoder removes ASCII whitespace, decodes complete four-character groups, and validates the final group. No helper may join all chunks.

- [ ] **Step 4: Implement backend command and file methods**

Wrap commands with a generated command ID, quoted environment assignments, optional `cd`, optional `timeout`, and `2>&1`. Patch last activity before opening exec. Stream decoded output using an incremental UTF-8 decoder with replacement on invalid bytes. Emit one `CommandExitEvent` after all channels drain.

Upload runs `mkdir -p <parent> && base64 -d > <path>` and feeds encoded request chunks through stdin. Download runs `base64 <path>` and yields decoded bytes. A missing file maps to `SandboxError` with the remote path but never includes file content.

- [ ] **Step 5: Verify true streaming and bounded buffering**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_backend.py -k 'command or exec or file or base64' -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes/control/remote_exec.py src/benchmark_service/sandbox/kubernetes/control/kubernetes.py tests/test_kubernetes_backend.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes/control/remote_exec.py src/benchmark_service/sandbox/kubernetes/control/kubernetes.py tests/test_kubernetes_backend.py
```

Expected: arbitrary chunk boundaries round-trip and command events arrive incrementally.

- [ ] **Step 6: Commit and push**

```bash
git add src/benchmark_service/sandbox/kubernetes/control/remote_exec.py src/benchmark_service/sandbox/kubernetes/control/kubernetes.py tests/test_kubernetes_backend.py
git commit -m "Stream EKS sandbox commands and files"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 5: Add egress reconciliation and idle cleanup

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/control/egress.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/kubernetes.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/app.py`
- Modify: `tests/test_kubernetes_backend.py`
- Modify: `tests/test_kubernetes_control_app.py`

**Interfaces:**
- Consumes: Cilium policy builder, Job activity annotations, and idempotent API deletes.
- Produces: `EgressPolicyDriver`, `CiliumEgressPolicyDriver`, replace/clear egress, and `delete_idle_sandboxes(now: datetime) -> int`.

- [ ] **Step 1: Write failing egress and janitor tests**

Cover mixed domains/CIDRs, replacement of an existing policy, clear-on-404, activity refresh on every operation, deletion only after `auto_stop_interval`, retention when interval is zero, malformed timestamp handling, duplicate janitor execution, and janitor cancellation during app shutdown.

- [ ] **Step 2: Run focused tests and confirm failure**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_backend.py tests/test_kubernetes_control_app.py -k 'egress or idle or janitor' -q`

Expected: failures because egress and cleanup methods are incomplete.

- [ ] **Step 3: Implement egress replacement and clear**

Define an `EgressPolicyDriver` protocol with `apply(instance_id, allowed_addresses)` and `clear(instance_id)`. `CiliumEgressPolicyDriver` validates with `resolve_allowed_addresses`, builds the Cilium policy, and uses create-or-replace semantics. Inject the driver into `KubernetesSandboxBackend`; `clear` deletes only the per-sandbox egress policy, so the ingress-only baseline remains and egress becomes unrestricted. Patch activity only after the driver operation succeeds.

- [ ] **Step 4: Implement idle cleanup and app lifespan task**

List all managed Jobs, parse the UTC activity annotation and `auto_stop_interval`, and call the ordinary idempotent delete path for expired Jobs. The app starts one loop that waits `janitor_interval_seconds`, runs cleanup, logs failures without exiting, and cancels/awaits the loop during shutdown. Multiple replicas may delete the same Job safely.

- [ ] **Step 5: Verify cleanup and egress**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_backend.py tests/test_kubernetes_control_app.py -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_backend.py tests/test_kubernetes_control_app.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_backend.py tests/test_kubernetes_control_app.py
```

Expected: all backend and control-app tests pass.

- [ ] **Step 6: Commit and push**

```bash
git add src/benchmark_service/sandbox/kubernetes/control/egress.py src/benchmark_service/sandbox/kubernetes/control/kubernetes.py src/benchmark_service/sandbox/kubernetes/control/app.py tests/test_kubernetes_backend.py tests/test_kubernetes_control_app.py
git commit -m "Add EKS sandbox egress and cleanup"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 6: Add a non-deploying entrypoint, live harness, and final documentation

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/control/main.py`
- Create: `tests/integration/test_kubernetes_control_service.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/__init__.py`
- Modify: `pyproject.toml`
- Modify: `README.md`
- Modify: `docs/KUBERNETES_SANDBOX_PROVIDER.md`

**Interfaces:**
- Consumes: settings, API adapter, backend, remote exec, and app factory.
- Produces: `kubernetes-sandbox-control` process entrypoint and opt-in live contract verification.

- [ ] **Step 1: Add environment parsing tests**

Test that `load_settings()` requires `KUBERNETES_SANDBOX_API_TOKEN` and `KUBERNETES_SANDBOX_DOCKER_IMAGE`, parses numeric/boolean overrides, never logs the token, and defaults local kubeconfig to disabled.

- [ ] **Step 2: Implement the process entrypoint**

`main.py` reads environment variables into `KubernetesControlSettings`, creates `KubernetesAsyncioApi`, `KubernetesRemoteExec`, `KubernetesSandboxBackend`, and the FastAPI app. Add:

```toml
kubernetes-sandbox-control = "benchmark_service.sandbox.kubernetes.control.main:main"
```

`main()` starts Uvicorn on `KUBERNETES_SANDBOX_HOST` default `0.0.0.0` and `KUBERNETES_SANDBOX_PORT` default `8080`. It performs no installation or resource creation at import time.

- [ ] **Step 3: Add an opt-in live test**

The integration test skips unless `TEST_KUBERNETES_CONTROL_URL`, `TEST_KUBERNETES_CONTROL_TOKEN`, and `TEST_KUBERNETES_IMAGE` exist. It creates one unique sandbox, proves lifecycle/get/list, multiple command chunks, binary upload/download and stream download, a temporary CIDR egress rule, clear, and deletion in `finally`. It does not install controllers or change cluster configuration.

- [ ] **Step 4: Finish operator documentation**

Document required environment variables, namespace-scoped RBAC verbs/resources, private load-balancer requirement, ECR digest policy, Kata/bare-metal GovCloud gate, Cilium/Kata validation, GPU node labels, Docker sidecar, health check, local ASGI tests, live test command, and explicit no-deploy status.

- [ ] **Step 5: Run complete verification**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check .
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/ tests/
git diff --check origin/main...HEAD
```

Expected: full suite passes with only known baseline warnings; no live test runs without explicit configuration; Ruff and BasedPyright report no errors.

- [ ] **Step 6: Commit and push**

```bash
git add pyproject.toml src/benchmark_service/sandbox/kubernetes/control/main.py src/benchmark_service/sandbox/kubernetes/control/__init__.py tests/integration/test_kubernetes_control_service.py README.md docs/KUBERNETES_SANDBOX_PROVIDER.md
git commit -m "Document EKS sandbox control service"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```
