# Kubernetes Sandbox Provider Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a registered `kubernetes` provider whose lifecycle, command, file, and egress operations call the private EKS sandbox control API with real command and download streaming.

**Architecture:** `KubernetesProviderConfig` creates a `KubernetesSandboxProvider` backed by `KubernetesControlClientDriver`. HTTP handles lifecycle, buffered exec, files, and egress; `httpx-ws` carries command events over the same authenticated client. `KubernetesSandbox` keeps the shared framework contract and performs caller-side environment validation.

**Tech Stack:** Python 3.12, Pydantic 2, HTTPX 0.28, httpx-ws 0.9, pytest, Ruff, BasedPyright.

## Global Constraints

- Do not deploy or create AWS or Kubernetes resources from this branch.
- The public provider discriminator is exactly `type: "kubernetes"`.
- Runtime class, namespace, cloud region, and cluster credentials are not request fields.
- `Sandbox.command` and `Sandbox.stream_download` must yield chunks before completion without joining them.
- `Sandbox.exec` and `Sandbox.download_file` remain buffered because their shared return types are buffered.
- Commands and uploads are never automatically replayed after an unknown outcome.
- Preserve `tests/__init__.py` without staging it.
- Use `uv add`, `uv run pytest`, `uv run ruff`, and `uv run basedpyright`.
- Commit and push each task separately after pulling the latest remote branch; never force push.

## File structure

- `src/benchmark_service/sandbox/kubernetes/config.py`: public Pydantic provider configuration.
- `src/benchmark_service/sandbox/kubernetes/protocol.py`: private control-API request, response, and event models.
- `src/benchmark_service/sandbox/kubernetes/client.py`: authenticated HTTP/WebSocket runtime driver and error translation.
- `src/benchmark_service/sandbox/kubernetes/runtime.py`: runtime-driver interface; streaming is required, not a buffered fallback.
- `src/benchmark_service/sandbox/kubernetes/sandbox.py`: shared sandbox adapter and command environment validation.
- `src/benchmark_service/sandbox/kubernetes/provider.py`: lifecycle delegation.
- `src/benchmark_service/sandbox/kubernetes/__init__.py`: provider package exports.
- `src/benchmark_service/sandbox/__init__.py`: public provider union and top-level exports.
- `tests/test_kubernetes_sandbox.py`: adapter contract tests.
- `tests/test_kubernetes_client.py`: HTTP/WebSocket client behavior and error tests.
- `tests/test_client.py`: provider-cache coverage for the new config.
- `README.md` and `docs/KUBERNETES_SANDBOX_PROVIDER.md`: supported configuration and current deployment status.

---

### Task 1: Require genuine streaming at the runtime boundary

**Files:**
- Modify: `src/benchmark_service/sandbox/kubernetes/runtime.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/sandbox.py`
- Modify: `tests/test_kubernetes_sandbox.py`

**Interfaces:**
- Consumes: `validate_command_env(env_vars: Mapping[str, str] | None) -> dict[str, str]`.
- Produces: abstract `KubernetesRuntimeDriver.stream_download(instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]` and validated `KubernetesSandbox.command(...)`.

- [ ] **Step 1: Write failing behavior tests**

Update the mock driver so its stream is independent from `download_file`, then add invalid and reserved environment cases:

```python
async def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
    self.streamed_files.append((instance_id, remote_path))
    yield b"first"
    yield b"second"


async def test_command_validates_environment_before_calling_driver(self) -> None:
    driver = MockKubernetesRuntimeDriver()

    for env_vars, message in [({"BAD-NAME": "x"}, "Invalid"), ({"TERM": "x"}, "Reserved")]:
        with pytest.raises(ValueError, match=message):
            _ = [chunk async for chunk in driver.sandbox.command("true", env_vars=env_vars)]

    assert driver.streamed_commands == []
```

Change the existing download assertions to require `[b"first", b"second"]`, one `streamed_files` entry, and no buffered download for that path.

- [ ] **Step 2: Run the focused test and confirm the red state**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_sandbox.py -q`

Expected: failure because `stream_download` still calls `download_file` and command environments are not validated.

- [ ] **Step 3: Make streaming abstract and validate command environments**

Replace the concrete fallback in `KubernetesRuntimeDriver` with:

```python
@abstractmethod
def stream_download(
    self,
    instance_id: str,
    remote_path: str,
) -> AsyncGenerator[bytes, None]: ...
```

Import `validate_command_env` in `sandbox.py` and change `command` to:

```python
env = validate_command_env(env_vars) if env_vars is not None else None
return self._driver.command(
    self.id,
    command,
    cwd=cwd,
    timeout=timeout,
    env_vars=env,
)
```

- [ ] **Step 4: Verify the adapter contract**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_sandbox.py -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes tests/test_kubernetes_sandbox.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes tests/test_kubernetes_sandbox.py
```

Expected: all commands pass; the stream test observes two chunks.

- [ ] **Step 5: Commit and push**

```bash
git add src/benchmark_service/sandbox/kubernetes/runtime.py src/benchmark_service/sandbox/kubernetes/sandbox.py tests/test_kubernetes_sandbox.py
git commit -m "Require Kubernetes provider streaming"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 2: Add protocol models and the lifecycle client

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/protocol.py`
- Create: `src/benchmark_service/sandbox/kubernetes/client.py`
- Create: `tests/test_kubernetes_client.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/__init__.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Interfaces:**
- Consumes: `SandboxCreateRequest`, `SandboxQuery`, `ExecResult`, `KubernetesRuntimeDriver`, and `KubernetesSandbox`.
- Produces: abstract-until-Task-4 `KubernetesControlClientDriver`, `SandboxRecord`, `SandboxListPage`, and stable error mapping.

- [ ] **Step 1: Add the direct WebSocket dependency**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv add 'httpx-ws>=0.9.0'`

Expected: `pyproject.toml` lists `httpx-ws>=0.9.0`; `uv.lock` remains resolved.

- [ ] **Step 2: Write failing lifecycle, retry, and error tests**

Create `tests/test_kubernetes_client.py` with one HTTPX `MockTransport` test covering create, get, paginated list, idempotent delete, one retryable 503, and 404 translation. The handler must inspect `Authorization: Bearer test-token`, return two list pages, and use these records:

```python
SANDBOX = {"id": "sandbox-1", "name": "task-1", "state": "running"}

driver = LifecycleTestDriver(
    api_url="https://sandbox.internal",
    api_token="test-token",
    transport=httpx.MockTransport(handler),
)
```

`LifecycleTestDriver` is a test-only subclass that implements the not-yet-added command, file, and egress abstract methods with `raise AssertionError("operation is outside this lifecycle test")`. Production code remains abstract until those real methods land.

- [ ] **Step 3: Run the tests and confirm missing imports**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_client.py -q`

Expected: collection fails because the protocol and driver do not exist.

- [ ] **Step 4: Add the protocol models**

Create `protocol.py` with these public shapes:

```python
class SandboxRecord(BaseModel):
    id: str
    name: str
    state: str


class SandboxListPage(BaseModel):
    items: list[SandboxRecord]
    continue_token: str | None = None


class ExecResponse(BaseModel):
    exit_code: int
    output: str = ""


class CommandRequest(BaseModel):
    command: str
    cwd: str | None = None
    timeout: float | None = None
    env_vars: dict[str, str] | None = None


class CommandOutputEvent(BaseModel):
    type: Literal["stdout", "stderr"]
    data: str


class CommandExitEvent(BaseModel):
    type: Literal["exit"]
    exit_code: int


class CommandErrorEvent(BaseModel):
    type: Literal["error"]
    code: str
    message: str
    request_id: str | None = None


CommandEvent = Annotated[CommandOutputEvent | CommandExitEvent | CommandErrorEvent, Field(discriminator="type")]
command_event_adapter: TypeAdapter[CommandEvent] = TypeAdapter(CommandEvent)


class EgressRequest(BaseModel):
    allowed_addresses: list[str]


class ControlErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str | None = None


class ControlErrorResponse(BaseModel):
    error: ControlErrorDetail
```

Lifecycle create payloads use `SandboxCreateRequest.model_dump(mode="json")`; list labels use repeated `label=name=value` parameters and `limit`/`continue_token`.

- [ ] **Step 5: Add the lifecycle driver**

Create `client.py` with `KubernetesControlClientDriver(KubernetesRuntimeDriver)`. Its constructor accepts `api_url: str`, `api_token: str`, positive connect/request timeouts, and an optional HTTPX transport; it builds one HTTPX async client with the bearer header, no redirects, TLS verification, and connection limits. Implement `_request(method, path, retryable, **kwargs)` with three attempts, exponential delays `0.25`, `0.5`, retry on `httpx.TransportError` and statuses `500`, `502`, `503`, `504`, and no retry for other failures. Parse structured errors and map `not_found`/404 to `SandboxNotFoundError`, transport or retry exhaustion to `SandboxConnectionError`, and other failures to `SandboxError`.

Implement lifecycle methods exactly as follows:

```python
async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
    response = await self._request("POST", "/v1/sandboxes", retryable=True, json=request.model_dump(mode="json"))
    return self._sandbox(SandboxRecord.model_validate(response.json()))

async def get_sandbox(self, instance_id: str) -> Sandbox:
    response = await self._request("GET", f"/v1/sandboxes/{quote(instance_id, safe='')}", retryable=True)
    return self._sandbox(SandboxRecord.model_validate(response.json()))

async def delete_sandbox(self, instance_id: str) -> None:
    await self._request("DELETE", f"/v1/sandboxes/{quote(instance_id, safe='')}", retryable=True, not_found_ok=True)

async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
    continue_token: str | None = None
    yielded = 0
    while yielded < query.page_size:
        params = [("label", f"{name}={value}") for name, value in sorted(query.labels.items())]
        params.extend([("limit", str(query.page_size - yielded))])
        if continue_token:
            params.append(("continue_token", continue_token))
        response = await self._request("GET", "/v1/sandboxes", retryable=True, params=params)
        page = SandboxListPage.model_validate(response.json())
        for record in page.items:
            yield self._sandbox(record)
            yielded += 1
            if yielded == query.page_size:
                return
        if not page.continue_token:
            return
        continue_token = page.continue_token
```

`close()` closes the HTTPX client. Leave command, file, and egress operations abstract; lifecycle tests use the explicit test-only subclass until Tasks 3 and 4 complete the production methods.

- [ ] **Step 6: Verify lifecycle behavior**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_client.py -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes tests/test_kubernetes_client.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes tests/test_kubernetes_client.py
```

Expected: all selected checks pass.

- [ ] **Step 7: Commit and push**

```bash
git add pyproject.toml uv.lock src/benchmark_service/sandbox/kubernetes tests/test_kubernetes_client.py
git commit -m "Add Kubernetes provider control client"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 3: Implement command streaming and buffered exec

**Files:**
- Modify: `src/benchmark_service/sandbox/kubernetes/client.py`
- Modify: `tests/test_kubernetes_client.py`

**Interfaces:**
- Consumes: `CommandRequest`, `command_event_adapter`, `ExecResponse`, and the authenticated HTTPX client.
- Produces: genuine `command(...) -> AsyncGenerator[str, None]` and buffered `exec(...) -> ExecResult`; the driver remains abstract only for file and egress methods until Task 4.

- [ ] **Step 1: Add failing ASGI WebSocket tests**

Build a small FastAPI app using `ASGIWebSocketTransport`. Its command route accepts the initial JSON payload, sends two output events, and then an exit event. Cover:

```python
chunks = [chunk async for chunk in sandbox.command("printf hello", cwd="/workspace", timeout=3, env_vars={"FOO": "bar"})]
assert chunks == ["hel", "lo"]

with pytest.raises(SandboxCommandError) as exc_info:
    _ = [chunk async for chunk in failed_sandbox.command("false")]
assert exc_info.value.exit_code == 7
```

Add an HTTP exec response assertion returning `{"exit_code": 0, "output": "done"}` and verify all request fields.

- [ ] **Step 2: Run the command tests and confirm failure**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_client.py -k 'command or exec' -q`

Expected: failure because the production driver does not yet implement command methods.

- [ ] **Step 3: Implement command and exec**

Use `aconnect_ws(self._ws_url(path), client=self._client)` and send `CommandRequest.model_dump(mode="json")`. Receive JSON until one exit event. Yield each stdout/stderr `data` field immediately. Raise `SandboxCommandError(exit_code)` only after output events have been yielded. Map a command error event through the same stable control-error mapper. Convert HTTPX/WebSocket transport failures to `SandboxConnectionError` without replaying the command.

Implement buffered exec with one non-retried POST:

```python
response = await self._request(
    "POST",
    f"/v1/sandboxes/{quote(instance_id, safe='')}/exec",
    retryable=False,
    json=CommandRequest(command=command, cwd=cwd, timeout=timeout).model_dump(mode="json"),
)
result = ExecResponse.model_validate(response.json())
return ExecResult(exit_code=result.exit_code, output=result.output)
```

- [ ] **Step 4: Verify streaming behavior and types**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_client.py -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes/client.py tests/test_kubernetes_client.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes/client.py tests/test_kubernetes_client.py
```

Expected: the first chunk is observable before the server sends the exit event; nonzero exit is preserved.

- [ ] **Step 5: Commit and push**

```bash
git add src/benchmark_service/sandbox/kubernetes/client.py tests/test_kubernetes_client.py
git commit -m "Stream Kubernetes sandbox commands"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 4: Implement file streaming and egress operations

**Files:**
- Modify: `src/benchmark_service/sandbox/kubernetes/client.py`
- Modify: `tests/test_kubernetes_client.py`

**Interfaces:**
- Consumes: authenticated `_request`, `EgressRequest`, and URL-quoted sandbox IDs.
- Produces: upload, buffered download, genuine streaming download, egress replacement, and unrestricted-egress restore.

- [ ] **Step 1: Write failing file and egress tests**

Use an ASGI `StreamingResponse` that yields `b"first"`, waits on an event, then yields `b"second"`. Assert the client receives `b"first"` before releasing the second chunk. Assert `download_file` returns `b"firstsecond"`, upload preserves arbitrary binary content and the exact `path` query parameter, egress sends `{"allowed_addresses": [...]}`, and clear sends DELETE.

- [ ] **Step 2: Run the focused tests and confirm failure**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_client.py -k 'file or download or egress' -q`

Expected: failure because the production driver does not yet implement file and egress methods.

- [ ] **Step 3: Implement files and egress without replay**

Implement upload with one non-retried `PUT` using `content=content` and `params={"path": remote_path}`. Implement streaming download with:

```python
async with self._client.stream(
    "GET",
    self._url(f"/v1/sandboxes/{quote(instance_id, safe='')}/files"),
    params={"path": remote_path},
) as response:
    self._raise_for_response(response)
    async for chunk in response.aiter_bytes():
        if chunk:
            yield chunk
```

Wrap initial and mid-stream HTTPX transport failures as `SandboxConnectionError`. `download_file` joins `stream_download`. Egress uses non-retried `PUT` and idempotent `DELETE`; clearing restores unrestricted egress to match Daytona, Modal, and tracker semantics.

- [ ] **Step 4: Verify all client operations**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_client.py tests/test_kubernetes_sandbox.py -q
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check src/benchmark_service/sandbox/kubernetes tests/test_kubernetes_client.py tests/test_kubernetes_sandbox.py
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/benchmark_service/sandbox/kubernetes tests/test_kubernetes_client.py tests/test_kubernetes_sandbox.py
```

Expected: all operations pass and the stream test proves incremental delivery.

- [ ] **Step 5: Commit and push**

```bash
git add src/benchmark_service/sandbox/kubernetes/client.py tests/test_kubernetes_client.py
git commit -m "Add Kubernetes file and egress operations"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 5: Register, document, and verify the runnable provider client

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/config.py`
- Modify: `src/benchmark_service/sandbox/__init__.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/__init__.py`
- Modify: `README.md`
- Modify: `docs/KUBERNETES_SANDBOX_PROVIDER.md`
- Modify: `tests/test_app.py`
- Modify: `tests/test_client.py`
- Modify: `tests/test_kubernetes_client.py`
- Modify: `tests/test_sandbox_contract.py`

**Interfaces:**
- Consumes: complete concrete `KubernetesControlClientDriver` and provider adapters.
- Produces: registered `KubernetesProviderConfig`, request examples, app parsing coverage, and a concrete-subclass regression guard.

- [ ] **Step 1: Write failing public configuration and contract assertions**

Create tests for this exact configuration:

```python
parsed = sandbox_provider_config_from_mapping(
    {
        "type": "kubernetes",
        "KUBERNETES_API_URL": "https://sandbox.internal",
        "KUBERNETES_API_TOKEN": "secret-reference-value",
    }
)
assert isinstance(parsed, KubernetesProviderConfig)
```

Extend `test_get_sandbox_provider_uses_each_provider_config` to create and cache one Kubernetes provider.

Extend the existing provider parsing tests with:

```json
{
  "type": "kubernetes",
  "KUBERNETES_API_URL": "https://sandbox.internal",
  "KUBERNETES_API_TOKEN": "secret-reference-value"
}
```

Assert `KubernetesSandbox`, `KubernetesSandboxProvider`, and `KubernetesControlClientDriver` have empty `__abstractmethods__`.

- [ ] **Step 2: Run the public tests and confirm the unregistered state**

Run: `UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest tests/test_kubernetes_client.py tests/test_client.py::test_get_sandbox_provider_uses_each_provider_config tests/test_sandbox_contract.py -q`

Expected: provider mapping fails because `KubernetesProviderConfig` is not registered.

- [ ] **Step 3: Add and register configuration**

Create `config.py`:

```python
class KubernetesProviderConfig(BaseModel):
    type: Literal["kubernetes"] = "kubernetes"
    KUBERNETES_API_URL: AnyHttpUrl
    KUBERNETES_API_TOKEN: str = Field(min_length=1)
    KUBERNETES_CONNECT_TIMEOUT: float = Field(default=10, gt=0)
    KUBERNETES_REQUEST_TIMEOUT: float = Field(default=60, gt=0)

    def create_provider(self) -> SandboxProvider:
        driver = KubernetesControlClientDriver(
            api_url=str(self.KUBERNETES_API_URL),
            api_token=self.KUBERNETES_API_TOKEN,
            connect_timeout=self.KUBERNETES_CONNECT_TIMEOUT,
            request_timeout=self.KUBERNETES_REQUEST_TIMEOUT,
        )
        return KubernetesSandboxProvider(driver)
```

Add it to the discriminated `SandboxProviderConfig` union and package exports.

- [ ] **Step 4: Update documentation**

Replace the README's “cannot run sandboxes yet” statement with a private control-service configuration example. State clearly that the client is runnable but no control service or EKS resources are deployed by this branch. Update the rollout document to list true streaming, open baseline egress with temporary allowlists, GPU request mapping, and the separate live deployment gates.

- [ ] **Step 5: Run final provider-client verification**

Run:

```bash
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run pytest
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run ruff check .
UV_CACHE_DIR=/tmp/cbs-kubernetes-uv-cache uv run basedpyright src/ tests/
git diff --check origin/main...HEAD
```

Expected: full suite passes with only known baseline warnings; Ruff and BasedPyright report no errors.

- [ ] **Step 6: Commit and push**

```bash
git add src/benchmark_service/sandbox/__init__.py src/benchmark_service/sandbox/kubernetes/config.py src/benchmark_service/sandbox/kubernetes/__init__.py README.md docs/KUBERNETES_SANDBOX_PROVIDER.md tests/test_app.py tests/test_client.py tests/test_sandbox_contract.py tests/test_kubernetes_client.py
git commit -m "Document Kubernetes sandbox client"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```
