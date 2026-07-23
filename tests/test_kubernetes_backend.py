"""Tests for EKS sandbox lifecycle reconciliation and streaming helpers."""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from benchmark_service.sandbox.kubernetes.control.api import KubernetesApiError
from benchmark_service.sandbox.kubernetes.control.backend import SandboxConflictError
from benchmark_service.sandbox.kubernetes.control.egress import CiliumEgressPolicyDriver
from benchmark_service.sandbox.kubernetes.control.kubernetes import KubernetesSandboxBackend
from benchmark_service.sandbox.kubernetes.control.remote_exec import (
    RemoteExecSession,
    decode_base64_chunks,
    encode_base64_chunks,
)
from benchmark_service.sandbox.kubernetes.control.resource_data import PodEndpoint
from benchmark_service.sandbox.kubernetes.control.resources import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    build_job,
    user_label_key,
)
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandExitEvent,
    CommandOutputEvent,
    CommandRequest,
)
from benchmark_service.sandbox.types import (
    ImageSource,
    Resources,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
)

DIGEST = "a" * 64


class MockKubernetesApi:
    """Store dictionary resources and record narrow API operations."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, object]] = {}
        self.pods: dict[str, list[dict[str, object]]] = {}
        self.operations: list[tuple[str, object]] = []
        self.activity_patch_count = 0
        self.activity_refreshed = asyncio.Event()
        self.list_result: dict[str, object] | None = None
        self.get_error: KubernetesApiError | None = None
        self.ready_on_create = True
        self.create_conflict_job: dict[str, object] | None = None
        self.closed = False

    async def create_job(self, namespace: str, body: dict[str, object]) -> dict[str, object]:
        name = str(_metadata(body)["name"])
        self.operations.append(("create_job", (namespace, name)))
        if self.create_conflict_job is not None:
            self.jobs[name] = self.create_conflict_job
            self.pods[name] = [_ready_pod(name)]
            raise KubernetesApiError(409, "already exists")
        self.jobs[name] = body
        if self.ready_on_create:
            self.pods.setdefault(name, [_ready_pod(name)])
        return body

    async def get_job(self, namespace: str, name: str) -> dict[str, object] | None:
        self.operations.append(("get_job", (namespace, name)))
        if self.get_error:
            raise self.get_error
        return self.jobs.get(name)

    async def list_jobs(
        self,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
    ) -> dict[str, object]:
        self.operations.append(("list_jobs", (namespace, label_selector, limit, continue_token)))
        return self.list_result or {"items": list(self.jobs.values()), "metadata": {}}

    async def patch_job(self, namespace: str, name: str, body: dict[str, object]) -> None:
        self.operations.append(("patch_job", (namespace, name, body)))
        self.activity_patch_count += 1
        if self.activity_patch_count >= 2:
            self.activity_refreshed.set()
        if name in self.jobs:
            patch_annotations = cast(dict[str, Any], _metadata(body).get("annotations", {}))
            annotations = cast(
                dict[str, Any],
                _metadata(self.jobs[name]).setdefault("annotations", {}),
            )
            annotations.update(patch_annotations)

    async def delete_job(self, namespace: str, name: str) -> None:
        self.operations.append(("delete_job", (namespace, name)))
        self.jobs.pop(name, None)

    async def list_pods(self, namespace: str, label_selector: str) -> list[dict[str, object]]:
        name = label_selector.partition("=")[2]
        self.operations.append(("list_pods", (namespace, label_selector)))
        return self.pods.get(name, [])

    async def create_network_policy(self, namespace: str, body: dict[str, object]) -> None:
        self.operations.append(("create_ingress", (namespace, _metadata(body)["name"])))

    async def delete_network_policy(self, namespace: str, name: str) -> None:
        self.operations.append(("delete_ingress", (namespace, name)))

    async def replace_custom_object(
        self,
        namespace: str,
        plural: str,
        name: str,
        body: dict[str, object],
    ) -> None:
        self.operations.append(("replace_custom", (namespace, plural, name, body)))

    async def delete_custom_object(self, namespace: str, plural: str, name: str) -> None:
        self.operations.append(("delete_custom", (namespace, plural, name)))

    async def close(self) -> None:
        self.closed = True


class MockResourceCache:
    """Return watched resources and record cache use."""

    def __init__(self, job: dict[str, object], pod: dict[str, object]) -> None:
        self.job_value = job
        self.pod = pod
        self.operations: list[str] = []
        self.closed = False

    async def job(self, resource_name: str) -> dict[str, object] | None:
        self.operations.append(f"job:{resource_name}")
        return self.job_value

    async def pods(self, resource_name: str) -> list[dict[str, object]]:
        self.operations.append(f"pods:{resource_name}")
        return [self.pod]

    async def wait_ready(
        self,
        resource_name: str,
        timeout: float,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        del timeout
        self.operations.append(f"wait:{resource_name}")
        return self.job_value, [self.pod]

    async def ready_pod_name(self, resource_name: str) -> str:
        self.operations.append(f"ready:{resource_name}")
        return str(_metadata(self.pod)["name"])

    async def ready_pod_endpoint(self, resource_name: str) -> PodEndpoint:
        self.operations.append(f"endpoint:{resource_name}")
        return PodEndpoint(
            name=str(_metadata(self.pod)["name"]),
            ip=str(cast(dict[str, object], self.pod["status"])["podIP"]),
        )

    async def close(self) -> None:
        self.closed = True


def _metadata(resource: dict[str, object]) -> dict[str, Any]:
    value = resource["metadata"]
    assert isinstance(value, dict)
    return cast(dict[str, Any], value)


def _settings() -> KubernetesControlSettings:
    return KubernetesControlSettings(
        api_token="test-token",
        docker_image=f"registry.internal/docker@sha256:{DIGEST}",
        allowed_image_prefixes=("registry.internal/",),
        require_image_digest=True,
    )


def _request(**changes: object) -> SandboxCreateRequest:
    values: dict[str, object] = {
        "source": ImageSource(image=f"registry.internal/python@sha256:{DIGEST}"),
        "resources": Resources(vcpu=2, memory=4, disk=20),
        "name": "task-1",
        "labels": {"run_id": "run-1"},
        "env_vars": {},
        "auto_stop_interval": 30,
        "create_timeout": 1,
    }
    values.update(changes)
    return SandboxCreateRequest.model_validate(values)


def _ready_pod(name: str) -> dict[str, object]:
    return {
        "metadata": {"name": f"{name}-pod"},
        "status": {
            "phase": "Running",
            "podIP": "10.0.0.8",
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


class TestKubernetesSandboxBackendLifecycle:
    """Lifecycle reconciliation through the narrow Kubernetes API."""

    async def test_creates_reuses_lists_and_deletes_sandboxes(self) -> None:
        """Reconcile deterministic Jobs without leaking caller cluster control.

        Test cases:
        - Job readiness returns a running record without per-sandbox ingress writes.
        - Matching retries reuse while conflicting specifications fail.
        - Listing preserves selectors and continuation; delete removes every owned resource.
        """
        api = MockKubernetesApi()
        backend = KubernetesSandboxBackend(_settings(), api)
        request = _request()
        created = await backend.create_sandbox(request)
        first_create_operations = [operation[0] for operation in api.operations]
        reused = await backend.create_sandbox(request)

        api.list_result = {"items": list(api.jobs.values()), "metadata": {"continue": "next"}}
        page = await backend.list_sandboxes({"run_id": "run-1"}, 5, "prior")
        await backend.delete_sandbox(created.id)

        assert created == reused and created.state == "running"
        assert created.labels == request.labels
        assert "create_ingress" not in first_create_operations
        assert first_create_operations.count("create_job") == 1
        assert page.continue_token == "next" and [item.id for item in page.items] == [created.id]
        list_call = next(value for name, value in api.operations if name == "list_jobs")
        assert list_call == (
            "benchmark-sandboxes",
            f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{user_label_key('run_id')}=run-1",
            5,
            "prior",
        )
        assert ("delete_custom", ("benchmark-sandboxes", "ciliumnetworkpolicies", "task-1-egress")) in api.operations
        assert ("delete_job", ("benchmark-sandboxes", "task-1")) in api.operations
        assert not any(name == "delete_ingress" for name, _ in api.operations)

        conflicting = _request(resources=Resources(vcpu=3, memory=4, disk=20))
        api.jobs["task-1"] = build_job(request, _settings())
        with pytest.raises(SandboxConflictError, match="conflicting specification"):
            await backend.create_sandbox(conflicting)

        race_api = MockKubernetesApi()
        race_api.create_conflict_job = build_job(request, _settings())
        race_backend = KubernetesSandboxBackend(_settings(), race_api, jitter=lambda delay: delay)
        raced = await race_backend.create_sandbox(request)
        assert raced.id == "task-1"
        assert sum(name == "create_job" for name, _ in race_api.operations) == 1

    async def test_uses_shared_watch_cache_for_readiness_and_pod_state(self) -> None:
        """Avoid request-driven Kubernetes reads when the production cache is available.

        Test cases:
        - Create readiness, get, and list use the shared watched state.
        - No get-Job or list-Pod API calls are made for those operations.
        - Backend shutdown closes the watches before the API client.
        """
        api = MockKubernetesApi()
        request = _request()
        job = build_job(request, _settings())
        pod = _ready_pod("task-1")
        cache = MockResourceCache(job, pod)
        api.list_result = {"items": [job], "metadata": {}}
        backend = KubernetesSandboxBackend(_settings(), api, resource_cache=cache)

        created = await backend.create_sandbox(request)
        fetched = await backend.get_sandbox("task-1")
        listed = await backend.list_sandboxes({}, 10, None)
        await backend.close()

        assert created.state == fetched.state == listed.items[0].state == "running"
        assert "wait:task-1" in cache.operations
        assert cache.operations.count("pods:task-1") == 2
        assert not any(name in {"get_job", "list_pods"} for name, _ in api.operations)
        assert cache.closed is True

    async def test_reports_pending_failures_states_and_api_errors(self) -> None:
        """Translate readiness and API failures into stable sandbox errors.

        Test cases:
        - Image pull failures have an actionable message.
        - Missing resources and API authorization/availability use shared errors.
        - Completed and failed Jobs produce stable states.
        """
        request = _request()
        cases = [
            (
                {
                    "containerStatuses": [
                        {
                            "state": {
                                "waiting": {
                                    "reason": "ErrImagePull",
                                    "message": "pull access denied for registry.internal/private",
                                }
                            }
                        }
                    ]
                },
                "image pull",
            ),
        ]
        for pod_status, message in cases:
            api = MockKubernetesApi()
            job = build_job(request, _settings())
            api.jobs["task-1"] = job
            api.pods["task-1"] = [{"status": pod_status}]
            backend = KubernetesSandboxBackend(_settings(), api)
            with pytest.raises(SandboxError, match=message):
                await backend.create_sandbox(request)

        api = MockKubernetesApi()
        backend = KubernetesSandboxBackend(_settings(), api)
        with pytest.raises(SandboxNotFoundError, match="not found"):
            await backend.get_sandbox("missing")

        for status, error_type in [(403, SandboxError), (503, SandboxConnectionError)]:
            api.get_error = KubernetesApiError(status, "failed")
            with pytest.raises(error_type):
                await backend.get_sandbox("task-1")
        api.get_error = None

        for status_value, expected in [({"succeeded": 1}, "stopped"), ({"failed": 1}, "failed")]:
            job = build_job(request, _settings())
            job["status"] = status_value
            api.jobs["task-1"] = job
            api.pods["task-1"] = []
            assert (await backend.get_sandbox("task-1")).state == expected

        timeout_api = MockKubernetesApi()
        timeout_api.ready_on_create = False
        times = iter([0.0, 2.0])
        timeout_backend = KubernetesSandboxBackend(
            _settings(),
            timeout_api,
            monotonic=times.__next__,
        )
        with pytest.raises(SandboxConnectionError, match="Timed out"):
            await timeout_backend.create_sandbox(_request(create_timeout=1))
        assert "task-1" not in timeout_api.jobs
        assert not any(name == "delete_ingress" for name, _ in timeout_api.operations)

        await backend.close()
        assert api.closed is True


class MockRemoteExecSession:
    """Emit configured channel frames and record stdin without buffering in production code."""

    def __init__(
        self,
        updates: list[tuple[bytes, bytes, int | None]],
        *,
        finish_on_stdin_close: bool = False,
    ) -> None:
        self.updates = deque(updates)
        self.stdout: deque[bytes] = deque()
        self.stderr: deque[bytes] = deque()
        self.stdin: list[bytes] = []
        self.finish_on_stdin_close = finish_on_stdin_close
        self._return_code: int | None = None
        self.closed = False

    async def read_stdout(self) -> bytes:
        return self.stdout.popleft() if self.stdout else b""

    async def read_stderr(self) -> bytes:
        return self.stderr.popleft() if self.stderr else b""

    async def write_stdin(self, data: bytes) -> None:
        self.stdin.append(data)

    async def close_stdin(self) -> None:
        if self.finish_on_stdin_close:
            self.updates.append((b"", b"", 0))

    async def update(self, timeout: float) -> None:
        del timeout
        if not self.updates:
            return
        stdout, stderr, return_code = self.updates.popleft()
        if stdout:
            self.stdout.append(stdout)
        if stderr:
            self.stderr.append(stderr)
        if return_code is not None:
            self._return_code = return_code

    def is_open(self) -> bool:
        return bool(self.updates or self.stdout or self.stderr) or self._return_code is None

    @property
    def return_code(self) -> int | None:
        return self._return_code

    async def close(self) -> None:
        self.closed = True


class MockBlockingRemoteExecSession(MockRemoteExecSession):
    """Hold one exec update until the consumer cancels."""

    def __init__(self) -> None:
        super().__init__([])
        self.started = asyncio.Event()

    async def update(self, timeout: float) -> None:
        del timeout
        self.started.set()
        await asyncio.Event().wait()


class MockRemoteExec:
    """Select mock sessions by remote command and record quoted arguments."""

    def __init__(self) -> None:
        self.command_sessions: deque[MockRemoteExecSession] = deque()
        self.opened_commands: list[list[str]] = []
        self.sessions: list[MockRemoteExecSession] = []
        self.terminated: list[tuple[str, str]] = []
        self.closed = False

    async def open(
        self,
        pod_name: str,
        command: list[str],
        *,
        stdin: bool = False,
    ) -> RemoteExecSession:
        del pod_name
        self.opened_commands.append(command)
        shell_command = command[-1]
        if "base64 -d" in shell_command and stdin:
            session = MockRemoteExecSession([], finish_on_stdin_close=True)
        elif shell_command.startswith("base64 "):
            session = MockRemoteExecSession([(b"Zm", b"", None), (b"ly c3Q=\n", b"", 0)])
        else:
            session = self.command_sessions.popleft()
        self.sessions.append(session)
        return session

    async def terminate(self, pod_name: str, command_id: str) -> None:
        self.terminated.append((pod_name, command_id))

    async def close(self) -> None:
        self.closed = True


class MockPodAgent:
    """Record direct Pod command and file traffic."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.closed = False

    async def command(
        self,
        pod_ip: str,
        resource_name: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandOutputEvent | CommandExitEvent, None]:
        self.calls.append(("command", (pod_ip, resource_name, request)))
        yield CommandOutputEvent(type="stdout", data="direct")
        yield CommandExitEvent(type="exit", exit_code=0)

    async def upload_file(
        self,
        pod_ip: str,
        resource_name: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None:
        self.calls.append(("upload", (pod_ip, resource_name, remote_path, b"".join([chunk async for chunk in chunks]))))

    async def stream_download(
        self,
        pod_ip: str,
        resource_name: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]:
        self.calls.append(("download", (pod_ip, resource_name, remote_path)))
        yield b"direct-file"

    async def close(self) -> None:
        self.closed = True


class MockBlockingPodAgent(MockPodAgent):
    """Hold a direct command open without producing output."""

    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def command(
        self,
        pod_ip: str,
        resource_name: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandOutputEvent | CommandExitEvent, None]:
        self.calls.append(("command", (pod_ip, resource_name, request)))
        self.started.set()
        await self.release.wait()
        yield CommandExitEvent(type="exit", exit_code=0)


async def _chunks(values: list[bytes]) -> AsyncGenerator[bytes, None]:
    for value in values:
        yield value


class TestKubernetesSandboxBackendStreaming:
    """Remote command and file streaming through Kubernetes exec channels."""

    async def test_streams_commands_quotes_inputs_and_caps_buffered_exec(self) -> None:
        """Stream decoded output while preserving exit, quoting, and output limits.

        Test cases:
        - Split UTF-8, stderr, and a nonzero exit become ordered command events.
        - Cwd, timeout, and environment values are shell quoted.
        - Buffered exec rejects output beyond its configured cap.
        - Cancellation and early stream closure terminate the remote process.
        """
        api = MockKubernetesApi()
        request = _request()
        api.jobs["task-1"] = build_job(request, _settings())
        api.pods["task-1"] = [_ready_pod("task-1")]
        remote = MockRemoteExec()
        command_session = MockRemoteExecSession(
            [(b"start \xe2", b"", None), (b"\x82\xac", b"warning", None), (b"", b"", 7)]
        )
        remote.command_sessions.append(command_session)
        backend = KubernetesSandboxBackend(_settings(), api, remote)

        events = [
            event
            async for event in backend.command(
                "task-1",
                CommandRequest(
                    command="printf ok",
                    cwd="/workspace/a b",
                    timeout=3,
                    env_vars={"SECRET": "value; $(false)"},
                ),
            )
        ]

        output_events = [event for event in events if isinstance(event, CommandOutputEvent)]
        assert [(event.type, event.data) for event in output_events] == [
            ("stdout", "start "),
            ("stdout", "€"),
            ("stderr", "warning"),
        ]
        assert isinstance(events[-1], CommandExitEvent) and events[-1].exit_code == 7
        shell_command = remote.opened_commands[0][-1]
        assert "cd '/workspace/a b'" in shell_command
        assert "SECRET='value; $(false)'" in shell_command
        assert "timeout 3" in shell_command
        assert "SANDBOX_COMMAND_PID_FILE=/tmp/sandbox-command-" in shell_command
        assert 'printf \'%s\\n\' "$$" > "$SANDBOX_COMMAND_PID_FILE"' in shell_command
        assert command_session.closed is True

        limited_remote = MockRemoteExec()
        limited_remote.command_sessions.append(MockRemoteExecSession([(b"12345", b"", 0)]))
        limited_settings = _settings().model_copy(update={"exec_output_limit_bytes": 4})
        limited_backend = KubernetesSandboxBackend(limited_settings, api, limited_remote)
        with pytest.raises(SandboxError, match="exceeded 4 bytes"):
            await limited_backend.exec("task-1", CommandRequest(command="true"))
        assert limited_remote.sessions[0].closed is True

        cancelled_remote = MockRemoteExec()
        blocking_session = MockBlockingRemoteExecSession()
        cancelled_remote.command_sessions.append(blocking_session)
        cancelled_backend = KubernetesSandboxBackend(_settings(), api, cancelled_remote)
        stream = cancelled_backend.command("task-1", CommandRequest(command="sleep 30"))
        pending_chunk = asyncio.create_task(anext(stream))
        await blocking_session.started.wait()
        pending_chunk.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending_chunk
        assert blocking_session.closed is True
        assert cancelled_remote.terminated and cancelled_remote.terminated[0][0] == "task-1-pod"

        closed_remote = MockRemoteExec()
        closed_remote.command_sessions.append(MockRemoteExecSession([(b"started", b"", None)]))
        closed_backend = KubernetesSandboxBackend(_settings(), api, closed_remote)
        closed_stream = closed_backend.command("task-1", CommandRequest(command="sleep 30"))
        assert isinstance(await anext(closed_stream), CommandOutputEvent)
        await closed_stream.aclose()
        assert closed_remote.sessions[0].closed is True
        assert closed_remote.terminated and closed_remote.terminated[0][0] == "task-1-pod"

    async def test_streams_base64_files_across_arbitrary_boundaries(self) -> None:
        """Transfer binary files without joining request or response streams.

        Test cases:
        - Base64 helpers preserve one-byte, two-byte, whitespace, and quartet splits.
        - Upload and download quote paths containing shell metacharacters.
        """
        content = b"\x00first\xffsecond"
        encoded = [chunk async for chunk in encode_base64_chunks(_chunks([content[:1], content[1:3], content[3:]]))]
        decoded = [
            chunk
            async for chunk in decode_base64_chunks(
                _chunks(
                    [
                        base64.b64encode(content)[:2],
                        b" \n" + base64.b64encode(content)[2:5],
                        base64.b64encode(content)[5:],
                    ]
                )
            )
        ]
        assert base64.b64decode("".join(encoded)) == content
        assert b"".join(decoded) == content

        api = MockKubernetesApi()
        request = _request()
        api.jobs["task-1"] = build_job(request, _settings())
        api.pods["task-1"] = [_ready_pod("task-1")]
        remote = MockRemoteExec()
        backend = KubernetesSandboxBackend(_settings(), api, remote)
        path = "/workspace/a b;$(false).bin"

        await backend.upload_file("task-1", path, _chunks([b"f", b"ir", b"st"]))
        downloaded = [chunk async for chunk in backend.stream_download("task-1", path)]

        assert base64.b64decode(b"".join(remote.sessions[0].stdin)) == b"first"
        assert b"".join(downloaded) == b"first"
        assert all("'/workspace/a b;$(false).bin'" in command[-1] for command in remote.opened_commands)

        limited_remote = MockRemoteExec()
        limited_backend = KubernetesSandboxBackend(
            _settings().model_copy(update={"upload_limit_bytes": 4}),
            api,
            limited_remote,
        )
        with pytest.raises(SandboxError, match="Upload exceeded 4 bytes"):
            await limited_backend.upload_file("task-1", path, _chunks([b"123", b"45"]))
        assert limited_remote.sessions[0].closed is True

    async def test_uses_direct_pod_agent_when_configured(self) -> None:
        """Keep command and file bytes away from Kubernetes exec.

        Test cases:
        - Watched Pod IP state selects the direct authenticated data plane.
        - Commands, uploads, and downloads do not open Kubernetes exec sessions.
        - Backend shutdown closes the Pod HTTP pool.
        """
        api = MockKubernetesApi()
        request = _request()
        job = build_job(request, _settings())
        pod = _ready_pod("task-1")
        cache = MockResourceCache(job, pod)
        agent = MockPodAgent()
        backend = KubernetesSandboxBackend(
            _settings(),
            api,
            resource_cache=cache,
            pod_agent=agent,
        )

        events = [event async for event in backend.command("task-1", CommandRequest(command="true"))]
        await backend.upload_file("task-1", "/workspace/file.bin", _chunks([b"up", b"load"]))
        downloaded = [chunk async for chunk in backend.stream_download("task-1", "/workspace/file.bin")]
        await backend.close()

        assert isinstance(events[0], CommandOutputEvent) and events[0].data == "direct"
        assert isinstance(events[-1], CommandExitEvent) and events[-1].exit_code == 0
        assert agent.calls == [
            ("command", ("10.0.0.8", "task-1", CommandRequest(command="true"))),
            ("upload", ("10.0.0.8", "task-1", "/workspace/file.bin", b"upload")),
            ("download", ("10.0.0.8", "task-1", "/workspace/file.bin")),
        ]
        assert downloaded == [b"direct-file"]
        assert cache.operations.count("endpoint:task-1") == 3
        assert agent.closed is True

    async def test_refreshes_activity_while_command_is_quiet(self) -> None:
        """Keep a live command from being mistaken for an idle sandbox.

        Test cases:
        - Activity is refreshed while a direct command produces no output.
        - The command stays open during the refresh and completes normally afterward.
        """
        api = MockKubernetesApi()
        settings = _settings().model_copy(update={"activity_write_interval_seconds": 30})
        job = build_job(_request(), settings)
        api.jobs["task-1"] = job
        cache = MockResourceCache(job, _ready_pod("task-1"))
        agent = MockBlockingPodAgent()
        refresh_waiting = asyncio.Event()
        release_refresh = asyncio.Event()
        wait_calls = 0
        current_time = [datetime(2026, 7, 22, tzinfo=UTC)]

        async def controlled_wait(seconds: float) -> None:
            nonlocal wait_calls
            assert seconds == 30
            wait_calls += 1
            if wait_calls == 1:
                refresh_waiting.set()
                await release_refresh.wait()
                return
            await asyncio.Event().wait()

        backend = KubernetesSandboxBackend(
            settings,
            api,
            resource_cache=cache,
            pod_agent=agent,
            wait=controlled_wait,
            now=lambda: current_time[0],
        )
        stream = backend.command("task-1", CommandRequest(command="sleep 120"))
        event_task = asyncio.create_task(anext(stream))
        await agent.started.wait()
        await asyncio.wait_for(refresh_waiting.wait(), timeout=0.1)

        current_time[0] += timedelta(seconds=31)
        release_refresh.set()
        await asyncio.wait_for(api.activity_refreshed.wait(), timeout=0.1)

        assert not event_task.done()
        assert api.activity_patch_count == 2

        agent.release.set()
        event = await event_task
        await stream.aclose()

        assert isinstance(event, CommandExitEvent) and event.exit_code == 0


class TestKubernetesSandboxBackendEgressAndCleanup:
    """Temporary egress and idle sandbox cleanup behavior."""

    async def test_replaces_egress_touches_activity_and_clears_to_unrestricted(self) -> None:
        """Keep temporary allowlists separate from unrestricted baseline egress.

        Test cases:
        - Mixed domains and CIDRs replace one Cilium policy.
        - Activity writes inside the debounce interval are coalesced.
        - Activity after the debounce interval persists a new timestamp.
        - Clear deletes only the egress policy.
        """
        api = MockKubernetesApi()
        request = _request()
        api.jobs["task-1"] = build_job(request, _settings())
        current_time = [datetime(2026, 7, 21, 12, tzinfo=UTC)]
        egress = CiliumEgressPolicyDriver(_settings(), api)
        backend = KubernetesSandboxBackend(_settings(), api, egress_driver=egress, now=lambda: current_time[0])

        await backend.modify_egress_rules("task-1", ["api.example.com", "10.0.0.3/24"])
        await backend.clear_egress_rules("task-1")
        current_time[0] += timedelta(seconds=31)
        await backend.modify_egress_rules("task-1", ["api.example.com"])

        replace_index = next(index for index, item in enumerate(api.operations) if item[0] == "replace_custom")
        first_patch_index = next(index for index, item in enumerate(api.operations) if item[0] == "patch_job")
        assert replace_index < first_patch_index
        replace = cast(tuple[str, str, str, dict[str, object]], api.operations[replace_index][1])
        body = replace[3]
        rules = cast(dict[str, Any], body["spec"])["egress"]
        assert rules[1]["toCIDR"] == ["10.0.0.0/24"]
        assert rules[2]["toFQDNs"] == [{"matchName": "api.example.com"}]
        assert ("delete_custom", ("benchmark-sandboxes", "ciliumnetworkpolicies", "task-1-egress")) in api.operations
        assert sum(name == "patch_job" for name, _ in api.operations) == 2

    async def test_deletes_only_expired_idle_sandboxes_idempotently(self) -> None:
        """Use activity annotations without deleting retained or malformed Jobs.

        Test cases:
        - Expired positive intervals are deleted through the ordinary delete path.
        - Zero intervals and malformed timestamps are retained.
        - A second janitor pass is safe and finds no duplicate work.
        """
        api = MockKubernetesApi()
        settings = _settings()
        now = datetime(2026, 7, 21, 12, tzinfo=UTC)
        expired = _request(name="expired", auto_stop_interval=1)
        debounce_grace = _request(name="debounce-grace", auto_stop_interval=1)
        retained = _request(name="retained", auto_stop_interval=0)
        malformed = _request(name="malformed", auto_stop_interval=1)
        api.jobs["expired"] = build_job(expired, settings, now=now - timedelta(minutes=2))
        api.jobs["debounce-grace"] = build_job(debounce_grace, settings, now=now - timedelta(seconds=75))
        api.jobs["retained"] = build_job(retained, settings, now=now - timedelta(days=1))
        api.jobs["malformed"] = build_job(malformed, settings, now=now - timedelta(days=1))
        _metadata(api.jobs["malformed"])["annotations"]["sandbox.vals.ai/last-activity"] = "not-a-date"
        backend = KubernetesSandboxBackend(settings, api)

        assert await backend.delete_idle_sandboxes(now) == 1
        assert await backend.delete_idle_sandboxes(now) == 0
        assert set(api.jobs) == {"debounce-grace", "retained", "malformed"}
