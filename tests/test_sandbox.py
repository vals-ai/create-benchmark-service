import asyncio
import shlex
from collections.abc import AsyncGenerator, Mapping
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, cast

import pytest
from aiohttp import ClientConnectionError, ClientResponseError, InvalidURL, RequestInfo
from daytona import GpuType, SandboxState
from daytona_api_client_async import (
    ApiException,
    SandboxClass,
)
from daytona.common.errors import (
    DaytonaConflictError,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaRateLimitError,
)
from daytona.common.pty import PtyResult
from multidict import CIMultiDict, CIMultiDictProxy
from pydantic import TypeAdapter, ValidationError
from yarl import URL

import benchmark_service.sandbox.daytona as daytona_module
from benchmark_service.sandbox import (
    ComposeSource,
    ComposeSandbox,
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    Sandbox,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
)
from benchmark_service.sandbox.daytona import (
    _PTY_STDOUT_TAIL_MAX_BYTES,  # pyright: ignore[reportPrivateUsage]
    DaytonaProviderConfig,
    _is_transient_daytona_error,  # pyright: ignore[reportPrivateUsage]
    DaytonaSandbox,
    DaytonaSandboxProvider,
    daytona_retry_after_seconds,
)


def _client_response_error(status: int, message: str) -> ClientResponseError:
    url = URL("https://daytona.example.test")
    headers: CIMultiDict[str] = CIMultiDict()
    request_info = RequestInfo(url=url, method="GET", headers=CIMultiDictProxy(headers), real_url=url)
    return ClientResponseError(request_info, (), status=status, message=message)


def _unwrap_shell_command(command: str) -> str:
    if command.startswith("sh -c "):
        return shlex.split(command)[2]
    return command


class Process:
    def __init__(self) -> None:
        self.command: str | None = None
        self.pty_envs: dict[str, str] | None = None
        self.pty_handle: PtyHandle | None = None

    async def exec(self, command: str) -> SimpleNamespace:
        self.command = command
        evaluated_command = _unwrap_shell_command(command)
        if evaluated_command.startswith("test -e "):
            return SimpleNamespace(exit_code=0, result="")
        if evaluated_command.startswith("cat "):
            return SimpleNamespace(exit_code=0, result="0")
        return SimpleNamespace(exit_code=0, result="")

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
    ) -> "PtyHandle":
        assert id
        self.pty_envs = envs
        self.pty_handle = PtyHandle(on_data)
        return self.pty_handle

    async def kill_pty_session(self, session_id: str) -> None:
        assert session_id


class RateLimitedProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        if self.attempts == 1:
            raise DaytonaRateLimitError("rate limited", headers={"retry-after-sandbox-create": "0"})
        return await super().exec(command)


class FailedExecuteCommandProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        if self.attempts == 1:
            raise DaytonaError("Failed to execute command:")
        return await super().exec(command)


def _daytona_error_with_cause(message: str, cause: BaseException) -> DaytonaError:
    try:
        raise DaytonaError(message) from cause
    except DaytonaError as exc:
        return exc


class WrappedConnectionErrorProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        if self.attempts == 1:
            raise _daytona_error_with_cause(
                "Failed to execute command: toolbox request failed",
                ClientConnectionError("tcp reset"),
            )
        return await super().exec(command)


class MisclassifiedTransportProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        if self.attempts == 1:
            raise DaytonaError(
                "Failed to execute command: File descriptor 127 is used by transport "
                "<TCPTransport closed=False reading=True 0xabc>"
            )
        return await super().exec(command)


class DetailedFailedExecuteCommandProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        raise DaytonaError("Failed to execute command: permission denied")


class RemovedSandboxProcess(Process):
    async def exec(self, command: str) -> SimpleNamespace:
        raise DaytonaNotFoundError("sandbox not found")


class PtyHandle:
    def __init__(self, on_data: Callable[[bytes], None | Awaitable[None]]) -> None:
        self._on_data = on_data
        self.inputs: list[str] = []

    async def send_input(self, data: str) -> None:
        self.inputs.append(data)
        if data.startswith("stty"):
            return
        result = self._on_data(b"hello")
        if result is not None:
            await result

    async def wait(self) -> PtyResult | None:
        pass

    async def disconnect(self) -> None:
        pass


class DisconnectedPtyHandle(PtyHandle):
    async def wait(self) -> None:
        raise RuntimeError("disconnected")


class ReconnectingProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.checked_session_id: str | None = None
        self.connected_session_id: str | None = None

    async def exec(self, command: str) -> SimpleNamespace:
        if _unwrap_shell_command(command).startswith("test -e "):
            return SimpleNamespace(exit_code=0 if self.connected_session_id else 1, result="")
        return await super().exec(command)

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
    ) -> PtyHandle:
        assert id
        assert envs == {"TERM": "dumb", "LANG": "C.UTF-8"}
        return DisconnectedPtyHandle(on_data)

    async def get_pty_session_info(self, session_id: str) -> SimpleNamespace:
        assert self.connected_session_id is None
        self.checked_session_id = session_id
        return SimpleNamespace()

    async def connect_pty_session(
        self,
        session_id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
    ) -> PtyHandle:
        assert self.checked_session_id == session_id
        self.connected_session_id = session_id
        return PtyHandle(on_data)


class FinishedPtyHandle(PtyHandle):
    def __init__(self, on_data: Callable[[bytes], None | Awaitable[None]], result: PtyResult) -> None:
        super().__init__(on_data)
        self._result = result

    async def wait(self) -> PtyResult:
        return self._result


class LostPtyProcess(ReconnectingProcess):
    def __init__(self, result: PtyResult, reconnect_error: DaytonaError) -> None:
        super().__init__()
        self._result = result
        self._reconnect_error = reconnect_error

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
    ) -> FinishedPtyHandle:
        assert id
        assert envs == {"TERM": "dumb", "LANG": "C.UTF-8"}
        return FinishedPtyHandle(on_data, self._result)

    async def get_pty_session_info(self, session_id: str) -> SimpleNamespace:
        self.checked_session_id = session_id
        raise self._reconnect_error


class CreatePtyFailureProcess(Process):
    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
    ) -> PtyHandle:
        raise DaytonaConnectionError("toolbox unreachable")


class CrashingReconnectProcess(ReconnectingProcess):
    def __init__(self, sandbox: "InnerSandbox") -> None:
        super().__init__()
        self._sandbox = sandbox

    async def get_pty_session_info(self, session_id: str) -> SimpleNamespace:
        self._sandbox.state = SandboxState.DESTROYED
        raise DaytonaConnectionError("toolbox unreachable")


class FloodingPtyHandle(PtyHandle):
    async def send_input(self, data: str) -> None:
        self.inputs.append(data)
        if data.startswith("stty"):
            return
        for index in range(200):
            result = self._on_data(f"chunk-{index:04d}-".encode() + b"x" * 1024)
            if result is not None:
                await result


class FloodingProcess(Process):
    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
    ) -> PtyHandle:
        assert id
        self.pty_envs = envs
        self.pty_handle = FloodingPtyHandle(on_data)
        return self.pty_handle


class BlockingPtyHandle(PtyHandle):
    disconnected = False

    async def wait(self) -> None:
        await asyncio.Event().wait()

    async def disconnect(self) -> None:
        self.disconnected = True


class BlockingProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.handle: BlockingPtyHandle | None = None
        self.killed_session_id: str | None = None

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
    ) -> BlockingPtyHandle:
        assert id
        assert envs == {"TERM": "dumb", "LANG": "C.UTF-8"}
        self.handle = BlockingPtyHandle(on_data)
        return self.handle

    async def kill_pty_session(self, session_id: str) -> None:
        self.killed_session_id = session_id


class Files:
    async def upload_file(self, content: bytes, remote_path: str) -> None:
        assert content == b"hello world"
        assert remote_path == "/tmp/result.txt"

    async def download_file_stream(self, remote_path: str) -> Any:
        assert remote_path == "/tmp/result.txt"

        async def chunks() -> Any:
            yield b"hello"
            yield b" world"

        return chunks()


class RemovedSandboxFiles(Files):
    async def upload_file(self, content: bytes, remote_path: str) -> None:
        raise _client_response_error(status=404, message="Not Found")

    async def download_file_stream(self, remote_path: str) -> Any:
        async def chunks() -> Any:
            raise _client_response_error(status=502, message="Bad Gateway")
            yield b""

        return chunks()


class RecordingSandbox(Sandbox):
    def __init__(self, exec_results: list[ExecResult] | None = None) -> None:
        self.exec_commands: list[str] = []
        self.command_env_vars: list[dict[str, str]] = []
        self.uploads: list[tuple[str, bytes]] = []
        self.downloads: list[str] = []
        self.allowed_addresses: list[str] | None = None
        self.egress_cleared = False
        self.exec_results = exec_results or []

    @property
    def id(self) -> str:
        return "outer-id"

    @property
    def name(self) -> str:
        return "outer-name"

    @property
    def state(self) -> str:
        return "started"

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.exec_commands.append(command)
        if self.exec_results:
            return self.exec_results.pop(0)
        return ExecResult(exit_code=0, output="ok")

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        self.exec_commands.append(command)
        self.command_env_vars.append(dict(env_vars or {}))
        yield "ok"

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.uploads.append((remote_path, content))

    async def download_file(self, remote_path: str) -> bytes:
        self.downloads.append(remote_path)
        return b"downloaded"

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        self.allowed_addresses = allowed_addresses

    async def clear_egress_rules(self) -> None:
        self.egress_cleared = True


class InnerSandbox:
    id = "sandbox-id"
    name = "sandbox-name"
    state = SandboxState.STARTED

    def __init__(self) -> None:
        self.process = Process()
        self.fs = Files()
        self.autostop_interval: int | None = None
        self.network_settings: list[dict[str, str | bool | None]] = []
        self.refresh_count = 0

    async def set_autostop_interval(self, interval: int) -> None:
        self.autostop_interval = interval

    async def update_network_settings(
        self,
        *,
        network_block_all: bool | None = None,
        network_allow_list: str | None = None,
        domain_allow_list: str | None = None,
    ) -> None:
        self.network_settings.append(
            {
                "network_block_all": network_block_all,
                "network_allow_list": network_allow_list,
                "domain_allow_list": domain_allow_list,
            }
        )

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        assert timeout == 0

    async def refresh_data(self) -> None:
        self.refresh_count += 1


class RemovedInnerSandbox(InnerSandbox):
    async def refresh_data(self) -> None:
        raise DaytonaNotFoundError("sandbox not found")


class DeleteConflictSandbox(InnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.autostop_attempts = 0

    async def set_autostop_interval(self, interval: int) -> None:
        self.autostop_attempts += 1
        if self.autostop_attempts == 1:
            raise DaytonaConflictError("Failed to set auto-stop interval: Sandbox was modified by another operation")
        await super().set_autostop_interval(interval)


class ErrorStateSandbox(InnerSandbox):
    state = SandboxState.ERROR

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        raise DaytonaError("sandbox failed to start")


class RecreatedInnerSandbox(InnerSandbox):
    id = "recreated-sandbox-id"


class InactiveSandbox(InnerSandbox):
    def __init__(self, state: SandboxState) -> None:
        super().__init__()
        self.state = state

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        raise AssertionError("inactive sandbox deletion must not wait for startup")

    async def refresh_data(self) -> None:
        raise AssertionError("inactive sandbox deletion must not refresh before deletion")

    async def set_autostop_interval(self, interval: int) -> None:
        raise AssertionError("inactive sandbox deletion must not update autostop")


class BuildingSandbox(InnerSandbox):
    state = SandboxState.BUILDING_SNAPSHOT

    def __init__(self) -> None:
        super().__init__()
        self.waited_for_start = False

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        assert timeout == 0
        self.waited_for_start = True
        self.state = SandboxState.STARTED


class RefreshToErrorSandbox(InnerSandbox):
    async def refresh_data(self) -> None:
        await super().refresh_data()
        self.state = SandboxState.ERROR


class BareHtml502RefreshSandbox(InnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.refresh_attempts = 0

    async def refresh_data(self) -> None:
        self.refresh_attempts += 1
        if self.refresh_attempts == 1:
            raise DaytonaError("Failed to refresh sandbox data: <html><h1>502 Bad Gateway</h1></html>")
        await super().refresh_data()


class UnexpectedRefreshSandbox(InnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.refresh_attempts = 0

    async def refresh_data(self) -> None:
        self.refresh_attempts += 1
        if self.refresh_attempts < 6:
            raise DaytonaError("Failed to refresh sandbox data: An unexpected error occurred.")
        await super().refresh_data()


class DaytonaClient:
    def __init__(self, sandbox: InnerSandbox) -> None:
        self.sandbox = sandbox
        self.created = False
        self.closed = False
        self.deleted = False
        self.listed_query: Any | None = None

    async def get(self, instance_id: str) -> InnerSandbox:
        assert instance_id == self.sandbox.name
        return self.sandbox

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        self.created = True
        return self.sandbox

    async def delete(self, sandbox: InnerSandbox) -> None:
        assert sandbox is self.sandbox
        self.deleted = True

    async def close(self) -> None:
        self.closed = True

    def list(self, query: object) -> Any:
        self.listed_query = query

        async def sandboxes() -> Any:
            yield self.sandbox

        return sandboxes()


class CreateFailureDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.create_attempts = 0

    async def get(self, instance_id: str) -> InnerSandbox:
        raise DaytonaNotFoundError("sandbox not found")

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        self.create_attempts += 1
        raise DaytonaError("OCI runtime create failed: invalid mount configuration")


class CreateNotFoundDaytonaClient(CreateFailureDaytonaClient):
    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        raise DaytonaError("sandbox not found", status_code=404)


class SysboxRunnerFaultDaytonaClient(DaytonaClient):
    def __init__(self) -> None:
        self.failed_sandbox = ErrorStateSandbox()
        super().__init__(self.failed_sandbox)
        self.sandbox_exists = False
        self.create_attempts = 0
        self.delete_attempts = 0
        self.deleted_sandboxes: list[InnerSandbox] = []

    async def get(self, instance_id: str) -> InnerSandbox:
        assert instance_id in (self.sandbox.id, self.sandbox.name)
        if not self.sandbox_exists:
            raise DaytonaNotFoundError("sandbox not found")
        return self.sandbox

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        self.create_attempts += 1
        if self.create_attempts == 1:
            self.sandbox_exists = True
            raise DaytonaError(
                "failed to create sandbox: OCI runtime create failed: failed to register with sysbox-mgr: unavailable"
            )
        assert not self.sandbox_exists
        self.sandbox = RecreatedInnerSandbox()
        self.sandbox_exists = True
        return self.sandbox

    async def delete(self, sandbox: InnerSandbox) -> None:
        assert sandbox is self.sandbox
        self.delete_attempts += 1
        if self.delete_attempts == 1:
            raise DaytonaConflictError("Sandbox was modified by another operation")
        self.deleted_sandboxes.append(sandbox)
        self.deleted = True
        self.sandbox_exists = False


class ConflictThenFlakyLookupDaytonaClient(DaytonaClient):
    """create always name-conflicts; the recovery lookup fails transiently once."""
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.get_attempts = 0

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        self.created = True
        raise DaytonaError(f"Sandbox with name {self.sandbox.name} already exists")

    async def get(self, instance_id: str) -> InnerSandbox:
        self.get_attempts += 1
        if self.get_attempts == 1:
            raise _daytona_error_with_cause(
                "Failed to get sandbox",
                ClientConnectionError("tcp reset"),
            )
        return await super().get(instance_id)


class UnexpectedGetDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.get_attempts = 0

    async def get(self, instance_id: str) -> InnerSandbox:
        self.get_attempts += 1
        if self.get_attempts == 1:
            raise DaytonaError("Failed to get sandbox: An unexpected error occurred.")
        return await super().get(instance_id)


class UnexpectedRemoveDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.delete_attempts = 0

    async def delete(self, sandbox: InnerSandbox) -> None:
        self.delete_attempts += 1
        if self.delete_attempts == 1:
            raise DaytonaError(
                "Failed to remove sandbox: Failed to refresh sandbox data: An unexpected error occurred."
            )
        await super().delete(sandbox)


def _provider(daytona: DaytonaClient) -> DaytonaSandboxProvider:
    provider = DaytonaSandboxProvider.__new__(DaytonaSandboxProvider)
    provider._daytona = cast(Any, daytona)  # pyright: ignore[reportPrivateUsage]
    return provider


def _request(
    name: str,
    resources: Resources | None = None,
    source: SandboxSource | None = None,
) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=source or ImageSource(image="python:3.12"),
        resources=resources or Resources(vcpu=2, memory=4, disk=10),
        name=name,
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )


def _usage_row(**updates: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "region_id": "us", "sandbox_class": SandboxClass.CONTAINER,
        "total_cpu_quota": 8, "current_cpu_usage": 2,
        "total_memory_quota": 32, "current_memory_usage": 4,
        "total_disk_quota": 100, "current_disk_usage": 25,
        "max_cpu_per_sandbox": None, "max_memory_per_sandbox": None, "max_disk_per_sandbox": None,
    }
    return SimpleNamespace(**(values | updates))


def _configured_daytona_provider(**updates: object) -> DaytonaSandboxProvider:
    values = {
        "DAYTONA_API_KEY": "secret", "DAYTONA_API_URL": "https://daytona.example/api",
        "DAYTONA_TARGET": "us", "DAYTONA_ORGANIZATION_ID": "org-1",
    }
    return DaytonaSandboxProvider(DaytonaProviderConfig.model_validate(values | updates))


def _admission_provider(
    monkeypatch: pytest.MonkeyPatch,
    result: Exception | SimpleNamespace,
    organization_id: str | None = "org-1",
    organization_limits: tuple[int, int, int] | Exception = (8, 32, 100),
) -> tuple[DaytonaSandboxProvider, list[str]]:
    requested: list[str] = []

    async def observe(organization: str) -> SimpleNamespace:
        requested.append(organization)
        if isinstance(result, Exception):
            raise result
        return result

    async def get_organization(_organization: str) -> SimpleNamespace:
        if isinstance(organization_limits, Exception):
            raise organization_limits
        return SimpleNamespace(
            max_cpu_per_sandbox=organization_limits[0],
            max_memory_per_sandbox=organization_limits[1],
            max_disk_per_sandbox=organization_limits[2],
        )

    def api_client(_value: object) -> nullcontext[None]:
        return nullcontext()

    def organizations_api(_value: object) -> SimpleNamespace:
        return SimpleNamespace(
            get_organization=get_organization,
            get_organization_usage_overview=observe,
        )

    monkeypatch.setattr("benchmark_service.sandbox.daytona.ApiClient", api_client)
    monkeypatch.setattr("benchmark_service.sandbox.daytona.OrganizationsApi", organizations_api)
    return _configured_daytona_provider(DAYTONA_ORGANIZATION_ID=organization_id), requested


def test_daytona_pool_id_is_stable_nonsecret_and_configuration_scoped() -> None:
    first = _configured_daytona_provider(DAYTONA_API_KEY="first-secret", DAYTONA_API_URL="https://d.test/")
    rotated = _configured_daytona_provider(DAYTONA_API_KEY="rotated-secret", DAYTONA_API_URL="https://d.test")
    pool_id = first.admission_pool_id

    assert pool_id is not None
    assert pool_id == rotated.admission_pool_id
    assert len(
        {
            pool_id,
            _configured_daytona_provider(DAYTONA_TARGET="eu").admission_pool_id,
            _configured_daytona_provider(DAYTONA_ORGANIZATION_ID="org-2").admission_pool_id,
            _configured_daytona_provider(DAYTONA_API_URL="https://other.example/api").admission_pool_id,
        }
    ) == 4
    assert _configured_daytona_provider(DAYTONA_ORGANIZATION_ID=None).admission_pool_id is None
    assert all(value not in pool_id for value in ("first-secret", "rotated-secret", "d.test"))


_ADMISSION_IMAGE = ImageSource(image="img")
_ADMISSION_RESOURCES = Resources(vcpu=2, memory=4, disk=10)


@pytest.mark.parametrize(
    ("organization_id", "source", "resources", "usage_updates", "expected", "observed"),
    [
        (None, _ADMISSION_IMAGE, _ADMISSION_RESOURCES, {}, True, False),
        ("org-1", SnapshotSource(snapshot="snap"), _ADMISSION_RESOURCES, {}, True, False),
        ("org-1", _ADMISSION_IMAGE, Resources(vcpu=2, memory=4, disk=10, gpu=1), {}, True, False),
        ("org-1", _ADMISSION_IMAGE, Resources(vcpu=0, memory=1, disk=1), {}, SandboxError, False),
        ("org-1", _ADMISSION_IMAGE, Resources(vcpu=1, memory=1, disk=-1), {}, SandboxError, False),
        ("org-1", _ADMISSION_IMAGE, _ADMISSION_RESOURCES, {}, True, True),
        ("org-1", ComposeSource(outer=_ADMISSION_IMAGE), Resources(vcpu=7, memory=4, disk=10), {}, False, True),
        ("org-1", _ADMISSION_IMAGE, _ADMISSION_RESOURCES, {"current_memory_usage": 29}, False, True),
        ("org-1", _ADMISSION_IMAGE, Resources(vcpu=9, memory=4, disk=10), {}, SandboxError, True),
        ("org-1", _ADMISSION_IMAGE, _ADMISSION_RESOURCES, {"max_disk_per_sandbox": 9}, SandboxError, True),
    ],
)
async def test_daytona_admission_decisions(
    monkeypatch: pytest.MonkeyPatch,
    organization_id: str | None,
    source: ImageSource | SnapshotSource | ComposeSource,
    resources: Resources,
    usage_updates: dict[str, object],
    expected: bool | type[SandboxError],
    observed: bool,
) -> None:
    overview = SimpleNamespace(
        region_usage=[
            _usage_row(region_id="eu", total_cpu_quota=0),
            _usage_row(sandbox_class=SandboxClass.LINUX_VM, total_cpu_quota=0),
            _usage_row(**usage_updates),
        ]
    )
    provider, requested = _admission_provider(
        monkeypatch, overview, organization_id=organization_id
    )

    if expected is SandboxError:
        with pytest.raises(SandboxError):
            await provider.check_admission(source, resources)
    else:
        assert await provider.check_admission(source, resources) is expected
    assert requested == (["org-1"] if observed else [])


async def test_daytona_admission_uses_organization_limits_only_for_null_region_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, _ = _admission_provider(
        monkeypatch,
        SimpleNamespace(region_usage=[_usage_row()]),
        organization_limits=(1, 32, 100),
    )

    with pytest.raises(SandboxError):
        await provider.check_admission(_ADMISSION_IMAGE, _ADMISSION_RESOURCES)

    provider, _ = _admission_provider(
        monkeypatch,
        SimpleNamespace(
            region_usage=[_usage_row(max_cpu_per_sandbox=8, max_memory_per_sandbox=32, max_disk_per_sandbox=100)]
        ),
        organization_limits=AssertionError("organization limits should not be fetched"),
    )
    assert await provider.check_admission(_ADMISSION_IMAGE, _ADMISSION_RESOURCES)


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (TimeoutError(), True),
        (ClientConnectionError("connection reset"), True),
        (InvalidURL("not-a-url"), SandboxError),
        (ApiException(status=429), True),
        (ApiException(status=503), True),
        (ApiException(status=401), SandboxError),
        (SimpleNamespace(region_usage=[_usage_row(), _usage_row()]), SandboxError),
    ],
)
async def test_daytona_admission_observation_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    result: Exception | SimpleNamespace,
    expected: bool | type[SandboxError],
) -> None:
    provider, requested = _admission_provider(monkeypatch, result)
    decision = provider.check_admission(_ADMISSION_IMAGE, _ADMISSION_RESOURCES)

    if expected is SandboxError:
        with pytest.raises(SandboxError):
            await decision
    else:
        assert await decision is expected
    assert requested == ["org-1"]


async def test_compose_sandbox_routes_operations_through_main_service() -> None:
    """Compose sandboxes should proxy sandbox operations through the Harbor main service.

    Test cases:
    - ComposeSource stores the DinD outer source and service name.
    - exec, command, upload, and download route through docker compose service `main`.
    """
    source = ComposeSource(
        outer=ImageSource(image="docker:28.3.3-dind"),
        compose_command="MAIN_IMAGE_NAME=task docker compose -p task -f /harbor/compose.yaml",
    )
    outer = RecordingSandbox()
    sandbox = ComposeSandbox(outer, source)

    exec_result = await sandbox.exec("pytest -q", cwd="/workspace", timeout=12)
    secret = "value with spaces; $(touch /tmp/leaked)"
    output = [
        chunk
        async for chunk in sandbox.command(
            "echo ok",
            cwd="/workspace",
            timeout=12,
            env_vars={"AGENT_SECRET": secret},
        )
    ]
    await sandbox.upload_file("/workspace/instruction.md", b"solve it")
    downloaded = await sandbox.download_file("/workspace/reward.json")
    await sandbox.modify_egress_rules(["api.openai.com"])
    await sandbox.clear_egress_rules()

    assert source.service == "main"
    assert isinstance(source.outer, ImageSource)
    assert source.outer.image == "docker:28.3.3-dind"
    assert sandbox.id == "outer-id"
    assert sandbox.name == "outer-name"
    assert sandbox.state == "started"
    assert exec_result.output == "ok"
    assert output == ["ok"]
    assert outer.exec_commands[0] == (
        "MAIN_IMAGE_NAME=task docker compose -p task -f /harbor/compose.yaml "
        "exec "
        "$(env | sed -n 's/^\\([A-Za-z_][A-Za-z0-9_]*\\)=.*/-e \\1/p') "
        "-T -w /workspace main sh -lc 'pytest -q'"
    )
    assert outer.exec_commands[1] == (
        "MAIN_IMAGE_NAME=task docker compose -p task -f /harbor/compose.yaml "
        "exec "
        "$(env | sed -n 's/^\\([A-Za-z_][A-Za-z0-9_]*\\)=.*/-e \\1/p') "
        "-T -w /workspace main sh -lc 'echo ok'"
    )
    assert outer.command_env_vars == [{"AGENT_SECRET": secret}]
    assert secret not in outer.exec_commands[1]
    assert outer.allowed_addresses == ["api.openai.com"]
    assert outer.egress_cleared
    upload_temp = outer.uploads[0][0]
    download_temp = outer.downloads[0]
    assert outer.uploads == [(upload_temp, b"solve it")]
    assert upload_temp.startswith("/var/tmp/compose-upload-")
    assert download_temp.startswith("/var/tmp/compose-download-")
    assert (
        "container_id=$(MAIN_IMAGE_NAME=task docker compose -p task -f /harbor/compose.yaml ps -q main); "
        "docker exec \"$container_id\" sh -lc 'mkdir -p /workspace'; "
        f"cat {upload_temp} | docker exec -i \"$container_id\" sh -lc 'cat > /workspace/instruction.md'"
    ) in outer.exec_commands
    assert (
        "container_id=$(MAIN_IMAGE_NAME=task docker compose -p task -f /harbor/compose.yaml ps -q main); "
        f"docker exec \"$container_id\" sh -lc 'cat /workspace/reward.json' > {download_temp}"
    ) in outer.exec_commands
    assert downloaded == b"downloaded"


async def test_compose_stream_download_streams_from_outer_and_cleans_temp() -> None:
    outer = RecordingSandbox()
    source = ComposeSource(outer=ImageSource(image="img"), service="main")
    sandbox = ComposeSandbox(outer, source)

    chunks = [chunk async for chunk in sandbox.stream_download("/workspace/reward.json")]

    assert chunks == [b"downloaded"]
    download_temp = outer.downloads[0]
    assert download_temp.startswith("/var/tmp/compose-download-")
    assert outer.exec_commands[-1] == f"rm -f {download_temp}"


async def test_compose_command_rejects_invalid_environment_names_before_outer_call() -> None:
    outer = RecordingSandbox()
    sandbox = ComposeSandbox(
        outer,
        ComposeSource(outer=ImageSource(image="docker:28.3.3-dind")),
    )

    with pytest.raises(ValueError, match="BAD-NAME"):
        _ = [chunk async for chunk in sandbox.command("true", env_vars={"BAD-NAME": "secret"})]

    assert outer.exec_commands == []


async def test_compose_sandbox_cleans_temp_files_when_copy_operations_fail() -> None:
    """Compose copy failures should raise while still deleting temporary outer files.

    Test cases:
    - Upload failure raises SandboxError and runs temp-file cleanup.
    - Download failure raises SandboxError and runs temp-file cleanup.
    """
    source = ComposeSource(
        outer=ImageSource(image="docker:28.3.3-dind"),
        compose_command="docker compose -f /harbor/compose.yaml",
    )
    upload_outer = RecordingSandbox([ExecResult(exit_code=1, output="upload failed")])
    download_outer = RecordingSandbox([ExecResult(exit_code=1, output="download failed")])

    with pytest.raises(SandboxError, match="compose upload failed: upload failed"):
        await ComposeSandbox(upload_outer, source).upload_file("/workspace/instruction.md", b"solve it")

    with pytest.raises(SandboxError, match="compose download failed: download failed"):
        await ComposeSandbox(download_outer, source).download_file("/workspace/reward.json")

    assert upload_outer.exec_commands[-1].startswith("rm -f /var/tmp/compose-upload-")
    assert download_outer.exec_commands[-1].startswith("rm -f /var/tmp/compose-download-")


async def test_daytona_provider_rejects_compose_source_before_create() -> None:
    """Daytona provider should only receive the outer source for compose-backed tasks.

    Test cases:
    - A ComposeSource passed directly to provider creation raises SandboxError.
    """
    provider = _provider(CreateFailureDaytonaClient(InnerSandbox()))
    request = SandboxCreateRequest(
        source=ComposeSource(
            outer=ImageSource(image="docker:28.3.3-dind"),
            compose_command="docker compose -f /harbor/compose.yaml",
        ),
        resources=Resources(vcpu=2, memory=4, disk=10),
        name="compose-task",
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )

    with pytest.raises(SandboxError, match="ComposeSource must be unwrapped"):
        await provider.create_sandbox(request)


async def test_daytona_command_applies_timeout_inside_cwd() -> None:
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", cwd="/workspace", timeout=60)

    assert inner.process.command == "cd /workspace && timeout 60 sh -c pytest"


async def test_daytona_command_preserves_fractional_timeout() -> None:
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", timeout=0.5)

    assert inner.process.command == "timeout 0.5 sh -c pytest"


async def test_daytona_command_wraps_shell_pipelines_when_timeout_is_set() -> None:
    """Timeout should preserve shell syntax instead of treating assignments as executables.

    Test cases:
    - A shell assignment and pipeline stay inside one shell command when timeout is set.
    """
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("container_id=$(docker compose ps -q main); cat /tmp/file | docker exec -i \"$container_id\" cat", timeout=60)

    assert inner.process.command == (
        "timeout 60 sh -c "
        "'container_id=$(docker compose ps -q main); cat /tmp/file | docker exec -i \"$container_id\" cat'"
    )


def test_daytona_retry_after_uses_specific_header_first() -> None:
    exc = DaytonaRateLimitError(
        "rate limited",
        headers={"retry-after": "10", "retry-after-sandbox-create": "3"},
    )

    assert daytona_retry_after_seconds(exc) == 3


def test_daytona_retry_after_uses_any_retry_after_header() -> None:
    exc = DaytonaRateLimitError("rate limited", headers={"retry-after-custom": "5"})

    assert daytona_retry_after_seconds(exc) == 5


@pytest.mark.parametrize(
    "message",
    [
        "Failed to get sandbox: An unexpected error occurred.",
        "Failed to refresh sandbox data: An unexpected error occurred.",
        "Failed to remove sandbox: Failed to refresh sandbox data: An unexpected error occurred.",
        "Failed to list sandboxes: An unexpected error occurred.",
    ],
)
def test_daytona_unexpected_provider_errors_are_transient(message: str) -> None:
    assert _is_transient_daytona_error(DaytonaError(message))


@pytest.mark.parametrize(
    "message",
    [
        "Failed to create sandbox: Temporary authentication service error",
        "Failed to get sandbox: Temporary authentication service error",
        "Failed to refresh sandbox data: Temporary authentication service error",
        "Failed to remove sandbox: Failed to refresh sandbox data: Temporary authentication service error",
        "Failed to set auto-stop interval: Temporary authentication service error",
    ],
)
def test_daytona_temporary_authentication_errors_are_transient(message: str) -> None:
    assert _is_transient_daytona_error(DaytonaError(message))


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
def test_daytona_retryable_http_statuses_are_transient(status_code: int) -> None:
    assert _is_transient_daytona_error(DaytonaError("provider request failed", status_code=status_code))


def test_daytona_retryable_client_response_status_is_transient() -> None:
    assert _is_transient_daytona_error(_client_response_error(503, "service unavailable"))


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 409])
def test_daytona_permanent_http_statuses_are_not_transient(status_code: int) -> None:
    assert not _is_transient_daytona_error(DaytonaError("provider request failed", status_code=status_code))


@pytest.mark.parametrize(
    "message",
    [
        "Failed to create sandbox: Snapshot example not found.",
        "Failed to create sandbox: SandboxState.BUILD_FAILED: unexpected status from image registry: 403 Forbidden",
    ],
)
def test_daytona_permanent_provider_errors_are_not_transient(message: str) -> None:
    assert not _is_transient_daytona_error(DaytonaError(message))


async def test_daytona_unexpected_refresh_uses_staged_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = UnexpectedRefreshSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))
    observed_waits: list[float] = []

    async def record_wait(seconds: float) -> None:
        observed_waits.append(seconds)

    retryer = cast(Any, DaytonaSandbox._check_sandbox_alive).retry  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(retryer, "sleep", record_wait)

    await sandbox._check_sandbox_alive()  # pyright: ignore[reportPrivateUsage]

    assert inner.refresh_attempts == 6
    assert len(observed_waits) == 5
    for observed, expected in zip(observed_waits, (5, 25, 90, 300, 420), strict=True):
        assert expected * 0.9 <= observed <= expected


async def test_daytona_exec_retries_rate_limits() -> None:
    inner = InnerSandbox()
    process = RateLimitedProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest")

    assert process.attempts == 2


async def test_daytona_exec_retries_failed_execute_command_errors() -> None:
    """Blank Daytona exec failures should be retried because they are usually transient.

    Test cases:
    - A generic DaytonaError with "Failed to execute command:" is retried.
    - The retry returns the successful exec result instead of surfacing SandboxError.
    """
    inner = InnerSandbox()
    process = FailedExecuteCommandProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    result = await sandbox.exec("pytest")

    assert result.exit_code == 0
    assert process.attempts == 2


async def test_daytona_exec_retries_wrapped_connection_errors() -> None:
    inner = InnerSandbox()
    process = WrappedConnectionErrorProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    result = await sandbox.exec("pytest")

    assert result.exit_code == 0
    assert process.attempts == 2


async def test_daytona_exec_retries_misclassified_transport_errors() -> None:
    inner = InnerSandbox()
    process = MisclassifiedTransportProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    result = await sandbox.exec("pytest")

    assert result.exit_code == 0
    assert process.attempts == 2


async def test_daytona_exec_does_not_retry_detailed_execute_command_errors() -> None:
    """Detailed Daytona exec failures should not be retried as transient blank errors.

    Test cases:
    - A DaytonaError with details after "Failed to execute command:" raises SandboxError.
    - The detailed error is attempted once instead of being retried.
    """
    inner = InnerSandbox()
    process = DetailedFailedExecuteCommandProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError, match="permission denied"):
        await sandbox.exec("pytest")

    assert process.attempts == 1


async def test_daytona_exec_raises_sandbox_not_found_when_removed() -> None:
    """Exec failures from removed Daytona sandboxes should use the provider not-found type.

    Test cases:
    - A Daytona not-found response from process exec raises SandboxNotFoundError with sandbox identity.
    """
    inner = InnerSandbox()
    inner.process = RemovedSandboxProcess()
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        await sandbox.exec("pytest")


async def test_daytona_file_operations_raise_sandbox_not_found_when_removed() -> None:
    """File operation failures from removed Daytona sandboxes should use the provider not-found type.

    Test cases:
    - A Daytona not-found response from file upload raises SandboxNotFoundError with sandbox identity.
    - A Daytona proxy 502 from file download raises SandboxNotFoundError with sandbox identity.
    """
    inner = InnerSandbox()
    inner.fs = RemovedSandboxFiles()
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        await sandbox.upload_file("/tmp/result.txt", b"hello world")

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        await sandbox.download_file("/tmp/result.txt")


async def test_daytona_command_raises_sandbox_not_found_when_removed() -> None:
    """PTY command failures from removed Daytona sandboxes should use the provider not-found type.

    Test cases:
    - A Daytona not-found response from command health checks raises SandboxNotFoundError with sandbox identity.
    """
    inner = RemovedInnerSandbox()
    inner.process = CreatePtyFailureProcess()
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.command("pytest")]


async def test_daytona_download_file_streams_content() -> None:
    sandbox = DaytonaSandbox(cast(Any, InnerSandbox()))

    assert await sandbox.download_file("/tmp/result.txt") == b"hello world"


async def test_daytona_stream_download_yields_chunks() -> None:
    sandbox = DaytonaSandbox(cast(Any, InnerSandbox()))

    chunks = [chunk async for chunk in sandbox.stream_download("/tmp/result.txt")]

    assert chunks == [b"hello", b" world"]


async def test_daytona_stream_download_raises_sandbox_not_found_when_removed() -> None:
    inner = InnerSandbox()
    inner.fs = RemovedSandboxFiles()
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.stream_download("/tmp/result.txt")]


async def test_daytona_pty_caps_result_output_without_dropping_streamed_chunks() -> None:
    """The PTY exec result should retain only a bounded output tail.

    Test cases:
    - Every chunk is still forwarded to the streaming queue.
    - ExecResult.output is capped near the tail limit and keeps the newest output.
    """
    inner = InnerSandbox()
    inner.process = FloodingProcess()
    sandbox = DaytonaSandbox(cast(Any, inner))
    queue: asyncio.Queue[str] = asyncio.Queue()

    result = await sandbox._exec_pty("noisy", queue, {})  # pyright: ignore[reportPrivateUsage]

    chunk_length = 1024 + len("chunk-0000-")
    total_streamed = 0
    while not queue.empty():
        total_streamed += len(queue.get_nowait())
    assert total_streamed == 200 * chunk_length
    assert len(result.output) <= _PTY_STDOUT_TAIL_MAX_BYTES + chunk_length
    assert "chunk-0199-" in result.output
    assert "chunk-0000-" not in result.output


async def test_daytona_command_streams_output() -> None:
    sandbox = DaytonaSandbox(cast(Any, InnerSandbox()))

    output = [chunk async for chunk in sandbox.command("printf hello")]

    assert output == ["hello"]


async def test_daytona_command_uses_native_process_environment() -> None:
    secret = "value with spaces; $(touch /tmp/leaked)"
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    output = [chunk async for chunk in sandbox.command("printf hello", env_vars={"AGENT_SECRET": secret})]

    assert output == ["hello"]
    assert inner.process.pty_envs == {
        "TERM": "dumb",
        "LANG": "C.UTF-8",
        "AGENT_SECRET": secret,
    }
    assert inner.process.pty_handle is not None
    assert all(secret not in data for data in inner.process.pty_handle.inputs)


@pytest.mark.parametrize(
    ("env_vars", "message"),
    [
        ({"BAD-NAME": "secret"}, "Invalid environment variable names: BAD-NAME"),
        ({"TERM": "xterm"}, "Reserved command environment variable names: TERM"),
    ],
)
async def test_daytona_command_rejects_invalid_environment_before_provider_call(
    env_vars: dict[str, str],
    message: str,
) -> None:
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(ValueError, match=message):
        _ = [chunk async for chunk in sandbox.command("true", env_vars=env_vars)]

    assert inner.process.pty_envs is None


async def test_daytona_command_finishes_when_pty_close_never_arrives(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("benchmark_service.sandbox.daytona._PTY_STATUS_POLL_SECONDS", 0)
    inner = InnerSandbox()
    process = BlockingProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    async with asyncio.timeout(1):
        output = [chunk async for chunk in sandbox.command("printf hello")]

    assert output == ["hello"]
    assert process.handle is not None
    assert process.handle.disconnected is True
    assert process.killed_session_id is not None


async def test_daytona_command_checks_pty_before_reconnecting() -> None:
    inner = InnerSandbox()
    process = ReconnectingProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    output = [chunk async for chunk in sandbox.command("printf hello")]

    assert output == ["hello"]
    assert process.checked_session_id is not None
    assert process.connected_session_id == process.checked_session_id


async def test_daytona_command_prefers_status_over_pty_exit() -> None:
    inner = InnerSandbox()
    process = LostPtyProcess(
        PtyResult(exit_code=137, error="SIGKILL"),
        DaytonaNotFoundError("PTY session not found"),
    )
    process.connected_session_id = "status-written"
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert [chunk async for chunk in sandbox.command("printf hello")] == ["hello"]
    assert process.checked_session_id is None


async def test_daytona_command_reports_pty_exit_before_status() -> None:
    inner = InnerSandbox()
    process = LostPtyProcess(
        PtyResult(exit_code=137, error="SIGKILL"),
        DaytonaNotFoundError("PTY session not found"),
    )
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError, match="PTY exited before writing command status.*exit_code=137, error='SIGKILL'"):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert process.checked_session_id is None


@pytest.mark.parametrize(
    "reconnect_error",
    [
        DaytonaNotFoundError("PTY session not found"),
        DaytonaConnectionError("PTY session not found"),
    ],
)
async def test_daytona_command_reports_missing_pty_context(reconnect_error: DaytonaError) -> None:
    inner = InnerSandbox()
    process = LostPtyProcess(
        PtyResult(exit_code=0),
        reconnect_error,
    )
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError) as exc_info:
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    message = str(exc_info.value)
    assert "PTY session disappeared before command status was written" in message
    assert "wait_result=(exit_code=0, error=None)" in message
    assert "state=SandboxState.STARTED" in message
    assert inner.refresh_count == 2


async def test_daytona_command_checks_sandbox_health_before_reconnecting() -> None:
    inner = InnerSandbox()
    inner.process = ReconnectingProcess()
    inner.state = SandboxState.DESTROYED
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 1


async def test_daytona_command_checks_sandbox_health_after_pty_create_failure() -> None:
    inner = InnerSandbox()
    inner.process = CreatePtyFailureProcess()
    inner.state = SandboxState.DESTROYED
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 1


async def test_daytona_command_checks_sandbox_health_after_reconnect_failure() -> None:
    inner = InnerSandbox()
    inner.process = CrashingReconnectProcess(inner)
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 2


async def test_daytona_command_keeps_error_state_as_sandbox_error() -> None:
    """Non-removal dead states should still report a generic sandbox failure.

    Test cases:
    - SandboxState.ERROR raises SandboxError during command health checks.
    """
    inner = InnerSandbox()
    inner.process = CreatePtyFailureProcess()
    inner.state = SandboxState.ERROR
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError, match="Sandbox is not running: name=sandbox-name, id=sandbox-id"):
        _ = [chunk async for chunk in sandbox.command("printf hello")]


async def test_daytona_command_cleans_up_when_consumer_stops() -> None:
    inner = InnerSandbox()
    process = BlockingProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    stream = sandbox.command("printf hello")
    assert await anext(stream) == "hello"

    await stream.aclose()

    assert process.handle is not None
    assert process.handle.disconnected is True
    assert process.killed_session_id is not None


async def test_daytona_provider_creates_fresh_sandbox() -> None:
    inner = InnerSandbox()
    daytona = DaytonaClient(inner)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.created is True


async def test_daytona_provider_recovers_existing_sandbox_on_create_conflict() -> None:
    """A lost create response followed by a retry hits a name conflict; the
    provider must recover the existing sandbox by name instead of failing and
    orphaning it."""
    inner = InnerSandbox()

    class ConflictingCreateClient(DaytonaClient):
        async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
            self.created = True
            raise DaytonaError(f"Sandbox with name {inner.name} already exists")

    daytona = ConflictingCreateClient(inner)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert daytona.created is True
    assert sandbox.id == inner.id


async def test_daytona_provider_times_out_while_recovering_conflicting_sandbox() -> None:
    """Conflict recovery must use the request's total creation deadline."""

    class NeverStartingSandbox(InnerSandbox):
        state = SandboxState.BUILDING_SNAPSHOT

        async def wait_for_sandbox_start(self, timeout: int) -> None:
            assert timeout == 0
            await asyncio.Event().wait()

    class ConflictingCreateClient(DaytonaClient):
        async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
            raise DaytonaConflictError("Sandbox already exists")

    inner = NeverStartingSandbox()
    request = _request(inner.name).model_copy(update={"create_timeout": 1})

    async with asyncio.timeout(2):
        with pytest.raises(SandboxConnectionError, match=r"timed out.*1"):
            await _provider(ConflictingCreateClient(inner)).create_sandbox(request)


class CapturingCreateDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: "InnerSandbox") -> None:
        super().__init__(sandbox)
        self.create_params: Any | None = None

    async def get(self, instance_id: str) -> "InnerSandbox":
        raise DaytonaNotFoundError("sandbox not found")

    async def create(self, *args: object, **_kwargs: object) -> "InnerSandbox":
        self.create_params = args[0]
        self.created = True
        return self.sandbox


async def test_daytona_provider_uses_target_only_for_targeted_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    clients: dict[str, CapturingCreateDaytonaClient] = {}

    def create_client(*, config: Any) -> CapturingCreateDaytonaClient:
        client = CapturingCreateDaytonaClient(InnerSandbox())
        clients[config.target] = client
        return client

    monkeypatch.setattr(daytona_module, "AsyncDaytona", create_client)
    provider = DaytonaSandboxProvider(
        DaytonaProviderConfig(DAYTONA_API_KEY="key", DAYTONA_API_URL="url", DAYTONA_TARGET="us")
    )

    await provider.create_sandbox(_request("normal", source=SnapshotSource(snapshot="normal-snapshot")))
    assert list(clients) == ["us"]
    assert clients["us"].create_params is not None
    assert clients["us"].create_params.snapshot == "normal-snapshot"

    targeted_request = _request(
        "targeted",
        source=TargetedSnapshotSource(snapshot="masscan-snapshot", target="us-west-3"),
    )
    await provider.create_sandbox(targeted_request)
    await provider.create_sandbox(targeted_request)
    assert list(clients) == ["us", "us-west-3"]
    assert clients["us-west-3"].create_params is not None
    assert clients["us-west-3"].create_params.snapshot == "masscan-snapshot"

    await provider.close()
    assert all(client.closed for client in clients.values())


async def test_targeted_snapshot_recovers_name_conflict_with_target_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default_inner = InnerSandbox()
    default_inner.name = "default-sandbox"
    targeted_inner = InnerSandbox()
    targeted_inner.name = "targeted-sandbox"

    class ConflictClient(DaytonaClient):
        def __init__(self, sandbox: InnerSandbox) -> None:
            super().__init__(sandbox)
            self.lookups: list[str] = []

        async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
            raise DaytonaError(f"Sandbox with name {self.sandbox.name} already exists")

        async def get(self, instance_id: str) -> InnerSandbox:
            self.lookups.append(instance_id)
            return self.sandbox

    default_client = DaytonaClient(default_inner)
    targeted_client = ConflictClient(targeted_inner)

    def create_client(*, config: Any) -> DaytonaClient:
        return targeted_client if config.target == "us-west-3" else default_client

    monkeypatch.setattr(daytona_module, "AsyncDaytona", create_client)
    provider = DaytonaSandboxProvider(
        DaytonaProviderConfig(DAYTONA_API_KEY="key", DAYTONA_API_URL="url", DAYTONA_TARGET="us")
    )

    sandbox = await provider.create_sandbox(
        _request(
            targeted_inner.name,
            source=TargetedSnapshotSource(snapshot="masscan-snapshot", target="us-west-3"),
        )
    )

    assert sandbox.name == targeted_inner.name
    assert targeted_client.lookups == [targeted_inner.name]


async def test_daytona_provider_maps_gpu_resources() -> None:
    inner = InnerSandbox()
    daytona = CapturingCreateDaytonaClient(inner)
    resources = Resources(vcpu=2, memory=4, disk=10, gpu=1, gpu_type="H100")

    await _provider(daytona).create_sandbox(_request(inner.name, resources=resources))

    assert daytona.create_params is not None
    assert daytona.create_params.resources.gpu == 1
    assert daytona.create_params.resources.gpu_type == GpuType.H100


async def test_daytona_provider_omits_gpu_by_default() -> None:
    inner = InnerSandbox()
    daytona = CapturingCreateDaytonaClient(inner)

    await _provider(daytona).create_sandbox(_request(inner.name))

    assert daytona.create_params is not None
    assert daytona.create_params.resources.gpu is None
    assert daytona.create_params.resources.gpu_type is None


async def test_daytona_provider_rejects_unsupported_gpu_type() -> None:
    daytona = CapturingCreateDaytonaClient(InnerSandbox())
    resources = Resources(vcpu=2, memory=4, disk=10, gpu=1, gpu_type="T4")

    with pytest.raises(SandboxError, match="Unsupported Daytona GPU type: T4"):
        await _provider(daytona).create_sandbox(_request("sandbox-name", resources=resources))


async def test_daytona_provider_rejects_gpu_for_snapshot_source() -> None:
    daytona = CapturingCreateDaytonaClient(InnerSandbox())
    resources = Resources(vcpu=2, memory=4, disk=10, gpu=1, gpu_type="H100")

    with pytest.raises(SandboxError, match="GPUs cannot be requested"):
        await _provider(daytona).create_sandbox(
            _request("sandbox-name", resources=resources, source=SnapshotSource(snapshot="snap-1"))
        )


def test_resources_gpu_type_requires_gpu_count() -> None:
    with pytest.raises(ValueError, match="gpu_type requires gpu >= 1"):
        Resources(vcpu=2, memory=4, disk=10, gpu_type="H100")


def test_sandbox_source_rejects_unknown_type() -> None:
    with pytest.raises(ValidationError) as exc_info:
        TypeAdapter(SandboxSource).validate_python({"type": "unknown", "snapshot": "snap-1"})

    assert exc_info.value.errors()[0]["type"] == "union_tag_invalid"


def test_targeted_snapshot_rejects_empty_target() -> None:
    with pytest.raises(ValidationError, match="at least 1 character"):
        TargetedSnapshotSource(snapshot="snap-1", target="")


async def test_daytona_provider_delete_sets_autostop_before_delete() -> None:
    inner = InnerSandbox()
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert inner.autostop_interval == 1
    assert daytona.deleted is True


async def test_daytona_provider_delete_retries_state_conflicts() -> None:
    inner = DeleteConflictSandbox()
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert inner.autostop_attempts == 2
    assert daytona.deleted is True


async def test_daytona_provider_delete_retries_bare_html_502_refresh_errors() -> None:
    inner = BareHtml502RefreshSandbox()
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert inner.refresh_attempts == 2
    assert daytona.deleted is True


async def test_daytona_provider_get_retries_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = InnerSandbox()
    daytona = UnexpectedGetDaytonaClient(inner)

    async def no_wait(_seconds: float) -> None:
        pass

    retryer = cast(Any, DaytonaSandboxProvider.get_sandbox).retry
    monkeypatch.setattr(retryer, "sleep", no_wait)

    sandbox = await _provider(daytona).get_sandbox(inner.name)

    assert sandbox.id == inner.id
    assert daytona.get_attempts == 2


async def test_daytona_provider_delete_retries_unexpected_remove_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = InnerSandbox()
    daytona = UnexpectedRemoveDaytonaClient(inner)

    async def no_wait(_seconds: float) -> None:
        pass

    retryer = cast(Any, DaytonaSandboxProvider.delete_sandbox).retry
    monkeypatch.setattr(retryer, "sleep", no_wait)

    await _provider(daytona).delete_sandbox(inner.name)

    assert daytona.delete_attempts == 2
    assert daytona.deleted is True


async def test_daytona_provider_create_retries_wrapped_lookup_connection_errors() -> None:
    inner = InnerSandbox()
    daytona = ConflictThenFlakyLookupDaytonaClient(inner)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.get_attempts == 2
    assert daytona.created is True


async def test_daytona_provider_create_recovers_from_sysbox_runner_fault() -> None:
    daytona = SysboxRunnerFaultDaytonaClient()

    sandbox = await _provider(daytona).create_sandbox(_request(daytona.sandbox.name))

    assert sandbox.id == RecreatedInnerSandbox.id
    assert daytona.create_attempts == 2
    assert daytona.delete_attempts == 2
    assert daytona.deleted_sandboxes == [daytona.failed_sandbox]


async def test_daytona_provider_create_maps_daytona_errors() -> None:
    """Unrelated create failures should remain non-retryable provider errors.

    Test cases:
    - An unrelated OCI runtime failure raises exactly SandboxError and is attempted once.
    """
    daytona = CreateFailureDaytonaClient(InnerSandbox())

    with pytest.raises(SandboxError, match="invalid mount configuration") as exc_info:
        await _provider(daytona).create_sandbox(_request("sandbox-name"))

    assert type(exc_info.value) is SandboxError
    assert daytona.create_attempts == 1


async def test_daytona_provider_create_maps_not_found_errors() -> None:
    """Create races with deleted Daytona sandboxes should preserve not-found semantics.

    Test cases:
    - A not-found Daytona create failure raises SandboxNotFoundError instead of generic SandboxError.
    """
    daytona = CreateNotFoundDaytonaClient(InnerSandbox())

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: sandbox not found"):
        await _provider(daytona).create_sandbox(_request("sandbox-name"))


async def test_daytona_provider_delete_removes_error_state_sandbox() -> None:
    """Failed-state sandboxes should be deletable without waiting for startup.

    Test cases:
    - A sandbox in SandboxState.ERROR is deleted even though waiting for startup fails.
    """
    inner = ErrorStateSandbox()
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert daytona.deleted is True


async def test_daytona_provider_delete_removes_sandbox_that_fails_after_refresh() -> None:
    """Sandbox state can change after the initial get and still be deletable.

    Test cases:
    - A sandbox that refreshes into SandboxState.ERROR is deleted without setting autostop.
    """
    inner = RefreshToErrorSandbox()
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert inner.refresh_count == 1
    assert inner.autostop_interval is None
    assert daytona.deleted is True


@pytest.mark.parametrize("state", [SandboxState.STOPPED, SandboxState.ARCHIVED])
async def test_daytona_provider_delete_removes_inactive_sandbox_without_starting(state: SandboxState) -> None:
    inner = InactiveSandbox(state)
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert daytona.deleted is True


async def test_daytona_provider_delete_waits_for_building_sandbox() -> None:
    inner = BuildingSandbox()
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert inner.waited_for_start is True
    assert inner.refresh_count == 1
    assert inner.autostop_interval == 1
    assert daytona.deleted is True


async def test_daytona_provider_lists_sandboxes_with_query() -> None:
    inner = InnerSandbox()
    daytona = DaytonaClient(inner)

    sandboxes = [
        sandbox async for sandbox in _provider(daytona).list_sandboxes(SandboxQuery(labels={"Benchmark": "vcb"}))
    ]

    assert [sandbox.id for sandbox in sandboxes] == [inner.id]
    assert daytona.listed_query is not None
    assert daytona.listed_query.labels == {"Benchmark": "vcb"}
    assert daytona.listed_query.limit == 10


async def test_daytona_updates_egress_rules() -> None:
    """Verify Daytona receives only one allowlist type per egress update.

    Test cases:
    - Domain entries use Daytona's domain allowlist.
    - CIDR and IPv4 entries use Daytona's network allowlist.
    - Mixed domain and CIDR entries fail before the provider request.
    - Clearing egress rules clears both allowlist fields.
    """
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.modify_egress_rules(["https://api.openai.com/v1", "github.com"])

    assert inner.network_settings[-1] == {
        "network_block_all": None,
        "network_allow_list": "",
        "domain_allow_list": "api.openai.com,github.com",
    }

    await sandbox.modify_egress_rules(["198.51.100.20/32", "203.0.113.10"])

    assert inner.network_settings[-1] == {
        "network_block_all": None,
        "network_allow_list": "198.51.100.20/32,203.0.113.10/32",
        "domain_allow_list": "",
    }

    request_count = len(inner.network_settings)
    with pytest.raises(ValueError, match="allowed addresses cannot mix domains and CIDRs"):
        await sandbox.modify_egress_rules(["https://api.openai.com/v1", "198.51.100.20/32"])

    assert len(inner.network_settings) == request_count

    await sandbox.clear_egress_rules()

    assert inner.network_settings[-1] == {
        "network_block_all": False,
        "network_allow_list": "",
        "domain_allow_list": "",
    }


async def test_daytona_create_forwards_network_block_all() -> None:
    daytona = CapturingCreateDaytonaClient(InnerSandbox())
    request = _request("grade-sb").model_copy(update={"network_block_all": True})

    await _provider(daytona).create_sandbox(request)

    assert daytona.create_params is not None
    assert daytona.create_params.network_block_all is True


def test_daytona_config_from_env_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAYTONA_API_KEY", "key-1")
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example")
    monkeypatch.setenv("DAYTONA_TARGET", "us")
    monkeypatch.setenv("DAYTONA_ORGANIZATION_ID", "org-1")
    config = DaytonaProviderConfig.from_env()
    assert config.DAYTONA_API_KEY == "key-1"
    assert config.DAYTONA_API_URL == "https://daytona.example"
    assert config.DAYTONA_TARGET == "us"
    assert config.DAYTONA_ORGANIZATION_ID == "org-1"


def test_daytona_config_from_env_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example")
    monkeypatch.setenv("DAYTONA_TARGET", "us")
    with pytest.raises(MissingSandboxConfigError, match="DAYTONA_API_KEY"):
        DaytonaProviderConfig.from_env()
