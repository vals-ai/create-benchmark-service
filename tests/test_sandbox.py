import asyncio
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, cast

import pytest
from aiohttp import ClientConnectionError, ClientResponseError, RequestInfo
from daytona import SandboxState
from daytona.common.errors import (
    DaytonaConflictError,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaRateLimitError,
)
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from benchmark_service.sandbox import (
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
)
from benchmark_service.sandbox.daytona import (
    DaytonaProviderConfig,
    DaytonaSandbox,
    DaytonaSandboxProvider,
    daytona_retry_after_seconds,
)


def _client_response_error(status: int, message: str) -> ClientResponseError:
    url = URL("https://daytona.example.test")
    headers: CIMultiDict[str] = CIMultiDict()
    request_info = RequestInfo(url=url, method="GET", headers=CIMultiDictProxy(headers), real_url=url)
    return ClientResponseError(request_info, (), status=status, message=message)


class Process:
    def __init__(self) -> None:
        self.command: str | None = None

    async def exec(self, command: str) -> SimpleNamespace:
        self.command = command
        if command.startswith("test -e "):
            return SimpleNamespace(exit_code=0, result="")
        if command.startswith("cat "):
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
        assert envs == {"TERM": "dumb", "LANG": "C.UTF-8"}
        return PtyHandle(on_data)

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

    async def send_input(self, data: str) -> None:
        if data.startswith("stty"):
            return
        result = self._on_data(b"hello")
        if result is not None:
            await result

    async def wait(self) -> None:
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
        if command.startswith("test -e "):
            return SimpleNamespace(exit_code=0 if self.connected_session_id else 1, result="")
        return await super().exec(command)

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
    ) -> DisconnectedPtyHandle:
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


class InnerSandbox:
    id = "sandbox-id"
    name = "sandbox-name"
    state = SandboxState.STARTED

    def __init__(self) -> None:
        self.process = Process()
        self.fs = Files()
        self.autostop_interval: int | None = None
        self.refresh_count = 0

    async def set_autostop_interval(self, interval: int) -> None:
        self.autostop_interval = interval

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


class DaytonaClient:
    def __init__(self, sandbox: InnerSandbox) -> None:
        self.sandbox = sandbox
        self.created = False
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

    def list(self, query: object) -> Any:
        self.listed_query = query

        async def sandboxes() -> Any:
            yield self.sandbox

        return sandboxes()


class CreateFailureDaytonaClient(DaytonaClient):
    async def get(self, instance_id: str) -> InnerSandbox:
        raise DaytonaNotFoundError("sandbox not found")

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        raise DaytonaError("sandbox failed to start")


class CreateNotFoundDaytonaClient(CreateFailureDaytonaClient):
    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        raise DaytonaError("sandbox not found", status_code=404)


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


def _provider(daytona: DaytonaClient) -> DaytonaSandboxProvider:
    provider = DaytonaSandboxProvider.__new__(DaytonaSandboxProvider)
    provider._daytona = cast(Any, daytona)  # pyright: ignore[reportPrivateUsage]
    return provider


def _request(name: str) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=ImageSource(image="python:3.12"),
        resources=Resources(vcpu=2, memory=4, disk=10),
        name=name,
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )


async def test_daytona_command_applies_timeout_inside_cwd() -> None:
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", cwd="/workspace", timeout=60)

    assert inner.process.command == "cd /workspace && timeout 60 pytest"


async def test_daytona_command_preserves_fractional_timeout() -> None:
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", timeout=0.5)

    assert inner.process.command == "timeout 0.5 pytest"


def test_daytona_retry_after_uses_specific_header_first() -> None:
    exc = DaytonaRateLimitError(
        "rate limited",
        headers={"retry-after": "10", "retry-after-sandbox-create": "3"},
    )

    assert daytona_retry_after_seconds(exc) == 3


def test_daytona_retry_after_uses_any_retry_after_header() -> None:
    exc = DaytonaRateLimitError("rate limited", headers={"retry-after-custom": "5"})

    assert daytona_retry_after_seconds(exc) == 5


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


async def test_daytona_command_streams_output() -> None:
    sandbox = DaytonaSandbox(cast(Any, InnerSandbox()))

    output = [chunk async for chunk in sandbox.command("printf hello")]

    assert output == ["hello"]


async def test_daytona_command_checks_pty_before_reconnecting() -> None:
    inner = InnerSandbox()
    process = ReconnectingProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    output = [chunk async for chunk in sandbox.command("printf hello")]

    assert output == ["hello"]
    assert process.checked_session_id is not None
    assert process.connected_session_id == process.checked_session_id


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


async def test_daytona_provider_create_retries_transient_conflict_lookup_errors() -> None:
    """A transient connection error during the conflict-recovery lookup must be
    retried, not fail the create."""
    inner = InnerSandbox()
    daytona = ConflictThenFlakyLookupDaytonaClient(inner)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.get_attempts == 2
    assert daytona.created is True


async def test_daytona_provider_create_maps_daytona_errors() -> None:
    """Create failures from Daytona should use the provider sandbox error type.

    Test cases:
    - A Daytona create failure raises SandboxError instead of leaking DaytonaError.
    """
    daytona = CreateFailureDaytonaClient(InnerSandbox())

    with pytest.raises(SandboxError, match="sandbox failed to start"):
        await _provider(daytona).create_sandbox(_request("sandbox-name"))


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


class CapturingCreateClient(DaytonaClient):
    """Forces the create path (get raises NotFound) and captures the params."""

    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.create_params: Any = None

    async def get(self, instance_id: str) -> InnerSandbox:
        raise DaytonaNotFoundError("sandbox not found")

    async def create(self, *args: object, **_kwargs: object) -> InnerSandbox:
        self.create_params = args[0]
        self.created = True
        return self.sandbox


async def test_daytona_create_forwards_network_block_all() -> None:
    daytona = CapturingCreateClient(InnerSandbox())
    request = _request("grade-sb").model_copy(update={"network_block_all": True})

    await _provider(daytona).create_sandbox(request)

    assert daytona.create_params.network_block_all is True


def test_daytona_config_from_env_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAYTONA_API_KEY", "key-1")
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example")
    monkeypatch.setenv("DAYTONA_TARGET", "us")
    config = DaytonaProviderConfig.from_env()
    assert config.DAYTONA_API_KEY == "key-1"
    assert config.DAYTONA_API_URL == "https://daytona.example"
    assert config.DAYTONA_TARGET == "us"


def test_daytona_config_from_env_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example")
    monkeypatch.setenv("DAYTONA_TARGET", "us")
    with pytest.raises(MissingSandboxConfigError, match="DAYTONA_API_KEY"):
        DaytonaProviderConfig.from_env()
