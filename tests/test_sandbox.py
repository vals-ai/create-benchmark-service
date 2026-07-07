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
    Resources,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
)
from benchmark_service.sandbox import daytona as daytona_module
from benchmark_service.sandbox.daytona import DaytonaSandbox, DaytonaSandboxProvider, daytona_retry_after_seconds


def _client_response_error(status: int, message: str) -> ClientResponseError:
    url = URL("https://daytona.example.test")
    headers: CIMultiDict[str] = CIMultiDict()
    request_info = RequestInfo(url=url, method="GET", headers=CIMultiDictProxy(headers), real_url=url)
    return ClientResponseError(request_info, (), status=status, message=message)


class Process:
    def __init__(self) -> None:
        self.command: str | None = None
        self.commands: list[str] = []

    async def exec(self, command: str) -> SimpleNamespace:
        self.command = command
        self.commands.append(command)
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
        self.network_block_all: bool | None = None
        self.network_allow_list: str | None = None
        self.refresh_count = 0

    async def set_autostop_interval(self, interval: int) -> None:
        self.autostop_interval = interval

    async def update_network_settings(self, *, network_block_all: bool, network_allow_list: str | None) -> None:
        self.network_block_all = network_block_all
        self.network_allow_list = network_allow_list

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


class ReusableLookupConnectionDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.get_attempts = 0

    async def get(self, instance_id: str) -> InnerSandbox:
        self.get_attempts += 1
        if self.get_attempts == 1:
            raise _daytona_error_with_cause(
                "Failed to get sandbox",
                ClientConnectionError("tcp reset"),
            )
        raise DaytonaNotFoundError("sandbox not found")


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


async def test_daytona_provider_reuses_started_sandbox() -> None:
    inner = InnerSandbox()
    daytona = DaytonaClient(inner)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.created is False


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


async def test_daytona_provider_create_retries_wrapped_lookup_connection_errors() -> None:
    inner = InnerSandbox()
    daytona = ReusableLookupConnectionDaytonaClient(inner)

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


async def test_daytona_updates_egress_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Runtime egress changes should map to Daytona's sandbox network settings.

    Test cases:
    - URL hosts resolve to Daytona CIDR rules and are pinned idempotently in /etc/hosts.
    - Explicit CIDR addresses pass through to the comma-separated allow-list value.
    - An empty address is rejected before updating provider rules.
    - IPv6 CIDRs are rejected because Daytona network rules use IPv4 CIDRs.
    - Calling sandbox.clear_egress_rules removes host pins and restores unrestricted outbound network access.
    """
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    def getaddrinfo(
        host: str, *_args: object, **_kwargs: object
    ) -> list[tuple[object, object, object, object, tuple[str, int]]]:
        assert host == "api.openai.com"

        return [(object(), object(), object(), object(), ("203.0.113.10", 443))]

    monkeypatch.setattr(daytona_module.socket, "getaddrinfo", getaddrinfo)

    await sandbox.modify_egress_rules(["https://api.openai.com/v1", "198.51.100.20/32"])

    assert inner.network_block_all is False
    assert inner.network_allow_list == "203.0.113.10/32,198.51.100.20/32"
    assert inner.process.command is not None
    assert "203.0.113.10 api.openai.com" in inner.process.command
    assert "# benchmark-service egress allowlist begin" in inner.process.command
    assert "sed '/^# benchmark-service egress allowlist begin$/,/^# benchmark-service egress allowlist end$/d'" in (
        inner.process.command
    )

    with pytest.raises(
        ValueError,
        match=("allowed addresses cannot be empty; use sandbox\\.clear_egress_rules to clear egress rules"),
    ):
        await sandbox.modify_egress_rules([" "])

    with pytest.raises(ValueError, match="allowed address is an IPv6 CIDR which is not supported"):
        await sandbox.modify_egress_rules(["2001:db8::/32"])

    await sandbox.clear_egress_rules()

    assert inner.network_block_all is False
    assert inner.network_allow_list is None
    assert inner.process.commands[-1] == (
        "sed '/^# benchmark-service egress allowlist begin$/,/^# benchmark-service egress allowlist end$/d' "
        "/etc/hosts > /tmp/.benchmark-service-egress-hosts"
        " && cat /tmp/.benchmark-service-egress-hosts > /etc/hosts"
        " && rm -f /tmp/.benchmark-service-egress-hosts"
    )
