import asyncio
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, cast

import pytest
from daytona import DaytonaNotFoundError, SandboxState
from daytona.common.errors import DaytonaConflictError, DaytonaConnectionError, DaytonaRateLimitError

from benchmark_service.sandbox import (
    ImageSource,
    Resources,
    SandboxError,
    SandboxCreateRequest,
    SandboxQuery,
)
from benchmark_service.sandbox.daytona import DaytonaSandbox, DaytonaSandboxProvider, daytona_retry_after_seconds


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
    async def download_file_stream(self, remote_path: str) -> Any:
        assert remote_path == "/tmp/result.txt"

        async def chunks() -> Any:
            yield b"hello"
            yield b" world"

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


class DeleteConflictSandbox(InnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.autostop_attempts = 0

    async def set_autostop_interval(self, interval: int) -> None:
        self.autostop_attempts += 1
        if self.autostop_attempts == 1:
            raise DaytonaConflictError("Failed to set auto-stop interval: Sandbox was modified by another operation")
        await super().set_autostop_interval(interval)


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


class DestroyingNameConflictDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox, conflict_message: str = "Sandbox name already exists") -> None:
        super().__init__(sandbox)
        self.conflict_message = conflict_message
        self.create_attempts = 0
        self.get_attempts = 0
        self.name_released = False

    async def get(self, instance_id: str) -> InnerSandbox:
        self.get_attempts += 1
        if self.get_attempts >= 3:
            self.name_released = True
            raise DaytonaNotFoundError("not found")
        return await super().get(instance_id)

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        self.create_attempts += 1
        if self.create_attempts == 1:
            raise DaytonaConflictError(self.conflict_message)
        assert self.name_released
        self.created = True
        self.sandbox.state = SandboxState.STARTED
        return self.sandbox


class NonDestroyingNameConflictDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.create_attempts = 0

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        self.create_attempts += 1
        raise DaytonaConflictError("Sandbox name already exists")


class InvisibleReservedNameConflictDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.create_attempts = 0
        self.get_attempts = 0

    async def get(self, instance_id: str) -> InnerSandbox:
        self.get_attempts += 1
        raise DaytonaNotFoundError("not found")

    async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
        self.create_attempts += 1
        if self.create_attempts < 3:
            raise DaytonaConflictError(f"Sandbox with name {self.sandbox.name} already exists")
        self.created = True
        self.sandbox.state = SandboxState.STARTED
        return self.sandbox


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

    with pytest.raises(SandboxError, match="crashed during command execution"):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 1


async def test_daytona_command_checks_sandbox_health_after_pty_create_failure() -> None:
    inner = InnerSandbox()
    inner.process = CreatePtyFailureProcess()
    inner.state = SandboxState.DESTROYED
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError, match="crashed during command execution"):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 1


async def test_daytona_command_checks_sandbox_health_after_reconnect_failure() -> None:
    inner = InnerSandbox()
    inner.process = CrashingReconnectProcess(inner)
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError, match="crashed during command execution"):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 2


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


async def test_daytona_provider_waits_for_destroying_sandbox_name_before_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Daytona can keep a sandbox name reserved while a previous sandbox is still destroying.

    Test cases:
    - Create name conflict waits for the destroying sandbox to disappear.
    - Creation retries with the original name after Daytona releases it.
    """
    inner = InnerSandbox()
    inner.state = SandboxState.DESTROYING
    daytona = DestroyingNameConflictDaytonaClient(inner)
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("benchmark_service.sandbox.daytona.asyncio.sleep", fake_sleep)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.name_released is True
    assert daytona.create_attempts == 2
    assert daytona.get_attempts == 3
    assert sleep_calls == [2, 2]


async def test_daytona_provider_waits_for_production_destroying_name_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Daytona create conflicts include the conflicting sandbox name in production.

    Test cases:
    - The production conflict wording waits for a destroying sandbox to disappear.
    - Creation retries with the original name after Daytona releases it.
    """
    inner = InnerSandbox()
    inner.state = SandboxState.DESTROYING
    daytona = DestroyingNameConflictDaytonaClient(inner, f"Sandbox with name {inner.name} already exists")
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("benchmark_service.sandbox.daytona.asyncio.sleep", fake_sleep)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.name_released is True
    assert daytona.create_attempts == 2
    assert daytona.get_attempts == 3
    assert sleep_calls == [2, 2]


async def test_daytona_provider_retries_when_reserved_name_is_not_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Daytona can reserve a sandbox name even when fetching that name returns not found.

    Test cases:
    - Create name conflict keeps retrying when the reserved name is not visible.
    - Creation succeeds after Daytona releases the invisible reservation.
    """
    inner = InnerSandbox()
    daytona = InvisibleReservedNameConflictDaytonaClient(inner)
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("benchmark_service.sandbox.daytona.asyncio.sleep", fake_sleep)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.create_attempts == 3
    assert daytona.get_attempts == 3
    assert sleep_calls == [2, 2]


async def test_daytona_provider_does_not_wait_for_non_destroying_name_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    A name conflict is only retryable when Daytona reports the existing sandbox is still destroying.

    Test cases:
    - A stopped sandbox name conflict raises without polling the full timeout.
    - No create retry happens when the existing sandbox is not being destroyed.
    """
    inner = InnerSandbox()
    inner.state = SandboxState.STOPPED
    daytona = NonDestroyingNameConflictDaytonaClient(inner)
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("benchmark_service.sandbox.daytona.asyncio.sleep", fake_sleep)

    with pytest.raises(SandboxError, match="not being destroyed"):
        await _provider(daytona).create_sandbox(_request(inner.name))

    assert daytona.create_attempts == 1
    assert sleep_calls == []


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
