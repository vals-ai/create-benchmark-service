import asyncio
import logging
import shlex
from collections.abc import AsyncGenerator, Mapping
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, cast

import pytest
from aiohttp import ClientConnectionError, ClientPayloadError, ClientResponseError, InvalidURL, RequestInfo
from daytona import DaytonaConfig, GpuType, SandboxState
from daytona.common.errors import (
    SOURCE_API,
    SOURCE_DAEMON,
    SOURCE_PROXY,
    DaytonaConflictError,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaRateLimitError,
    DaytonaTimeoutError,
)
from daytona.common.pty import PtyResult, PtySize
from daytona_api_client import VolumeState
from daytona_api_client_async import SandboxClass
from daytona_api_client_async.exceptions import ApiException, NotFoundException
from daytona_toolbox_api_client_async.models.pty_session_info import PtySessionInfo
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
    VolumeMount,
)
from benchmark_service.sandbox.daytona import (
    _PTY_COLS,  # pyright: ignore[reportPrivateUsage]
    _PTY_CREATE_MARKER_ENV,  # pyright: ignore[reportPrivateUsage]
    _PTY_ROWS,  # pyright: ignore[reportPrivateUsage]
    _PTY_STDOUT_TAIL_MAX_BYTES,  # pyright: ignore[reportPrivateUsage]
    DaytonaProviderConfig,
    _is_ambiguous_pty_create_error,  # pyright: ignore[reportPrivateUsage]
    _is_not_found_error,  # pyright: ignore[reportPrivateUsage]
    _is_transient_daytona_error,  # pyright: ignore[reportPrivateUsage]
    DaytonaSandbox,
    DaytonaSandboxProvider,
    daytona_retry_after_seconds,
)


_SENSITIVE_RETRY_SENTINEL = "SENSITIVE_RETRY_SENTINEL"
_STANDARD_LOG_RECORD_KEYS = frozenset(vars(logging.LogRecord("", logging.WARNING, "", 0, "", (), None)))


def _client_response_error(
    status: int,
    message: str,
    *,
    url: str = "https://daytona.example.test",
    headers: Mapping[str, str] | None = None,
) -> ClientResponseError:
    request_url = URL(url)
    header_values: CIMultiDict[str] = CIMultiDict(headers or {})
    header_proxy = CIMultiDictProxy(header_values)
    request_info = RequestInfo(url=request_url, method="GET", headers=header_proxy, real_url=request_url)
    return ClientResponseError(request_info, (), status=status, message=message, headers=header_proxy)


def _callback_extra(record: logging.LogRecord) -> dict[str, object]:
    return {
        key: value
        for key, value in vars(record).items()
        if key not in _STANDARD_LOG_RECORD_KEYS and key not in {"asctime", "message"}
    }


def _retry_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == daytona_module.__name__ and record.getMessage().startswith("daytona.retry ")
    ]


def _assert_safe_retry_record(
    record: logging.LogRecord,
    expected_message: str,
    expected_fields: Mapping[str, object],
) -> None:
    assert record.levelno == logging.WARNING
    assert record.getMessage() == expected_message
    assert _callback_extra(record) == expected_fields
    assert record.exc_info is None
    assert record.exc_text is None
    assert record.stack_info is None
    assert _SENSITIVE_RETRY_SENTINEL not in repr((record.getMessage(), vars(record)))


def _skip_retry_sleep(monkeypatch: pytest.MonkeyPatch, method: object) -> None:
    """Run a tenacity-wrapped method's retries without their real backoff delays."""

    async def no_wait(_seconds: float) -> None:
        pass

    monkeypatch.setattr(cast(Any, method).retry, "sleep", no_wait)


def _unwrap_shell_command(command: str) -> str:
    if command.startswith("sh -c "):
        return shlex.split(command)[2]
    return command


def _assert_pty_create_config(envs: Mapping[str, str], pty_size: PtySize) -> None:
    assert envs["TERM"] == "dumb"
    assert envs["LANG"] == "C.UTF-8"
    assert envs[_PTY_CREATE_MARKER_ENV]
    assert pty_size == PtySize(rows=_PTY_ROWS, cols=_PTY_COLS)


class Process:
    def __init__(self) -> None:
        self.command: str | None = None
        self.pty_envs: dict[str, str] | None = None
        self.pty_size: PtySize | None = None
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
        pty_size: PtySize,
    ) -> "PtyHandle":
        assert id
        _assert_pty_create_config(envs, pty_size)
        self.pty_envs = envs
        self.pty_size = pty_size
        self.pty_handle = PtyHandle(on_data)
        return self.pty_handle

    async def kill_pty_session(self, session_id: str) -> None:
        assert session_id


class RetryingProcess(Process):
    def __init__(self, errors: list[BaseException]) -> None:
        super().__init__()
        self._errors = list(errors)
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        if self._errors:
            raise self._errors.pop(0)
        return await super().exec(command)


async def _run_retrying_exec(
    process: RetryingProcess,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> tuple[ExecResult, list[float], list[logging.LogRecord]]:
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))
    observed_waits: list[float] = []

    async def record_wait(seconds: float) -> None:
        observed_waits.append(seconds)

    retryer = cast(Any, DaytonaSandbox.exec).retry
    monkeypatch.setattr(retryer, "sleep", record_wait)
    with caplog.at_level(logging.WARNING, logger=daytona_module.__name__):
        result = await sandbox.exec(_SENSITIVE_RETRY_SENTINEL)

    return result, observed_waits, _retry_records(caplog)


async def _disable_pty_retry_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_wait(_seconds: float) -> None:
        pass

    for method in (
        DaytonaSandbox._create_pty_session,  # pyright: ignore[reportPrivateUsage]
        DaytonaSandbox._get_pty_create_session_info,  # pyright: ignore[reportPrivateUsage]
        DaytonaSandbox._connect_created_pty,  # pyright: ignore[reportPrivateUsage]
    ):
        monkeypatch.setattr(cast(Any, method).retry, "sleep", no_wait)


class FailedExecuteCommandProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        if self.attempts == 1:
            raise DaytonaError("Failed to execute command:")
        return await super().exec(command)


class ContainerIpProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        if self.attempts == 1:
            raise DaytonaError("Failed to execute command: failed to resolve container IP after 3 attempts")
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


class HangingProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class SlowSuccessProcess(Process):
    def __init__(self, delay: float) -> None:
        super().__init__()
        self.attempts = 0
        self._delay = delay

    async def exec(self, command: str) -> SimpleNamespace:
        self.attempts += 1
        await asyncio.sleep(self._delay)
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
        self.disconnected = False

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
        self.disconnected = True


class DisconnectedPtyHandle(PtyHandle):
    async def wait(self) -> None:
        raise RuntimeError("disconnected")


class ReconnectingProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.checked_session_id: str | None = None
        self.connected_session_id: str | None = None
        self.handles: list[PtyHandle] = []

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
        pty_size: PtySize,
    ) -> PtyHandle:
        assert id
        _assert_pty_create_config(envs, pty_size)
        handle = DisconnectedPtyHandle(on_data)
        self.handles.append(handle)
        return handle

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
        handle = PtyHandle(on_data)
        self.handles.append(handle)
        return handle


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
        pty_size: PtySize,
    ) -> FinishedPtyHandle:
        assert id
        _assert_pty_create_config(envs, pty_size)
        handle = FinishedPtyHandle(on_data, self._result)
        self.handles.append(handle)
        return handle

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
        pty_size: PtySize,
    ) -> PtyHandle:
        _assert_pty_create_config(envs, pty_size)
        raise DaytonaConnectionError("toolbox unreachable")


class CreatePtyConflictProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.create_attempts = 0
        self.reconnect_attempts = 0
        self.killed_session_ids: list[str] = []

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
        pty_size: PtySize,
    ) -> PtyHandle:
        _assert_pty_create_config(envs, pty_size)
        self.create_attempts += 1
        raise DaytonaConflictError("PTY session already exists")

    async def get_pty_session_info(self, session_id: str) -> SimpleNamespace:
        self.reconnect_attempts += 1
        raise AssertionError(f"must not inspect conflicting PTY session: {session_id}")

    async def connect_pty_session(
        self, session_id: str, on_data: Callable[[bytes], None | Awaitable[None]]
    ) -> PtyHandle:
        self.reconnect_attempts += 1
        raise AssertionError(f"must not connect conflicting PTY session: {session_id}")

    async def kill_pty_session(self, session_id: str) -> None:
        self.killed_session_ids.append(session_id)


class ReconcilingPtyProcess(Process):
    def __init__(
        self,
        create_outcomes: list[BaseException | None],
        *,
        get_errors: list[BaseException] | None = None,
        connect_errors: list[BaseException] | None = None,
    ) -> None:
        super().__init__()
        self.create_outcomes = list(create_outcomes)
        self.get_errors = list(get_errors or [])
        self.connect_errors = list(connect_errors or [])
        self.create_ids: list[str] = []
        self.create_envs: list[dict[str, str]] = []
        self.get_session_ids: list[str] = []
        self.connect_session_ids: list[str] = []
        self.handles: list[PtyHandle] = []
        self.killed_session_ids: list[str] = []
        self.status_cleanup_calls = 0
        self.info_id: str | None = None
        self.info_rows = _PTY_ROWS
        self.info_cols = _PTY_COLS
        self.info_env_overrides: dict[str, str] = {}
        self.info_env_omissions: set[str] = set()
        self.destroy_sandbox_on_get: Any | None = None
        self.destroy_sandbox_on_connect: Any | None = None

    async def exec(self, command: str) -> SimpleNamespace:
        if _unwrap_shell_command(command).startswith("rm -f "):
            self.status_cleanup_calls += 1
        return await super().exec(command)

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
        pty_size: PtySize,
    ) -> PtyHandle:
        _assert_pty_create_config(envs, pty_size)
        self.create_ids.append(id)
        self.create_envs.append(dict(envs))
        if not self.create_outcomes:
            raise AssertionError("unexpected extra PTY create attempt")
        outcome = self.create_outcomes.pop(0)
        if outcome is not None:
            raise outcome
        handle = PtyHandle(on_data)
        self.handles.append(handle)
        return handle

    async def get_pty_session_info(self, session_id: str) -> PtySessionInfo:
        self.get_session_ids.append(session_id)
        if self.destroy_sandbox_on_get is not None:
            self.destroy_sandbox_on_get.state = SandboxState.DESTROYED
        if self.get_errors:
            raise self.get_errors.pop(0)
        envs = dict(self.create_envs[0])
        envs.update(self.info_env_overrides)
        for key in self.info_env_omissions:
            envs.pop(key, None)
        return PtySessionInfo(
            active=False,
            cols=self.info_cols,
            created_at="2026-08-07T00:00:00Z",
            cwd="/home/daytona",
            envs=envs,
            id=self.info_id or session_id,
            lazy_start=False,
            rows=self.info_rows,
        )

    async def connect_pty_session(
        self,
        session_id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
    ) -> PtyHandle:
        self.connect_session_ids.append(session_id)
        if self.destroy_sandbox_on_connect is not None:
            self.destroy_sandbox_on_connect.state = SandboxState.DESTROYED
        if self.connect_errors:
            raise self.connect_errors.pop(0)
        handle = PtyHandle(on_data)
        self.handles.append(handle)
        return handle

    async def kill_pty_session(self, session_id: str) -> None:
        self.killed_session_ids.append(session_id)


class BlockingReconciliationProcess(ReconcilingPtyProcess):
    def __init__(self, blocked_stage: str) -> None:
        super().__init__([DaytonaConnectionError("create outcome lost"), DaytonaConflictError("already exists")])
        self.blocked_stage = blocked_stage
        self.block_started = asyncio.Event()

    async def get_pty_session_info(self, session_id: str) -> PtySessionInfo:
        if self.blocked_stage == "get":
            self.get_session_ids.append(session_id)
            self.block_started.set()
            await asyncio.Event().wait()
        return await super().get_pty_session_info(session_id)

    async def connect_pty_session(
        self,
        session_id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
    ) -> PtyHandle:
        if self.blocked_stage == "connect":
            self.connect_session_ids.append(session_id)
            self.block_started.set()
            await asyncio.Event().wait()
        return await super().connect_pty_session(session_id, on_data)


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
        pty_size: PtySize,
    ) -> PtyHandle:
        assert id
        _assert_pty_create_config(envs, pty_size)
        self.pty_envs = envs
        self.pty_handle = FloodingPtyHandle(on_data)
        return self.pty_handle


class BlockingPtyHandle(PtyHandle):
    async def wait(self) -> None:
        await asyncio.Event().wait()


class BlockingProcess(Process):
    handle_cls: type[PtyHandle] = BlockingPtyHandle

    def __init__(self) -> None:
        super().__init__()
        self.handle: PtyHandle | None = None
        self.killed_session_id: str | None = None

    async def create_pty_session(
        self,
        *,
        id: str,
        on_data: Callable[[bytes], None | Awaitable[None]],
        envs: dict[str, str],
        pty_size: PtySize,
    ) -> PtyHandle:
        assert id
        _assert_pty_create_config(envs, pty_size)
        self.handle = self.handle_cls(on_data)
        return self.handle

    async def kill_pty_session(self, session_id: str) -> None:
        self.killed_session_id = session_id


class RunningProcess(BlockingProcess):
    def __init__(self) -> None:
        super().__init__()
        self.status_probe_started = asyncio.Event()

    async def exec(self, command: str) -> SimpleNamespace:
        if _unwrap_shell_command(command).startswith("test -e "):
            self.status_probe_started.set()
            return SimpleNamespace(exit_code=1, result="")
        return await super().exec(command)


class DyingProcess(Process):
    def __init__(self) -> None:
        super().__init__()
        self.status_probe_commands = 0

    async def exec(self, command: str) -> SimpleNamespace:
        if _unwrap_shell_command(command).startswith("test -e "):
            self.status_probe_commands += 1
            raise DaytonaNotFoundError("sandbox not found")
        return await super().exec(command)


class StalledSendPtyHandle(BlockingPtyHandle):
    async def send_input(self, data: str) -> None:
        # Let the stty prologue through so the stall lands on the command send itself.
        if data.startswith("stty"):
            return
        await asyncio.Event().wait()


class StalledSendProcess(BlockingProcess):
    handle_cls = StalledSendPtyHandle


class StalledDisconnectPtyHandle(PtyHandle):
    async def disconnect(self) -> None:
        await super().disconnect()
        await asyncio.Event().wait()


class StalledDisconnectProcess(BlockingProcess):
    handle_cls = StalledDisconnectPtyHandle


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


class FlakyStreamingFiles(Files):
    def __init__(self, error: Exception) -> None:
        self._error = error
        self.attempts = 0

    async def download_file_stream(self, remote_path: str) -> Any:
        assert remote_path == "/tmp/result.txt"
        self.attempts += 1
        fails = self.attempts == 1

        async def chunks() -> Any:
            yield b"hello"
            if fails:
                raise self._error
            yield b" world"

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
        self.labels: dict[str, str] = {}
        self.created_at: str | None = None
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


class ErrorStateSandbox(InnerSandbox):
    state = SandboxState.ERROR

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        raise DaytonaError("sandbox failed to start")


class RecreatedInnerSandbox(InnerSandbox):
    id = "recreated-sandbox-id"


class DeleteWithoutPreparationSandbox(InnerSandbox):
    def __init__(self, state: SandboxState) -> None:
        super().__init__()
        self.state = state

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        raise AssertionError("sandbox deletion must not wait for startup")

    async def refresh_data(self) -> None:
        raise AssertionError("sandbox deletion must not refresh before deletion")

    async def set_autostop_interval(self, interval: int) -> None:
        raise AssertionError("sandbox deletion must not update autostop")


class HungStartInnerSandbox(InnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.start_attempts = 0

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        assert timeout == 0
        self.start_attempts += 1
        await asyncio.Event().wait()


class BareHtml502RefreshSandbox(InnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.refresh_attempts = 0

    async def refresh_data(self) -> None:
        self.refresh_attempts += 1
        if self.refresh_attempts == 1:
            raise DaytonaError("Failed to refresh sandbox data: <html><h1>502 Bad Gateway</h1></html>")
        await super().refresh_data()


class PersistentBareHtml502RefreshSandbox(InnerSandbox):
    async def refresh_data(self) -> None:
        raise DaytonaError("Failed to refresh sandbox data: <html><h1>502 Bad Gateway</h1></html>")


class StatusDuringFailedProbeProcess(Process):
    def __init__(self, *, publish_status_on_failure: bool = True, fail_reprobe: bool = False) -> None:
        super().__init__()
        self.publish_status_on_failure = publish_status_on_failure
        self.fail_reprobe = fail_reprobe
        self.status_exists = False
        self.status_probe_commands: list[str] = []

    async def exec(self, command: str) -> SimpleNamespace:
        evaluated_command = _unwrap_shell_command(command)
        if evaluated_command.startswith("test -e "):
            self.status_probe_commands.append(evaluated_command)
            if len(self.status_probe_commands) == 1:
                self.status_exists = self.publish_status_on_failure
                raise DaytonaError("Failed to execute command: initial status probe failed")
            assert evaluated_command == self.status_probe_commands[0]
            if self.fail_reprobe:
                raise DaytonaError("Failed to execute command: reconciliation status probe failed")
            return SimpleNamespace(exit_code=0 if self.status_exists else 1, result="")
        return await super().exec(command)


class HangingRefreshSandbox(InnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.refresh_attempts = 0

    async def refresh_data(self) -> None:
        self.refresh_attempts += 1
        await asyncio.Event().wait()


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


class DeleteConflictDaytonaClient(DaytonaClient):
    def __init__(self, sandbox: InnerSandbox) -> None:
        super().__init__(sandbox)
        self.delete_attempts = 0

    async def delete(self, sandbox: InnerSandbox) -> None:
        assert sandbox is self.sandbox
        self.delete_attempts += 1
        if self.delete_attempts == 1:
            raise DaytonaConflictError("Sandbox was modified by another operation")
        await super().delete(sandbox)


class DeleteNotFoundDaytonaClient(DaytonaClient):
    async def delete(self, sandbox: InnerSandbox) -> None:
        assert sandbox is self.sandbox
        raise DaytonaNotFoundError("sandbox not found")


class DaytonaRegionsClient:
    def __init__(self, regions: list[SimpleNamespace]) -> None:
        self.regions = regions
        self.list_attempts = 0
        self.close_attempts = 0

    def create_api_client(self, configuration: Any) -> "DaytonaRegionsClient":
        assert configuration.host == "https://daytona.example.test"
        assert configuration.access_token == "test-key"
        return self

    def create_organizations_api(self, api_client: object) -> "DaytonaRegionsClient":
        assert api_client is self
        return self

    async def __aenter__(self) -> "DaytonaRegionsClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        self.close_attempts += 1

    async def list_available_regions(self) -> list[SimpleNamespace]:
        self.list_attempts += 1
        return self.regions


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


def _inventory_provider(
    monkeypatch: pytest.MonkeyPatch,
    *,
    target: str,
    regions: list[SimpleNamespace],
) -> tuple[DaytonaSandboxProvider, DaytonaClient, DaytonaRegionsClient]:
    daytona = DaytonaClient(InnerSandbox())
    regions_client = DaytonaRegionsClient(regions)

    def create_daytona(*, config: DaytonaConfig) -> DaytonaClient:
        assert config.target == target
        return daytona

    monkeypatch.setattr(daytona_module, "AsyncDaytona", create_daytona)
    monkeypatch.setattr(daytona_module, "DaytonaApiClient", regions_client.create_api_client)
    monkeypatch.setattr(daytona_module, "OrganizationsApi", regions_client.create_organizations_api)
    provider = DaytonaSandboxProvider(
        DaytonaProviderConfig(
            DAYTONA_API_KEY="test-key",
            DAYTONA_API_URL="https://daytona.example.test",
            DAYTONA_TARGET=target,
        )
    )
    return provider, daytona, regions_client


def _request(
    name: str,
    resources: Resources | None = None,
    source: SandboxSource | None = None,
    labels: dict[str, str] | None = None,
    volumes: list[VolumeMount] | None = None,
) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=source or ImageSource(image="python:3.12"),
        resources=resources or Resources(vcpu=2, memory=4, disk=10),
        name=name,
        labels=labels or {},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
        volumes=volumes or [],
    )


def _usage_row(**updates: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "region_id": "us",
        "sandbox_class": SandboxClass.CONTAINER,
        "total_cpu_quota": 8,
        "current_cpu_usage": 2,
        "total_memory_quota": 32,
        "current_memory_usage": 4,
        "total_disk_quota": 100,
        "current_disk_usage": 25,
        "max_cpu_per_sandbox": None,
        "max_memory_per_sandbox": None,
        "max_disk_per_sandbox": None,
    }
    return SimpleNamespace(**(values | updates))


def _configured_daytona_provider(**updates: object) -> DaytonaSandboxProvider:
    values = {
        "DAYTONA_API_KEY": "secret",
        "DAYTONA_API_URL": "https://daytona.example/api",
        "DAYTONA_TARGET": "us",
        "DAYTONA_ORGANIZATION_ID": "org-1",
    }
    return DaytonaSandboxProvider(DaytonaProviderConfig.model_validate(values | updates))


def _admission_provider(
    monkeypatch: pytest.MonkeyPatch,
    result: Exception | SimpleNamespace,
    organization_id: str | None = "org-1",
    regions: Exception | list[SimpleNamespace] | None = None,
    region_requests: list[str] | None = None,
) -> tuple[DaytonaSandboxProvider, list[str]]:
    requested: list[str] = []

    async def observe(organization: str) -> SimpleNamespace:
        requested.append(organization)
        if isinstance(result, Exception):
            raise result
        return result

    async def get_organization(_organization: str) -> SimpleNamespace:
        raise AssertionError("organization details should not be fetched for admission")

    async def list_available_regions() -> list[SimpleNamespace]:
        if region_requests is not None:
            region_requests.append("list_available_regions")
        if isinstance(regions, Exception):
            raise regions
        return [SimpleNamespace(id="us", name="us")] if regions is None else regions

    def api_client(_value: object) -> nullcontext[None]:
        return nullcontext()

    def organizations_api(_value: object) -> SimpleNamespace:
        return SimpleNamespace(
            get_organization=get_organization,
            get_organization_usage_overview=observe,
            list_available_regions=list_available_regions,
        )

    monkeypatch.setattr("benchmark_service.sandbox.daytona.DaytonaApiClient", api_client)
    monkeypatch.setattr("benchmark_service.sandbox.daytona.OrganizationsApi", organizations_api)
    return _configured_daytona_provider(DAYTONA_ORGANIZATION_ID=organization_id), requested


def test_daytona_pool_id_is_stable_nonsecret_and_capacity_account_scoped() -> None:
    first = _configured_daytona_provider(DAYTONA_API_KEY="first-secret", DAYTONA_API_URL="https://d.test/")
    rotated = _configured_daytona_provider(DAYTONA_API_KEY="rotated-secret", DAYTONA_API_URL="https://d.test")
    pool_id = first.admission_pool_id

    assert pool_id is not None
    assert pool_id == rotated.admission_pool_id
    assert (
        pool_id
        == _configured_daytona_provider(
            DAYTONA_API_URL="https://d.test", DAYTONA_TARGET="canonical-region-id"
        ).admission_pool_id
    )
    assert (
        len(
            {
                pool_id,
                _configured_daytona_provider(DAYTONA_ORGANIZATION_ID="org-2").admission_pool_id,
                _configured_daytona_provider(DAYTONA_API_URL="https://other.example/api").admission_pool_id,
            }
        )
        == 3
    )
    assert _configured_daytona_provider(DAYTONA_ORGANIZATION_ID=None).admission_pool_id is None
    assert all(value not in pool_id for value in ("first-secret", "rotated-secret", "d.test"))


_ADMISSION_IMAGE = ImageSource(image="img")
_ADMISSION_RESOURCES = Resources(vcpu=2, memory=4, disk=10)


async def test_daytona_admission_resolves_named_target(monkeypatch: pytest.MonkeyPatch) -> None:
    region_requests: list[str] = []
    provider, requested = _admission_provider(
        monkeypatch,
        SimpleNamespace(region_usage=[_usage_row(region_id="region-id")]),
        regions=[SimpleNamespace(id="region-id", name="us")],
        region_requests=region_requests,
    )

    assert await provider.check_admission(_ADMISSION_IMAGE, _ADMISSION_RESOURCES)
    assert requested == ["org-1"]
    assert region_requests == ["list_available_regions"]


async def test_daytona_admission_accepts_target_id_without_region_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    region_requests: list[str] = []
    provider, requested = _admission_provider(
        monkeypatch,
        SimpleNamespace(region_usage=[_usage_row(region_id="us")]),
        regions=AssertionError("canonical target IDs should not require a region lookup"),
        region_requests=region_requests,
    )

    assert await provider.check_admission(_ADMISSION_IMAGE, _ADMISSION_RESOURCES)
    assert requested == ["org-1"]
    assert region_requests == []


@pytest.mark.parametrize(
    ("organization_id", "source", "resources", "usage_updates", "expected", "observed"),
    [
        (None, _ADMISSION_IMAGE, _ADMISSION_RESOURCES, {}, True, False),
        ("org-1", SnapshotSource(snapshot="snap"), _ADMISSION_RESOURCES, {}, True, False),
        ("org-1", _ADMISSION_IMAGE, Resources(vcpu=2, memory=4, disk=10, gpu=1), {}, True, False),
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
    provider, requested = _admission_provider(monkeypatch, overview, organization_id=organization_id)

    if expected is SandboxError:
        with pytest.raises(SandboxError):
            await provider.check_admission(source, resources)
    else:
        assert await provider.check_admission(source, resources) is expected
    assert requested == (["org-1"] if observed else [])


@pytest.mark.parametrize(
    "updates",
    [
        {"vcpu": 0},
        {"memory": 0},
        {"disk": 0},
        {"vcpu": -1},
        {"memory": -1},
        {"disk": -1},
    ],
)
def test_resources_require_positive_capacity(updates: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        Resources.model_validate({"vcpu": 2, "memory": 4, "disk": 10} | updates)


async def test_daytona_admission_treats_null_region_limits_as_unknown_without_organization_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, requested = _admission_provider(
        monkeypatch,
        SimpleNamespace(region_usage=[_usage_row()]),
    )

    assert await provider.check_admission(_ADMISSION_IMAGE, _ADMISSION_RESOURCES)
    assert requested == ["org-1"]


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


@pytest.mark.parametrize(
    ("regions", "expected"),
    [
        (TimeoutError(), True),
        (ApiException(status=503), True),
        (ApiException(status=401), SandboxError),
        ([], SandboxError),
    ],
)
async def test_daytona_admission_target_resolution_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    regions: Exception | list[SimpleNamespace],
    expected: bool | type[SandboxError],
) -> None:
    region_requests: list[str] = []
    provider, requested = _admission_provider(
        monkeypatch,
        SimpleNamespace(region_usage=[_usage_row(region_id="region-id")]),
        regions=regions,
        region_requests=region_requests,
    )
    decision = provider.check_admission(_ADMISSION_IMAGE, _ADMISSION_RESOURCES)

    if expected is SandboxError:
        with pytest.raises(SandboxError):
            await decision
    else:
        assert await decision is expected
    assert requested == ["org-1"]
    assert region_requests == ["list_available_regions"]


def test_sandbox_metadata_defaults_are_optional_for_existing_subclasses() -> None:
    sandbox = RecordingSandbox()

    assert sandbox.labels is None
    assert sandbox.created_at is None


def test_compose_sandbox_delegates_inventory_metadata() -> None:
    created_at = datetime(2026, 7, 24, 12, 30, tzinfo=UTC)
    outer = RecordingSandbox()
    outer.labels = {"run_id": "r1"}
    outer.created_at = created_at
    sandbox = ComposeSandbox(outer, ComposeSource(outer=ImageSource(image="docker:28.3.3-dind")))

    assert sandbox.labels == {"run_id": "r1"}
    assert sandbox.created_at == created_at


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

    await sandbox.exec(
        'container_id=$(docker compose ps -q main); cat /tmp/file | docker exec -i "$container_id" cat', timeout=60
    )

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
    "invalid_delay",
    [
        pytest.param(-1, id="negative"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive-infinity"),
        pytest.param(float("-inf"), id="negative-infinity"),
    ],
)
def test_daytona_retry_after_rejects_negative_and_non_finite_values(invalid_delay: float) -> None:
    exc = DaytonaRateLimitError("rate limited", headers={"retry-after": invalid_delay})

    assert daytona_retry_after_seconds(exc) is None


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


def test_daytona_throttled_unauthorized_errors_are_transient() -> None:
    exc = DaytonaError(
        "Failed to execute command: unauthorized: authentication failed: Bearer token validation error: "
        "ThrottlerException: Too Many Requests",
        status_code=401,
    )

    assert _is_transient_daytona_error(exc)
    assert not _is_ambiguous_pty_create_error(exc)


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
def test_daytona_retryable_http_statuses_are_transient(status_code: int) -> None:
    assert _is_transient_daytona_error(DaytonaError("provider request failed", status_code=status_code))


def test_daytona_retryable_client_response_status_is_transient() -> None:
    assert _is_transient_daytona_error(_client_response_error(503, "service unavailable"))


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 409])
def test_daytona_permanent_http_statuses_are_not_transient(status_code: int) -> None:
    assert not _is_transient_daytona_error(DaytonaError("provider request failed", status_code=status_code))


def test_daytona_not_found_classification_uses_structured_code_and_status_fallback() -> None:
    assert _is_not_found_error(DaytonaError("sandbox missing", code="NOT_FOUND"))
    assert _is_not_found_error(DaytonaError("sandbox missing", status_code=404))


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


async def test_daytona_exec_logs_each_rate_limit_retry(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive_message = f"rate limited {_SENSITIVE_RETRY_SENTINEL}"
    sensitive_code = f"CODE_{_SENSITIVE_RETRY_SENTINEL}"
    errors: list[BaseException] = [
        DaytonaRateLimitError(sensitive_message, 429, {"retry-after": "nan"}, sensitive_code, SOURCE_API),
        DaytonaRateLimitError(
            sensitive_message, 429, {"retry-after-sandbox-create": "0.25"}, sensitive_code, SOURCE_PROXY
        ),
        DaytonaRateLimitError(
            sensitive_message, 429, {"retry-after-sandbox-create": "0.5"}, sensitive_code, SOURCE_DAEMON
        ),
    ]
    process = RetryingProcess(errors)

    result, observed_waits, records = await _run_retrying_exec(process, monkeypatch, caplog)

    assert result.exit_code == 0
    assert process.attempts == 4
    assert observed_waits == [1.0, 0.25, 0.5]
    assert len(records) == 3

    expected_messages = [
        "daytona.retry sandbox_provider=daytona daytona_step=exec daytona_retry_attempt=1 "
        "daytona_retry_delay_s=1.0 daytona_retry_category=rate_limit daytona_status_code=429 daytona_source=api",
        "daytona.retry sandbox_provider=daytona daytona_step=exec daytona_retry_attempt=2 "
        "daytona_retry_delay_s=0.25 daytona_retry_category=rate_limit daytona_status_code=429 daytona_source=proxy",
        "daytona.retry sandbox_provider=daytona daytona_step=exec daytona_retry_attempt=3 "
        "daytona_retry_delay_s=0.5 daytona_retry_category=rate_limit daytona_status_code=429 daytona_source=daemon",
    ]
    for attempt, (record, delay, source, expected_message) in enumerate(
        zip(records, observed_waits, ("api", "proxy", "daemon"), expected_messages, strict=True),
        start=1,
    ):
        expected_fields = {
            "sandbox_provider": "daytona",
            "daytona_step": "exec",
            "daytona_retry_attempt": attempt,
            "daytona_retry_delay_s": delay,
            "daytona_retry_category": "rate_limit",
            "daytona_status_code": 429,
            "daytona_source": source,
        }
        _assert_safe_retry_record(record, expected_message, expected_fields)


@pytest.mark.parametrize(
    ("provider_error", "status_code"),
    [
        pytest.param(
            DaytonaError(
                f"provider message {_SENSITIVE_RETRY_SENTINEL}",
                status_code=502,
                headers={"x-sensitive": _SENSITIVE_RETRY_SENTINEL},
                code=f"CODE_{_SENSITIVE_RETRY_SENTINEL}",
                source=_SENSITIVE_RETRY_SENTINEL,
            ),
            502,
            id="daytona-error",
        ),
        pytest.param(
            _client_response_error(
                503,
                f"provider message {_SENSITIVE_RETRY_SENTINEL}",
                url=f"https://daytona.example.test/{_SENSITIVE_RETRY_SENTINEL}",
                headers={"x-sensitive": _SENSITIVE_RETRY_SENTINEL},
            ),
            503,
            id="client-response-error",
        ),
    ],
)
async def test_daytona_exec_logs_typed_status_without_sensitive_details(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    provider_error: DaytonaError | ClientResponseError,
    status_code: int,
) -> None:
    process = RetryingProcess([provider_error])

    result, observed_waits, records = await _run_retrying_exec(process, monkeypatch, caplog)

    assert result.exit_code == 0
    assert process.attempts == 2
    assert len(observed_waits) == 1
    expected_fields = {
        "sandbox_provider": "daytona",
        "daytona_step": "exec",
        "daytona_retry_attempt": 1,
        "daytona_retry_delay_s": observed_waits[0],
        "daytona_retry_category": "transient",
        "daytona_status_code": status_code,
    }
    expected_message = (
        "daytona.retry sandbox_provider=daytona daytona_step=exec daytona_retry_attempt=1 "
        f"daytona_retry_delay_s={observed_waits[0]} daytona_retry_category=transient "
        f"daytona_status_code={status_code}"
    )
    _assert_safe_retry_record(records[0], expected_message, expected_fields)


async def test_daytona_retry_logging_failure_preserves_retry_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    process = RetryingProcess([DaytonaError("provider unavailable", status_code=503)])
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))
    observed_waits: list[float] = []
    warning_calls = 0

    async def record_wait(seconds: float) -> None:
        observed_waits.append(seconds)

    def fail_warning(_message: str, **_kwargs: object) -> None:
        nonlocal warning_calls
        warning_calls += 1
        raise RuntimeError("logging unavailable")

    retryer = cast(Any, DaytonaSandbox.exec).retry
    monkeypatch.setattr(retryer, "sleep", record_wait)
    monkeypatch.setattr(daytona_module.logger, "warning", fail_warning)

    result = await sandbox.exec("pytest")

    assert result.exit_code == 0
    assert process.attempts == 2
    assert warning_calls == 1
    assert len(observed_waits) == 1


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


async def test_daytona_exec_with_timeout_bounds_hanging_toolbox_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """A toolbox call that never returns must surface as a retryable error rather than hang."""
    inner = InnerSandbox()
    process = HangingProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.05)

    _skip_retry_sleep(monkeypatch, DaytonaSandbox.exec)

    with pytest.raises(SandboxConnectionError, match="timed out"):
        await asyncio.wait_for(sandbox.exec("pytest", timeout=0.01), timeout=5)

    assert process.attempts == 6


async def test_daytona_exec_does_not_cap_untimed_long_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """An untimed command must outlive the toolbox bound.

    Capping it would abort long installs and re-run non-idempotent commands on the retry.
    """
    inner = InnerSandbox()
    process = SlowSuccessProcess(delay=0.2)
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.02)

    result = await asyncio.wait_for(sandbox.exec("sleep 999"), timeout=5)

    assert result.exit_code == 0
    assert process.attempts == 1


async def test_daytona_control_exec_bounds_hanging_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Internal probes stay bounded even though an untimed public exec does not."""
    inner = InnerSandbox()
    process = HangingProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.05)

    _skip_retry_sleep(monkeypatch, DaytonaSandbox._control_exec)  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(SandboxConnectionError, match="timed out"):
        await asyncio.wait_for(sandbox._control_exec("test -e /tmp/x"), timeout=5)  # pyright: ignore[reportPrivateUsage]

    assert process.attempts == 6


async def test_daytona_check_sandbox_alive_bounds_hanging_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """The liveness check runs inside the PTY poll loop, so a stalled refresh must not hang it."""
    inner = HangingRefreshSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.05)

    _skip_retry_sleep(monkeypatch, DaytonaSandbox._check_sandbox_alive)  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(SandboxConnectionError, match="timed out"):
        await asyncio.wait_for(sandbox._check_sandbox_alive(), timeout=5)  # pyright: ignore[reportPrivateUsage]

    assert inner.refresh_attempts == 6


async def test_daytona_exec_retries_container_ip_resolution_failures() -> None:
    inner = InnerSandbox()
    process = ContainerIpProcess()
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


@pytest.mark.parametrize(
    "stream_error",
    [
        ConnectionResetError("connection reset"),
        ClientPayloadError("response truncated"),
    ],
)
async def test_daytona_download_retries_stream_errors(
    monkeypatch: pytest.MonkeyPatch,
    stream_error: Exception,
) -> None:
    """Transport errors raised mid-stream escape the SDK untyped, so they need their own retry."""
    inner = InnerSandbox()
    files = FlakyStreamingFiles(stream_error)
    inner.fs = files
    sandbox = DaytonaSandbox(cast(Any, inner))

    _skip_retry_sleep(monkeypatch, DaytonaSandbox.download_file)

    result = await sandbox.download_file("/tmp/result.txt")

    assert result == b"hello world"
    assert files.attempts == 2


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


async def test_daytona_command_publishes_status_atomically_and_cleans_temp() -> None:
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert [chunk async for chunk in sandbox.command("printf hello")] == ["hello"]

    assert inner.process.pty_handle is not None
    publish_input = inner.process.pty_handle.inputs[1]
    write_command, move_command = publish_input.split(" && mv ", maxsplit=1)
    temp_path = shlex.split(write_command.rsplit(">", maxsplit=1)[1])[0]
    move_args = shlex.split(move_command.removesuffix("; exit\n"))
    assert move_args == [temp_path, temp_path.removesuffix(".tmp")]

    assert inner.process.command is not None
    cleanup_args = shlex.split(_unwrap_shell_command(inner.process.command))
    assert cleanup_args == ["rm", "-f", temp_path.removesuffix(".tmp"), temp_path]


async def test_daytona_command_uses_native_process_environment() -> None:
    secret = "value with spaces; $(touch /tmp/leaked)"
    inner = InnerSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    output = [chunk async for chunk in sandbox.command("printf hello", env_vars={"AGENT_SECRET": secret})]

    assert output == ["hello"]
    assert inner.process.pty_envs is not None
    assert inner.process.pty_envs["TERM"] == "dumb"
    assert inner.process.pty_envs["LANG"] == "C.UTF-8"
    assert inner.process.pty_envs["AGENT_SECRET"] == secret
    marker = inner.process.pty_envs[_PTY_CREATE_MARKER_ENV]
    assert marker
    assert inner.process.pty_size == PtySize(rows=_PTY_ROWS, cols=_PTY_COLS)
    assert inner.process.pty_handle is not None
    assert inner.process.pty_handle.inputs[0].startswith("stty -echo; unset ")
    assert all(secret not in data and marker not in data for data in inner.process.pty_handle.inputs)


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


async def test_daytona_command_bounds_stalled_pty_send(monkeypatch: pytest.MonkeyPatch) -> None:
    """A PTY that never accepts input must fail the command and still release the session."""
    inner = InnerSandbox()
    process = StalledSendProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.05)

    with pytest.raises(SandboxConnectionError, match="timed out"):
        async with asyncio.timeout(5):
            _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert process.handle is not None
    assert process.handle.disconnected is True
    assert process.killed_session_id is not None


async def test_daytona_command_continues_cleanup_when_disconnect_stalls(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stalled disconnect must not strand cleanup after the command already succeeded."""
    inner = InnerSandbox()
    process = StalledDisconnectProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.05)

    async with asyncio.timeout(5):
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
    command_inputs = [data for handle in process.handles for data in handle.inputs if "mkdir -p " in data]
    assert len(command_inputs) == 1
    assert command_inputs[0] in process.handles[0].inputs
    assert process.handles[1].inputs == []


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


async def test_daytona_command_returns_completed_status_without_health_refresh() -> None:
    inner = PersistentBareHtml502RefreshSandbox()
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert [chunk async for chunk in sandbox.command("printf hello")] == ["hello"]
    assert inner.refresh_count == 0


async def test_daytona_command_does_not_refresh_sandbox_state_while_pty_is_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daytona_module, "_PTY_STATUS_POLL_SECONDS", 0)
    inner = InnerSandbox()
    process = RunningProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    stream = sandbox.command("printf hello")
    assert await anext(stream) == "hello"
    await asyncio.wait_for(process.status_probe_started.wait(), timeout=1)
    assert inner.refresh_count == 0
    await stream.aclose()


async def test_daytona_command_reports_sandbox_death_from_control_exec_failure() -> None:
    inner = InnerSandbox()
    process = DyingProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert process.status_probe_commands == 2
    assert inner.refresh_count == 0


async def test_daytona_command_rechecks_status_after_initial_probe_failure() -> None:
    inner = InnerSandbox()
    process = StatusDuringFailedProbeProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert [chunk async for chunk in sandbox.command("printf hello")] == ["hello"]
    assert len(process.status_probe_commands) == 2


@pytest.mark.parametrize("fail_reprobe", [False, True], ids=["status-absent", "reprobe-failed"])
async def test_daytona_command_preserves_initial_probe_error_without_completed_status(fail_reprobe: bool) -> None:
    inner = InnerSandbox()
    process = StatusDuringFailedProbeProcess(publish_status_on_failure=False, fail_reprobe=fail_reprobe)
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError, match="initial status probe failed"):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert len(process.status_probe_commands) == 2
    assert inner.refresh_count == 0


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
    assert inner.refresh_count == 1


async def test_daytona_command_reconnects_without_per_poll_sandbox_health_check() -> None:
    inner = InnerSandbox()
    inner.process = ReconnectingProcess()
    inner.state = SandboxState.DESTROYED
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert [chunk async for chunk in sandbox.command("printf hello")] == ["hello"]

    assert inner.refresh_count == 0


async def test_daytona_command_checks_sandbox_health_after_pty_create_failure() -> None:
    inner = InnerSandbox()
    inner.process = CreatePtyFailureProcess()
    inner.state = SandboxState.DESTROYED
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 1


@pytest.mark.parametrize("inner_type", [InnerSandbox, BareHtml502RefreshSandbox])
async def test_daytona_command_keeps_pty_create_conflicts_terminal(inner_type: type[InnerSandbox]) -> None:
    inner = inner_type()
    process = CreatePtyConflictProcess()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError, match="PTY session already exists") as exc_info:
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert type(exc_info.value) is SandboxError
    assert process.create_attempts == 1
    assert process.reconnect_attempts == 0
    assert process.pty_handle is None
    assert process.killed_session_ids == []
    assert process.command is None
    assert inner.refresh_count == 0
    if isinstance(inner, BareHtml502RefreshSandbox):
        assert inner.refresh_attempts == 0


@pytest.mark.parametrize(
    ("create_outcomes", "expected_create_attempts"),
    [
        pytest.param(
            [DaytonaConnectionError("connection lost"), DaytonaConflictError("already exists")],
            2,
            id="connection",
        ),
        pytest.param(
            [DaytonaTimeoutError("timed out"), DaytonaConflictError("already exists")],
            2,
            id="timeout",
        ),
        pytest.param(
            [DaytonaError("server disconnected"), DaytonaConflictError("already exists")],
            2,
            id="flattened-transport-message",
        ),
        pytest.param(
            [
                _daytona_error_with_cause("create failed", ClientConnectionError("tcp reset")),
                DaytonaConflictError("already exists"),
            ],
            2,
            id="wrapped-connection-cause",
        ),
        *[
            pytest.param(
                [DaytonaError("server failed", status_code=status), DaytonaConflictError("already exists")],
                2,
                id=f"server-{status}",
            )
            for status in (408, 500, 502, 503, 504)
        ],
        pytest.param(
            [
                DaytonaError("server failed", status_code=500),
                DaytonaRateLimitError("rate limited", 429, {"retry-after": "0"}, "RATE_LIMIT", SOURCE_API),
                DaytonaConflictError("already exists"),
            ],
            3,
            id="ambiguity-survives-rate-limit",
        ),
    ],
)
async def test_daytona_command_reconciles_ambiguous_pty_create_once(
    monkeypatch: pytest.MonkeyPatch,
    create_outcomes: list[BaseException | None],
    expected_create_attempts: int,
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    process = ReconcilingPtyProcess(create_outcomes)
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert [chunk async for chunk in sandbox.command("printf unique-command")] == ["hello"]

    assert len(process.create_ids) == expected_create_attempts
    assert len(set(process.create_ids)) == 1
    assert len({_env[_PTY_CREATE_MARKER_ENV] for _env in process.create_envs}) == 1
    assert process.get_session_ids == [process.create_ids[0]]
    assert process.connect_session_ids == [process.create_ids[0]]
    command_inputs = [data for handle in process.handles for data in handle.inputs if "unique-command" in data]
    assert len(command_inputs) == 1
    assert process.killed_session_ids == [process.create_ids[0]]
    assert process.status_cleanup_calls == 1


@pytest.mark.parametrize(
    "create_outcomes",
    [
        pytest.param(
            [DaytonaError("rate limited", status_code=429), DaytonaConflictError("already exists")],
            id="generic-rate-limit-before-typed-conflict",
        ),
        pytest.param(
            [DaytonaConnectionError("connection lost"), DaytonaError("already exists")],
            id="message-only-conflict-after-ambiguity",
        ),
        pytest.param(
            [DaytonaConnectionError("connection lost"), DaytonaError("plain conflict", status_code=409)],
            id="untyped-409-after-ambiguity",
        ),
    ],
)
async def test_daytona_command_does_not_reconcile_nonqualifying_conflicts(
    monkeypatch: pytest.MonkeyPatch,
    create_outcomes: list[BaseException | None],
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    process = ReconcilingPtyProcess(create_outcomes)
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert process.get_session_ids == []
    assert process.connect_session_ids == []
    assert process.handles == []
    assert process.killed_session_ids == []
    assert process.status_cleanup_calls == 0


@pytest.mark.parametrize(
    "mismatch",
    ["missing-marker", "wrong-marker", "wrong-environment", "wrong-id", "wrong-rows", "wrong-cols"],
)
async def test_daytona_command_rejects_mismatched_pty_create_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    mismatch: str,
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    process = ReconcilingPtyProcess([DaytonaConnectionError("connection lost"), DaytonaConflictError("already exists")])
    if mismatch == "missing-marker":
        process.info_env_omissions.add(_PTY_CREATE_MARKER_ENV)
    elif mismatch == "wrong-marker":
        process.info_env_overrides[_PTY_CREATE_MARKER_ENV] = "wrong-marker"
    elif mismatch == "wrong-environment":
        process.info_env_overrides["EXPECTED_ENV"] = "wrong-value"
    elif mismatch == "wrong-id":
        process.info_id = "unrelated-session"
    elif mismatch == "wrong-rows":
        process.info_rows += 1
    else:
        process.info_cols += 1
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))
    sensitive_value = "sensitive-environment-value"

    with caplog.at_level(logging.WARNING, logger=daytona_module.__name__), pytest.raises(SandboxError) as exc_info:
        _ = [chunk async for chunk in sandbox.command("printf hello", env_vars={"EXPECTED_ENV": sensitive_value})]

    marker = process.create_envs[0][_PTY_CREATE_MARKER_ENV]
    diagnostic_data = repr((str(exc_info.value), [(record.getMessage(), vars(record)) for record in caplog.records]))
    assert marker not in diagnostic_data
    assert sensitive_value not in diagnostic_data
    assert "wrong-marker" not in diagnostic_data
    assert process.get_session_ids == [process.create_ids[0]]
    assert process.connect_session_ids == []
    assert process.handles == []
    assert process.killed_session_ids == []
    assert process.status_cleanup_calls == 0


async def test_daytona_command_keeps_marker_operation_scoped_and_unsets_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    process = ReconcilingPtyProcess(
        [DaytonaConnectionError("first lost"), None, DaytonaConnectionError("second lost"), None]
    )
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    for command in ("printf first-command", "printf second-command"):
        assert [
            chunk
            async for chunk in sandbox.command(
                command,
                env_vars={_PTY_CREATE_MARKER_ENV: "caller-value", "EXPECTED_ENV": "expected"},
            )
        ] == ["hello"]

    first_markers = {envs[_PTY_CREATE_MARKER_ENV] for envs in process.create_envs[:2]}
    second_markers = {envs[_PTY_CREATE_MARKER_ENV] for envs in process.create_envs[2:]}
    assert len(first_markers) == len(second_markers) == 1
    assert first_markers != second_markers
    assert "caller-value" not in first_markers | second_markers
    assert process.get_session_ids == []
    assert process.connect_session_ids == []
    for handle in process.handles:
        assert handle.inputs[0].startswith(f"stty -echo; unset {_PTY_CREATE_MARKER_ENV}")
        marker_values = first_markers | second_markers
        assert all(marker not in data for marker in marker_values for data in handle.inputs)
    command_inputs = [data for handle in process.handles for data in handle.inputs if "mkdir -p " in data]
    assert len(command_inputs) == 2
    assert len(process.killed_session_ids) == 2
    assert process.status_cleanup_calls == 2


@pytest.mark.parametrize("retry_stage", ["get", "connect"])
async def test_daytona_command_retries_only_the_active_reconciliation_phase(
    monkeypatch: pytest.MonkeyPatch,
    retry_stage: str,
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    process = ReconcilingPtyProcess(
        [DaytonaConnectionError("create lost"), DaytonaConflictError("already exists")],
        get_errors=[DaytonaConnectionError("get lost")] if retry_stage == "get" else None,
        connect_errors=[DaytonaConnectionError("connect lost")] if retry_stage == "connect" else None,
    )
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert [chunk async for chunk in sandbox.command("printf phase-command")] == ["hello"]

    assert len(process.create_ids) == 2
    assert len(process.get_session_ids) == (2 if retry_stage == "get" else 1)
    assert len(process.connect_session_ids) == (2 if retry_stage == "connect" else 1)
    command_inputs = [data for handle in process.handles for data in handle.inputs if "phase-command" in data]
    assert len(command_inputs) == 1
    assert len(process.killed_session_ids) == 1


@pytest.mark.parametrize(
    (
        "failure_stage",
        "provider_error",
        "destroy_sandbox",
        "expected_type",
        "expected_message",
        "expected_kills",
    ),
    [
        pytest.param(
            "get",
            DaytonaError("invalid get", status_code=400),
            False,
            SandboxError,
            "invalid get",
            0,
            id="get-permanent",
        ),
        pytest.param(
            "get",
            DaytonaNotFoundError("PTY missing"),
            False,
            SandboxError,
            "PTY session disappeared during create reconciliation",
            0,
            id="get-not-found",
        ),
        pytest.param(
            "get",
            DaytonaNotFoundError("PTY missing"),
            True,
            SandboxNotFoundError,
            "Sandbox not found: name=sandbox-name, id=sandbox-id.",
            0,
            id="get-sandbox-destroyed",
        ),
        pytest.param(
            "connect",
            DaytonaError("invalid connect", status_code=400),
            False,
            SandboxError,
            "invalid connect",
            1,
            id="connect-permanent",
        ),
        pytest.param(
            "connect",
            DaytonaNotFoundError("PTY missing"),
            False,
            SandboxError,
            "PTY session disappeared before create reconciliation connected",
            1,
            id="connect-not-found",
        ),
        pytest.param(
            "connect",
            DaytonaNotFoundError("PTY missing"),
            True,
            SandboxNotFoundError,
            "Sandbox not found: name=sandbox-name, id=sandbox-id.",
            1,
            id="connect-sandbox-destroyed",
        ),
    ],
)
async def test_daytona_command_applies_reconciliation_ownership_to_terminal_failures(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    provider_error: BaseException,
    destroy_sandbox: bool,
    expected_type: type[SandboxError],
    expected_message: str,
    expected_kills: int,
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    process = ReconcilingPtyProcess(
        [DaytonaConnectionError("create lost"), DaytonaConflictError("already exists")],
        get_errors=[provider_error] if failure_stage == "get" else None,
        connect_errors=[provider_error] if failure_stage == "connect" else None,
    )
    inner = InnerSandbox()
    inner.process = process
    if destroy_sandbox:
        if failure_stage == "get":
            process.destroy_sandbox_on_get = inner
        else:
            process.destroy_sandbox_on_connect = inner
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxError) as exc_info:
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert type(exc_info.value) is expected_type
    assert expected_message in str(exc_info.value)
    assert all(handle.inputs == [] for handle in process.handles)
    assert len(process.killed_session_ids) == expected_kills
    assert process.status_cleanup_calls == expected_kills


@pytest.mark.parametrize(
    ("blocked_stage", "operation", "expected_kills"),
    [
        ("get", "process.get_pty_session_info", 0),
        ("connect", "process.connect_pty_session", 1),
    ],
)
async def test_daytona_command_bounds_stalled_pty_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
    blocked_stage: str,
    operation: str,
    expected_kills: int,
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.01)
    process = BlockingReconciliationProcess(blocked_stage)
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))
    generator = sandbox.command("printf hello")

    with pytest.raises(SandboxConnectionError, match=rf"Daytona {operation} timed out"):
        await asyncio.wait_for(anext(generator), timeout=2)
    await generator.aclose()

    assert all(handle.inputs == [] for handle in process.handles)
    assert len(process.get_session_ids) == (6 if blocked_stage == "get" else 1)
    assert len(process.connect_session_ids) == (0 if blocked_stage == "get" else 6)
    assert len(process.killed_session_ids) == expected_kills
    assert process.status_cleanup_calls == expected_kills


@pytest.mark.parametrize(("blocked_stage", "expected_kills"), [("get", 0), ("connect", 1)])
async def test_daytona_command_cancellation_respects_reconciliation_ownership(
    monkeypatch: pytest.MonkeyPatch,
    blocked_stage: str,
    expected_kills: int,
) -> None:
    await _disable_pty_retry_waits(monkeypatch)
    process = BlockingReconciliationProcess(blocked_stage)
    inner = InnerSandbox()
    inner.process = process
    sandbox = DaytonaSandbox(cast(Any, inner))
    generator = sandbox.command("printf hello")
    task = asyncio.create_task(anext(generator))

    await asyncio.wait_for(process.block_started.wait(), timeout=1)
    assert all(handle.inputs == [] for handle in process.handles)
    assert process.killed_session_ids == []
    assert process.status_cleanup_calls == 0

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await generator.aclose()

    assert all(handle.inputs == [] for handle in process.handles)
    assert len(process.killed_session_ids) == expected_kills
    assert process.status_cleanup_calls == expected_kills


async def test_daytona_command_checks_sandbox_health_after_reconnect_failure() -> None:
    inner = InnerSandbox()
    inner.process = CrashingReconnectProcess(inner)
    sandbox = DaytonaSandbox(cast(Any, inner))

    with pytest.raises(SandboxNotFoundError, match="Sandbox not found: name=sandbox-name, id=sandbox-id\\."):
        _ = [chunk async for chunk in sandbox.command("printf hello")]

    assert inner.refresh_count == 1


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


def test_daytona_sandbox_exposes_inventory_metadata() -> None:
    inner = InnerSandbox()
    inner.labels = {"Benchmark": "vcb", "clean-up": "true"}
    inner.created_at = "2026-07-24T05:30:00-07:00"
    sandbox = DaytonaSandbox(cast(Any, inner))

    assert sandbox.labels == {"Benchmark": "vcb", "clean-up": "true"}
    assert sandbox.created_at == datetime(2026, 7, 24, 12, 30, tzinfo=UTC)


def test_daytona_sandbox_allows_missing_creation_timestamp() -> None:
    inner = InnerSandbox()

    assert DaytonaSandbox(cast(Any, inner)).created_at is None


@pytest.mark.parametrize("created_at", ["not-a-timestamp", "2026-07-24T12:30:00"])
def test_daytona_sandbox_rejects_invalid_creation_timestamp(created_at: str) -> None:
    inner = InnerSandbox()
    inner.created_at = created_at

    with pytest.raises(SandboxError):
        DaytonaSandbox(cast(Any, inner))


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


async def test_daytona_provider_create_conflict_bounds_hung_sandbox_start(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recovering a conflicting sandbox must not block forever on a start that never lands."""
    inner = HungStartInnerSandbox()

    class ConflictingHungStartClient(DaytonaClient):
        async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
            raise DaytonaError(f"Sandbox with name {inner.name} already exists")

    daytona = ConflictingHungStartClient(inner)

    monkeypatch.setattr(daytona_module, "_SANDBOX_START_TIMEOUT_SECONDS", 0.05)

    _skip_retry_sleep(monkeypatch, DaytonaSandboxProvider.create_sandbox)

    with pytest.raises(SandboxConnectionError, match="timed out"):
        await asyncio.wait_for(_provider(daytona).create_sandbox(_request(inner.name)), timeout=5)

    assert inner.start_attempts == 6


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


class RecordingVolumeService:
    def __init__(self, results: list[Any]) -> None:
        self.results = results
        self.calls: list[tuple[str, bool]] = []

    async def get(self, name: str, create: bool = False) -> Any:
        self.calls.append((name, create))
        result = self.results.pop(0) if len(self.results) > 1 else self.results[0]
        if isinstance(result, BaseException):
            raise result
        return result


class VolumeCapturingDaytonaClient(CapturingCreateDaytonaClient):
    def __init__(self, sandbox: InnerSandbox, volume_results: list[Any]) -> None:
        super().__init__(sandbox)
        self.volume = RecordingVolumeService(volume_results)


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


@pytest.mark.parametrize(
    "state",
    [
        SandboxState.CREATING,
        SandboxState.STARTED,
        SandboxState.BUILDING_SNAPSHOT,
        SandboxState.ERROR,
        SandboxState.STOPPED,
        SandboxState.ARCHIVED,
    ],
)
async def test_daytona_provider_delete_does_not_prepare_sandbox(state: SandboxState) -> None:
    inner = DeleteWithoutPreparationSandbox(state)
    daytona = DaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert daytona.deleted is True


async def test_daytona_provider_delete_retries_state_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = InnerSandbox()
    daytona = DeleteConflictDaytonaClient(inner)
    _skip_retry_sleep(monkeypatch, DaytonaSandboxProvider.delete_sandbox)

    await _provider(daytona).delete_sandbox(inner.name)

    assert daytona.delete_attempts == 2
    assert daytona.deleted is True


async def test_daytona_provider_delete_treats_final_not_found_as_absent() -> None:
    inner = InnerSandbox()
    daytona = DeleteNotFoundDaytonaClient(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert daytona.deleted is False


async def test_daytona_provider_get_retries_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = InnerSandbox()
    daytona = UnexpectedGetDaytonaClient(inner)

    _skip_retry_sleep(monkeypatch, DaytonaSandboxProvider.get_sandbox)

    sandbox = await _provider(daytona).get_sandbox(inner.name)

    assert sandbox.id == inner.id
    assert daytona.get_attempts == 2


async def test_daytona_provider_delete_retries_unexpected_remove_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = InnerSandbox()
    daytona = UnexpectedRemoveDaytonaClient(inner)

    _skip_retry_sleep(monkeypatch, DaytonaSandboxProvider.delete_sandbox)

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


def test_sandbox_query_rejects_naive_creation_cutoff() -> None:
    with pytest.raises(ValueError):
        SandboxQuery(labels={"Benchmark": "vcb"}, created_at_lte=datetime(2026, 7, 24, 12, 30))


@pytest.mark.parametrize("configured_target", ["configured-target", "configured-target_a1b2"])
async def test_daytona_provider_lists_sandboxes_with_canonical_target(
    monkeypatch: pytest.MonkeyPatch,
    configured_target: str,
) -> None:
    cutoff = datetime(2026, 7, 24, 12, 30, tzinfo=UTC)
    canonical_target = "configured-target_a1b2"
    provider, daytona, regions_client = _inventory_provider(
        monkeypatch,
        target=configured_target,
        regions=[
            SimpleNamespace(id="configured-target", name="another-target"),
            SimpleNamespace(id=canonical_target, name="configured-target"),
        ],
    )
    query = SandboxQuery(labels={"Benchmark": "vcb"}, page_size=25, created_at_lte=cutoff)
    resolution_started = asyncio.Event()
    release_resolution = asyncio.Event()
    list_available_regions = regions_client.list_available_regions

    async def gated_list_available_regions() -> list[SimpleNamespace]:
        resolution_started.set()
        await release_resolution.wait()
        return await list_available_regions()

    async def list_once(started: asyncio.Event | None = None) -> list[DaytonaSandbox]:
        if started is not None:
            started.set()
        return [sandbox async for sandbox in provider.list_sandboxes(query)]

    monkeypatch.setattr(regions_client, "list_available_regions", gated_list_available_regions)
    first_list = asyncio.create_task(list_once())
    await resolution_started.wait()
    second_started = asyncio.Event()
    second_list = asyncio.create_task(list_once(second_started))
    await second_started.wait()
    release_resolution.set()
    sandboxes = await asyncio.gather(first_list, second_list)

    assert [[sandbox.id for sandbox in result] for result in sandboxes] == [["sandbox-id"], ["sandbox-id"]]
    assert regions_client.list_attempts == 1
    assert regions_client.close_attempts == 1
    assert daytona.listed_query is not None
    assert daytona.listed_query.labels == {"Benchmark": "vcb"}
    assert daytona.listed_query.targets == [canonical_target]
    assert daytona.listed_query.limit == 25
    assert daytona.listed_query.created_at_before == cutoff


async def test_daytona_target_resolution_preserves_provider_error(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, _, regions_client = _inventory_provider(monkeypatch, target="configured-target", regions=[])
    error = ApiException(status=429, reason="rate limited")
    error.headers = {"retry-after": "3"}

    async def fail() -> list[SimpleNamespace]:
        raise error

    monkeypatch.setattr(regions_client, "list_available_regions", fail)
    with pytest.raises(DaytonaRateLimitError) as exc_info:
        await provider._resolve_target_id()  # pyright: ignore[reportPrivateUsage]

    assert daytona_retry_after_seconds(exc_info.value) == 3
    assert regions_client.close_attempts == 1


async def test_daytona_list_bounds_each_page_not_the_whole_drain(monkeypatch: pytest.MonkeyPatch) -> None:
    """A healthy listing must not be killed for having more pages than the bound covers.

    AsyncDaytona.list is cursor-paginated, so bounding the drain caps total pagination time and a
    retry restarts from page one -- a long-but-progressing listing could then never complete.
    """
    provider, daytona, _ = _inventory_provider(
        monkeypatch,
        target="configured-target",
        regions=[SimpleNamespace(id="configured-target", name="configured-target")],
    )
    page_delay = 0.1
    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", page_delay * 2.5)
    # Without this, a regression here retries through the real backoff ladder before failing.
    _skip_retry_sleep(monkeypatch, DaytonaSandboxProvider._list_sandboxes)  # pyright: ignore[reportPrivateUsage]

    def paginated(query: object) -> Any:
        daytona.listed_query = query

        async def pages() -> Any:
            for _ in range(5):
                await asyncio.sleep(page_delay)
                yield daytona.sandbox

        return pages()

    monkeypatch.setattr(daytona, "list", paginated)

    # Five pages cost 5 * page_delay in total, well past the bound; each page stays inside it.
    sandboxes = [sandbox async for sandbox in provider.list_sandboxes(SandboxQuery(labels={}))]

    assert len(sandboxes) == 5


async def test_daytona_list_still_bounds_a_stalled_page(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, daytona, _ = _inventory_provider(
        monkeypatch,
        target="configured-target",
        regions=[SimpleNamespace(id="configured-target", name="configured-target")],
    )
    monkeypatch.setattr(daytona_module, "_TOOLBOX_CALL_TIMEOUT_SECONDS", 0.05)
    _skip_retry_sleep(monkeypatch, DaytonaSandboxProvider._list_sandboxes)  # pyright: ignore[reportPrivateUsage]

    def stalled(query: object) -> Any:
        daytona.listed_query = query

        async def pages() -> Any:
            await asyncio.Event().wait()
            yield daytona.sandbox

        return pages()

    monkeypatch.setattr(daytona, "list", stalled)

    with pytest.raises(SandboxConnectionError, match="daytona.list timed out"):
        _ = [sandbox async for sandbox in provider.list_sandboxes(SandboxQuery(labels={}))]


async def test_daytona_provider_rejects_unavailable_target(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, daytona, regions_client = _inventory_provider(monkeypatch, target="missing-target", regions=[])

    with pytest.raises(SandboxError, match="Daytona target is not available: 'missing-target'"):
        _ = [sandbox async for sandbox in provider.list_sandboxes(SandboxQuery(labels={}))]

    assert daytona.listed_query is None
    assert regions_client.close_attempts == 1


def test_daytona_provider_rejects_blank_target() -> None:
    with pytest.raises(MissingSandboxConfigError, match="DAYTONA_TARGET must not be blank"):
        DaytonaSandboxProvider(
            DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key",
                DAYTONA_API_URL="https://daytona.example.test",
                DAYTONA_TARGET="   ",
            )
        )


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


async def test_daytona_create_retries_rate_limited_volume_lookup_safely(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = ApiException(
        status=429,
        reason=f"rate limited {_SENSITIVE_RETRY_SENTINEL}",
        body=f"body {_SENSITIVE_RETRY_SENTINEL}",
        data={"secret": _SENSITIVE_RETRY_SENTINEL},
    )
    error.headers = {"x-sensitive": _SENSITIVE_RETRY_SENTINEL}
    ready = SimpleNamespace(id="volume-id", name="fixtures", state=VolumeState.READY, error_reason=None)
    daytona = VolumeCapturingDaytonaClient(InnerSandbox(), [error, ready])
    observed_waits: list[float] = []

    async def record_wait(seconds: float) -> None:
        observed_waits.append(seconds)

    retryer = cast(Any, DaytonaSandboxProvider.create_sandbox).retry
    monkeypatch.setattr(retryer, "sleep", record_wait)
    with caplog.at_level(logging.WARNING, logger=daytona_module.__name__):
        sandbox = await _provider(daytona).create_sandbox(
            _request(
                "grade-sb",
                volumes=[VolumeMount(name="fixtures", mount_path="/fixtures")],
            )
        )

    assert sandbox.id == "sandbox-id"
    assert daytona.volume.calls == [("fixtures", False), ("fixtures", False)]
    assert daytona.created is True
    assert len(observed_waits) == 1
    assert 4.5 <= observed_waits[0] <= 5
    records = _retry_records(caplog)
    assert len(records) == 1
    expected_fields = {
        "sandbox_provider": "daytona",
        "daytona_step": "create_sandbox",
        "daytona_retry_attempt": 1,
        "daytona_retry_delay_s": observed_waits[0],
        "daytona_retry_category": "rate_limit",
        "daytona_status_code": 429,
    }
    expected_message = (
        "daytona.retry sandbox_provider=daytona daytona_step=create_sandbox daytona_retry_attempt=1 "
        f"daytona_retry_delay_s={observed_waits[0]} daytona_retry_category=rate_limit daytona_status_code=429"
    )
    _assert_safe_retry_record(records[0], expected_message, expected_fields)


async def test_daytona_create_awaits_volume_and_waits_until_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    pending = SimpleNamespace(
        id="volume-id",
        name="fixtures",
        state=VolumeState.PENDING_CREATE,
        error_reason=None,
    )
    ready = SimpleNamespace(
        id="volume-id",
        name="fixtures",
        state=VolumeState.READY,
        error_reason=None,
    )
    daytona = VolumeCapturingDaytonaClient(InnerSandbox(), [pending, ready])
    monkeypatch.setattr(daytona_module, "_VOLUME_READY_POLL_SECONDS", 0)

    await _provider(daytona).create_sandbox(
        _request(
            "grade-sb",
            labels={"run-id": "run-123"},
            volumes=[
                VolumeMount(
                    name="fixtures",
                    mount_path="/fixtures",
                    create_if_missing=True,
                    subpath="runs/{run_id}",
                )
            ],
        )
    )

    assert daytona.volume.calls == [("fixtures", True), ("fixtures", False)]
    assert daytona.create_params is not None
    mount = daytona.create_params.volumes[0]
    assert mount.volume_id == "volume-id"
    assert mount.mount_path == "/fixtures"
    assert mount.subpath == "runs/run-123"


async def test_daytona_create_bounds_volume_readiness_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    pending = SimpleNamespace(
        id="volume-id",
        name="fixtures",
        state=VolumeState.PENDING_CREATE,
        error_reason=None,
    )
    daytona = VolumeCapturingDaytonaClient(InnerSandbox(), [pending])
    monkeypatch.setattr(daytona_module, "_VOLUME_READY_POLL_SECONDS", 60)
    monkeypatch.setattr(daytona_module, "_VOLUME_READY_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(SandboxError, match="did not become ready within 0.01s"):
        await _provider(daytona).create_sandbox(
            _request(
                "grade-sb",
                volumes=[VolumeMount(name="fixtures", mount_path="/fixtures")],
            )
        )

    assert daytona.create_params is None


async def test_daytona_create_reports_missing_volume_as_sandbox_error() -> None:
    missing = NotFoundException(status=404, reason="volume not found")
    daytona = VolumeCapturingDaytonaClient(InnerSandbox(), [missing])

    with pytest.raises(SandboxError, match="does not exist") as raised:
        await _provider(daytona).create_sandbox(
            _request(
                "grade-sb",
                volumes=[VolumeMount(name="missing", mount_path="/fixtures")],
            )
        )

    assert type(raised.value) is SandboxError


async def test_daytona_create_rejects_read_only_volume_before_lookup() -> None:
    ready = SimpleNamespace(
        id="volume-id",
        name="fixtures",
        state=VolumeState.READY,
        error_reason=None,
    )
    daytona = VolumeCapturingDaytonaClient(InnerSandbox(), [ready])

    with pytest.raises(SandboxError, match="does not support read-only"):
        await _provider(daytona).create_sandbox(
            _request(
                "grade-sb",
                volumes=[VolumeMount(name="fixtures", mount_path="/fixtures", read_only=True)],
            )
        )

    assert daytona.volume.calls == []
    assert daytona.create_params is None


async def test_volume_run_subpath_requires_run_label_before_lookup() -> None:
    ready = SimpleNamespace(
        id="volume-id",
        name="fixtures",
        state=VolumeState.READY,
        error_reason=None,
    )
    daytona = VolumeCapturingDaytonaClient(InnerSandbox(), [ready])

    with pytest.raises(SandboxError, match="requires a non-empty"):
        await _provider(daytona).create_sandbox(
            _request(
                "grade-sb",
                volumes=[
                    VolumeMount(
                        name="fixtures",
                        mount_path="/fixtures",
                        subpath="runs/{run_id}",
                    )
                ],
            )
        )

    assert daytona.volume.calls == []


@pytest.mark.parametrize(
    ("organization_headers", "expected"),
    [
        ({"x-organization-id": "org-primary"}, "org-primary"),
        ({"daytona_organization_id": "org-legacy"}, None),
        ({}, None),
    ],
)
def test_daytona_config_from_headers_reads_optional_organization_id(
    organization_headers: dict[str, str],
    expected: str | None,
) -> None:
    config = DaytonaProviderConfig.from_headers(
        {
            "x-api-key": "key-1",
            "x-api-url": "https://daytona.example",
            "x-target": "us",
            **organization_headers,
        }
    )

    assert (
        config.DAYTONA_API_KEY,
        config.DAYTONA_API_URL,
        config.DAYTONA_TARGET,
        config.DAYTONA_ORGANIZATION_ID,
    ) == ("key-1", "https://daytona.example", "us", expected)


def test_daytona_config_from_headers_reads_legacy_required_fields() -> None:
    config = DaytonaProviderConfig.from_headers(
        {
            "daytona_api_key": "legacy-key",
            "daytona_api_url": "https://legacy.daytona.example",
            "daytona_target": "legacy-target",
            "x-organization-id": "org-primary",
        }
    )

    assert (
        config.DAYTONA_API_KEY,
        config.DAYTONA_API_URL,
        config.DAYTONA_TARGET,
        config.DAYTONA_ORGANIZATION_ID,
    ) == ("legacy-key", "https://legacy.daytona.example", "legacy-target", "org-primary")


def test_daytona_config_normalizes_api_url() -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="key-1",
        DAYTONA_API_URL="https://daytona.example///",
        DAYTONA_TARGET="us",
    )

    assert config.DAYTONA_API_URL == "https://daytona.example"


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


def test_volume_mount_requires_an_absolute_path() -> None:
    """A relative mount path silently lands somewhere provider-defined."""
    with pytest.raises(ValidationError):
        VolumeMount(name="fixtures", mount_path="vol")


def test_volume_mounts_reject_duplicate_paths() -> None:
    """Two volumes on one path is a config error, not a last-one-wins race."""
    with pytest.raises(ValidationError):
        SandboxCreateRequest(
            source=ImageSource(image="python:3.12"),
            resources=Resources(vcpu=2, memory=4, disk=10),
            name="dup",
            labels={},
            env_vars={},
            auto_stop_interval=600,
            create_timeout=360,
            volumes=[
                VolumeMount(name="a", mount_path="/vol"),
                VolumeMount(name="b", mount_path="/vol"),
            ],
        )


def test_requests_default_to_no_volumes_or_provider_secrets() -> None:
    """Every existing caller keeps its current behaviour."""
    request = SandboxCreateRequest(
        source=ImageSource(image="python:3.12"),
        resources=Resources(vcpu=2, memory=4, disk=10),
        name="plain",
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )

    assert request.volumes == []
    assert request.sandbox_secrets == {}


def test_provider_secret_references_cannot_overlap_plaintext_environment() -> None:
    request = _request("overlap").model_dump()
    request["env_vars"] = {"API_KEY": "plaintext"}
    request["sandbox_secrets"] = {"API_KEY": "provider-secret"}

    with pytest.raises(ValidationError, match="both plaintext and provider-managed secrets"):
        SandboxCreateRequest.model_validate(request)


@pytest.mark.parametrize(
    ("sandbox_secrets", "message"),
    [
        ({"BAD-NAME": "provider-secret"}, "Invalid environment variable names: BAD-NAME"),
        ({"TERM": "provider-secret"}, "Reserved command environment variable names: TERM"),
        ({"API_KEY": "   "}, "provider secret references cannot be blank: API_KEY"),
    ],
)
def test_provider_secret_references_reject_invalid_configuration(
    sandbox_secrets: dict[str, str],
    message: str,
) -> None:
    request = _request("invalid-secret").model_dump()
    request["sandbox_secrets"] = sandbox_secrets

    with pytest.raises(ValidationError, match=message):
        SandboxCreateRequest.model_validate(request)


@pytest.mark.parametrize("subpath", ["/absolute", "../escape", "runs/{missing_label}"])
def test_volume_mount_rejects_invalid_subpath(subpath: str) -> None:
    with pytest.raises(ValidationError):
        VolumeMount(name="fixtures", mount_path="/fixtures", subpath=subpath)
