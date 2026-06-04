from __future__ import annotations

import asyncio
import shlex
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import suppress
from typing import Any

from aiohttp import ClientResponseError
from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    DaytonaConfig,
    DaytonaNotFoundError,
    ListSandboxesQuery,
    SandboxState,
)
from daytona import (
    Resources as DaytonaResources,
)
from daytona.common.errors import (
    DaytonaConflictError,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaRateLimitError,
    DaytonaTimeoutError,
)
from daytona.handle.async_pty_handle import AsyncPtyHandle
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential, wait_fixed

from benchmark_service.sandbox.types import (
    DaytonaBackendConfig,
    ExecResult,
    ImageSource,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SnapshotSource,
)

_PTY_STATUS_CHECK_ATTEMPTS = 30
_STATUS_DIR = "/tmp/.sandbox-provider"
_REMOVED_SANDBOX_STATES = (SandboxState.DESTROYING, SandboxState.DESTROYED)
_FAILED_SANDBOX_STATES = (SandboxState.ERROR, SandboxState.BUILD_FAILED)
_DEAD_SANDBOX_STATES = (*_REMOVED_SANDBOX_STATES, SandboxState.STOPPED, *_FAILED_SANDBOX_STATES)
_SANDBOX_OPERATION_ERRORS = (DaytonaError, ClientResponseError)
_TRANSIENT_DAYTONA_ERRORS = (DaytonaConnectionError, DaytonaRateLimitError, DaytonaTimeoutError)
_RETRY_AFTER_PREFIX = "retry-after-"
_KNOWN_THROTTLERS = ("sandbox-create", "sandbox-lifecycle", "authenticated", "anonymous")
_DELETE_CONFLICT_MESSAGES = ("state change in progress", "modified by another operation")
_FIXED_PROVIDER_WAIT = wait_fixed(2)
_RATE_LIMIT_WAIT = wait_exponential(multiplier=1, min=1, max=30)


def _provider_retry_wait(retry_state: RetryCallState) -> float:
    assert retry_state.outcome is not None
    exc = retry_state.outcome.exception()
    assert exc is not None

    rate_limit_error = _rate_limit_error(exc)
    if rate_limit_error is None:
        return _FIXED_PROVIDER_WAIT(retry_state)

    seconds = daytona_retry_after_seconds(rate_limit_error)
    if seconds is not None:
        return seconds

    return _RATE_LIMIT_WAIT(retry_state)


def _rate_limit_error(exc: BaseException) -> DaytonaRateLimitError | None:
    if isinstance(exc, DaytonaRateLimitError):
        return exc
    if isinstance(exc.__cause__, DaytonaRateLimitError):
        return exc.__cause__
    return None


def _is_delete_conflict(exc: DaytonaConflictError) -> bool:
    error = str(exc).lower()
    return any(message in error for message in _DELETE_CONFLICT_MESSAGES)


def _is_not_found_error(exc: DaytonaError | ClientResponseError) -> bool:
    if isinstance(exc, ClientResponseError):
        return exc.status == 404
    return (
        isinstance(exc, DaytonaNotFoundError)
        or exc.status_code == 404
        or (exc.error_code is not None and exc.error_code.upper() == "NOT_FOUND")
    )


def _parse_retry_after_seconds(value: object) -> float | None:
    try:
        seconds = float(str(value))
    except ValueError:
        return None

    if seconds < 0:
        return None

    return seconds


def _get_header(headers: dict[str, Any], header_name: str) -> object | None:
    header_name = header_name.lower()
    for key, value in headers.items():
        if str(key).lower() == header_name:
            return value
    return None


def daytona_retry_after_seconds(exc: DaytonaRateLimitError) -> float | None:
    for throttler in _KNOWN_THROTTLERS:
        seconds = _parse_retry_after_seconds(_get_header(exc.headers, f"retry-after-{throttler}"))
        if seconds is not None:
            return seconds

    seconds = _parse_retry_after_seconds(_get_header(exc.headers, "retry-after"))
    if seconds is not None:
        return seconds

    for key, value in exc.headers.items():
        if str(key).lower().startswith(_RETRY_AFTER_PREFIX):
            seconds = _parse_retry_after_seconds(value)
            if seconds is not None:
                return seconds

    return None


_PROVIDER_RETRY = retry(
    retry=retry_if_exception_type(SandboxConnectionError),
    stop=stop_after_attempt(3),
    wait=_provider_retry_wait,
    reraise=True,
)


class DaytonaSandbox(Sandbox):
    def __init__(self, sandbox: AsyncSandbox) -> None:
        self._sandbox = sandbox

    @property
    def id(self) -> str:
        return self._sandbox.id

    @property
    def name(self) -> str:
        return self._sandbox.name

    @property
    def state(self) -> str:
        return str(self._sandbox.state)

    @property
    def _sandbox_ref(self) -> str:
        return f"name={self.name}, id={self.id}"

    def _removed_error(self) -> SandboxNotFoundError:
        return SandboxNotFoundError(f"Sandbox not found: {self._sandbox_ref}.")

    def _sandbox_error(self, exc: DaytonaError | ClientResponseError) -> SandboxError:
        if _is_not_found_error(exc):
            return self._removed_error()
        if isinstance(exc, _TRANSIENT_DAYTONA_ERRORS):
            return SandboxConnectionError(f"Sandbox connection error for {self._sandbox_ref}: {exc}")
        return SandboxError(f"Sandbox operation failed for {self._sandbox_ref}: {exc}")

    @_PROVIDER_RETRY
    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        full_command = _command(command, cwd, timeout)
        try:
            result = await self._sandbox.process.exec(full_command)
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

        return ExecResult(exit_code=result.exit_code, output=result.result or "")

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[str, None]:
        output: asyncio.Queue[str] = asyncio.Queue()
        exec_task = asyncio.create_task(self._exec_pty(_command(command, cwd, timeout), output))

        try:
            while not exec_task.done():
                try:
                    yield await asyncio.wait_for(output.get(), timeout=0.1)
                except TimeoutError:
                    continue

            while not output.empty():
                yield output.get_nowait()

            result = await exec_task
            if result.exit_code != 0:
                raise SandboxCommandError(result.exit_code)
        finally:
            if not exec_task.done():
                exec_task.cancel()
                with suppress(asyncio.CancelledError):
                    await exec_task

    @_PROVIDER_RETRY
    async def upload_file(self, remote_path: str, content: bytes) -> None:
        try:
            await self._sandbox.fs.upload_file(content, remote_path)
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def download_file(self, remote_path: str) -> bytes:
        try:
            stream = await self._sandbox.fs.download_file_stream(remote_path)
            return b"".join([chunk async for chunk in stream])
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

    async def _exec_pty(self, command: str, output: asyncio.Queue[str]) -> ExecResult:
        session_id = f"{self.id}:exec-{uuid.uuid4().hex}"
        status_path = f"{_STATUS_DIR}/{uuid.uuid4().hex}.status"
        stdout: list[str] = []
        handle: AsyncPtyHandle | None = None

        async def on_data(data: bytes) -> None:
            text = data.decode("utf-8", errors="replace")
            stdout.append(text)
            output.put_nowait(text)

        try:
            handle = await self._create_pty_session(session_id, on_data)
            await handle.send_input("stty -echo\n")
            await handle.send_input(
                f"mkdir -p {shlex.quote(_STATUS_DIR)}; {command}; echo $? > {shlex.quote(status_path)}; exit\n"
            )
            with suppress(Exception):
                await handle.wait()

            for _ in range(_PTY_STATUS_CHECK_ATTEMPTS):
                await self._check_sandbox_alive()
                result = await self.exec(f"test -e {shlex.quote(status_path)}")
                if result.exit_code == 0:
                    break
                handle = await self._reconnect_pty(session_id, on_data)
                await asyncio.sleep(1)
            else:
                raise SandboxConnectionError(
                    f"Daytona PTY command did not write an exit code for {self._sandbox_ref}: session_id={session_id}"
                )

            result = await self.exec(f"cat {shlex.quote(status_path)}")
            if result.exit_code != 0 or not result.output:
                raise SandboxError(
                    f"Failed to read Daytona PTY exit code for {self._sandbox_ref}: status_path={status_path}"
                )
            return ExecResult(exit_code=int(result.output.strip()), output="".join(stdout))
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc
        finally:
            if handle:
                with suppress(Exception):
                    await handle.disconnect()
            with suppress(Exception):
                await self._sandbox.process.kill_pty_session(session_id)
            with suppress(Exception):
                await self.exec(f"rm -f {shlex.quote(status_path)}")

    @_PROVIDER_RETRY
    async def _create_pty_session(
        self,
        session_id: str,
        on_data: Callable[[bytes], Awaitable[None]],
    ) -> AsyncPtyHandle:
        try:
            return await self._sandbox.process.create_pty_session(
                id=session_id,
                on_data=on_data,
                envs={"TERM": "dumb", "LANG": "C.UTF-8"},
            )
        except _SANDBOX_OPERATION_ERRORS as exc:
            await self._check_sandbox_alive()
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def _reconnect_pty(
        self,
        session_id: str,
        on_data: Callable[[bytes], Awaitable[None]],
    ) -> AsyncPtyHandle:
        try:
            await self._sandbox.process.get_pty_session_info(session_id)
            handle = await self._sandbox.process.connect_pty_session(session_id, on_data)
            with suppress(Exception):
                await handle.wait()
            return handle
        except DaytonaNotFoundError as exc:
            raise SandboxError(
                f"Daytona PTY session no longer exists for {self._sandbox_ref}: session_id={session_id}"
            ) from exc
        except DaytonaConnectionError as exc:
            await self._check_sandbox_alive()
            if "not found" in str(exc).lower():
                raise SandboxError(
                    f"Daytona PTY session no longer exists for {self._sandbox_ref}: session_id={session_id}"
                ) from exc
            raise self._sandbox_error(exc) from exc
        except _SANDBOX_OPERATION_ERRORS as exc:
            await self._check_sandbox_alive()
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def _check_sandbox_alive(self) -> None:
        try:
            await self._sandbox.refresh_data()
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

        if self._sandbox.state in _DEAD_SANDBOX_STATES:
            if self._sandbox.state in _REMOVED_SANDBOX_STATES:
                raise self._removed_error()
            raise SandboxError(f"Sandbox is not running: {self._sandbox_ref}, state={self.state}.")


class DaytonaSandboxProvider(SandboxProvider):
    def __init__(self, config: DaytonaBackendConfig) -> None:
        self._daytona = AsyncDaytona(
            config=DaytonaConfig(
                api_key=config.api_key,
                api_url=config.api_url,
                target=config.target,
                connection_pool_maxsize=None,
            )
        )

    def _sandbox_error(self, exc: DaytonaError) -> SandboxError:
        if isinstance(exc, _TRANSIENT_DAYTONA_ERRORS):
            return SandboxConnectionError(f"Daytona sandbox provider connection error: {exc}")
        return SandboxError(f"Daytona sandbox provider error: {exc}")

    @_PROVIDER_RETRY
    async def create_sandbox(self, request: SandboxCreateRequest) -> DaytonaSandbox:
        existing = await self._find_reusable_sandbox(request.name)
        if existing is not None:
            return DaytonaSandbox(existing)

        resources = DaytonaResources(
            cpu=request.resources.vcpu,
            memory=request.resources.memory,
            disk=request.resources.disk,
        )

        match request.source:
            case ImageSource(image=image):
                params = CreateSandboxFromImageParams(
                    auto_stop_interval=request.auto_stop_interval,
                    auto_delete_interval=0,
                    name=request.name,
                    labels=request.labels,
                    image=image,
                    network_block_all=False,
                    resources=resources,
                    env_vars=request.env_vars,
                )
            case SnapshotSource(snapshot=snapshot):
                params = CreateSandboxFromSnapshotParams(
                    auto_stop_interval=request.auto_stop_interval,
                    auto_delete_interval=0,
                    name=request.name,
                    labels=request.labels,
                    snapshot=snapshot,
                    language="python",
                    network_block_all=False,
                    env_vars=request.env_vars,
                )

        try:
            inner = await self._daytona.create(params, timeout=request.create_timeout)
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

        return DaytonaSandbox(inner)

    async def _find_reusable_sandbox(self, name: str) -> AsyncSandbox | None:
        try:
            sandbox = await self._daytona.get(name)
        except DaytonaNotFoundError:
            return None
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

        try:
            if sandbox.state in (SandboxState.DESTROYING, SandboxState.DESTROYED, SandboxState.STOPPED):
                return None
            await sandbox.wait_for_sandbox_start(timeout=0)
            return sandbox
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def get_sandbox(self, instance_id: str) -> DaytonaSandbox:
        try:
            return DaytonaSandbox(await self._daytona.get(instance_id))
        except DaytonaNotFoundError as exc:
            raise SandboxNotFoundError(f"Sandbox not found: id_or_name={instance_id}.") from exc
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            sandbox = await self._daytona.get(instance_id)
            if sandbox.state not in (*_REMOVED_SANDBOX_STATES, *_FAILED_SANDBOX_STATES):
                await sandbox.wait_for_sandbox_start(timeout=0)
                await sandbox.refresh_data()
            if sandbox.state in _REMOVED_SANDBOX_STATES:
                return
            if sandbox.state not in _FAILED_SANDBOX_STATES:
                await sandbox.set_autostop_interval(interval=1)
            await self._daytona.delete(sandbox)
        except DaytonaNotFoundError:
            return
        except DaytonaConflictError as exc:
            if _is_delete_conflict(exc):
                raise SandboxConnectionError(str(exc)) from exc
            raise self._sandbox_error(exc) from exc
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[DaytonaSandbox, None]:
        for sandbox in await self._list_sandboxes(query):
            if sandbox.state in (SandboxState.DESTROYING, SandboxState.DESTROYED):
                continue
            yield DaytonaSandbox(sandbox)

    @_PROVIDER_RETRY
    async def _list_sandboxes(self, query: SandboxQuery) -> list[AsyncSandbox]:
        try:
            daytona_query = ListSandboxesQuery(labels=query.labels, limit=query.page_size)
            return [sandbox async for sandbox in self._daytona.list(daytona_query)]
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

    async def close(self) -> None:
        await self._daytona.close()


def _command(command: str, cwd: str | None, timeout: float | None) -> str:
    if timeout is not None:
        command = f"timeout {timeout:g} {command}"
    if cwd:
        command = f"cd {shlex.quote(cwd)} && {command}"
    return command
