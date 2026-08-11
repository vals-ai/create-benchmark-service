from __future__ import annotations

import asyncio
import logging
import math
import os
import shlex
import uuid
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar

from aiohttp import ClientConnectionError, ClientPayloadError, ClientResponseError
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
    GpuType,
)
from daytona import (
    VolumeMount as DaytonaVolumeMount,
)
from daytona import (
    Resources as DaytonaResources,
)
from daytona.common.errors import (
    SOURCE_API,
    SOURCE_DAEMON,
    SOURCE_PROXY,
    DaytonaConflictError,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaRateLimitError,
    DaytonaTimeoutError,
    create_daytona_error,
)
from daytona.common.pty import PtyResult, PtySize
from daytona.handle.async_pty_handle import AsyncPtyHandle
from daytona_api_client_async import ApiClient as DaytonaApiClient
from daytona_api_client_async import Configuration as DaytonaApiConfiguration
from daytona_api_client_async import OrganizationsApi
from daytona_api_client_async.exceptions import NotFoundException, OpenApiException
from daytona_toolbox_api_client_async.models.pty_session_info import PtySessionInfo
from pydantic import BaseModel
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_chain,
    wait_exponential,
    wait_random,
)

from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.types import (
    ComposeSource,
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SnapshotSource,
    TargetedSnapshotSource,
    VolumeMount,
    resolve_volume_subpath,
    validate_command_env,
)

logger = logging.getLogger(__name__)

_SOURCE_NAMES: Mapping[str | None, str] = {
    SOURCE_API: "api",
    SOURCE_DAEMON: "daemon",
    SOURCE_PROXY: "proxy",
}
_PTY_STATUS_CHECK_ATTEMPTS = 30
_PTY_STATUS_POLL_SECONDS = 5
_PTY_STDOUT_TAIL_MAX_BYTES = 64 * 1024
_PTY_CREATE_MARKER_ENV = "_CBS_PTY_CREATE_MARKER"
_PTY_ROWS = 24
_PTY_COLS = 80
_STATUS_DIR = "/tmp/.sandbox-provider"
_REMOVED_SANDBOX_STATES = (SandboxState.DESTROYING, SandboxState.DESTROYED)
_FAILED_SANDBOX_STATES = (SandboxState.ERROR, SandboxState.BUILD_FAILED)
_DEAD_SANDBOX_STATES = (
    *_REMOVED_SANDBOX_STATES,
    SandboxState.STOPPED,
    *_FAILED_SANDBOX_STATES,
)
_SANDBOX_OPERATION_ERRORS = (DaytonaError, ClientResponseError)
_TRANSIENT_DAYTONA_ERRORS = (DaytonaConnectionError, DaytonaRateLimitError, DaytonaTimeoutError)
_RETRY_AFTER_PREFIX = "retry-after-"
_KNOWN_THROTTLERS = ("sandbox-create", "sandbox-lifecycle", "authenticated", "anonymous")
_DELETE_CONFLICT_MESSAGES = ("state change in progress", "modified by another operation")
_REMOVED_SANDBOX_CLIENT_STATUSES = (404, 502)
_RETRYABLE_PROVIDER_STATUSES = (408, 429, 500, 502, 503, 504)
_FAILED_EXECUTE_COMMAND_PREFIX = "failed to execute command:"
_VOLUME_READY_POLL_SECONDS = 0.5
_VOLUME_READY_TIMEOUT_SECONDS = 30.0
_VOLUME_PENDING_STATES = frozenset({"creating", "pending_create"})
# Daytona sometimes flattens a transport failure into a bare DaytonaError message with no chained
# cause; these substrings recover those cases by text when no typed cause survives to match.
_TRANSPORT_ERROR_MESSAGES = (
    " is used by transport ",
    "[errno 32] broken pipe",
    "[errno 9] bad file descriptor",
    "502 bad gateway",
    "an unexpected error occurred.",
    "failed to register with sysbox-mgr",
    "failed to resolve container ip",
    "server disconnected",
    "temporary authentication service error",
)
_RETRYABLE_DAYTONA_CAUSES = (ClientConnectionError, ClientPayloadError, ConnectionError, TimeoutError)
_PROVIDER_RETRY_DELAYS_SECONDS = (5, 25, 90, 300, 420)
_FIXED_PROVIDER_WAIT = wait_chain(*(wait_random(delay * 0.9, delay) for delay in _PROVIDER_RETRY_DELAYS_SECONDS))
_RATE_LIMIT_WAIT = wait_exponential(multiplier=1, min=1, max=30)

# Far above a healthy round trip, but low enough that a worst-case pass through the full
# _PROVIDER_RETRY_DELAYS_SECONDS ladder stays under half an hour.
_TOOLBOX_CALL_TIMEOUT_SECONDS = 120.0

# The SDK reads wait_for_sandbox_start(timeout=0) as "wait forever". 600s matches the largest
# create budget in this service (grading's _GRADING_CREATE_TIMEOUT_S), so a start that a concurrent
# create is still legitimately waiting on is never aborted early.
_SANDBOX_START_TIMEOUT_SECONDS = 600.0

_T = TypeVar("_T")


async def _bounded(operation: str, awaitable: Awaitable[_T], timeout: float | None) -> _T:
    """Await `awaitable` under a hard timeout, reporting expiry as a retryable connection error.

    A `timeout` of None means unbounded, and is used only where the request legitimately stays
    open for as long as the in-sandbox command runs (see DaytonaSandbox.exec).
    """
    if timeout is None:
        return await awaitable
    try:
        async with asyncio.timeout(timeout):
            return await awaitable
    except TimeoutError as exc:
        raise SandboxConnectionError(f"Daytona {operation} timed out after {timeout:g}s") from exc


async def _collect_sandboxes(sandboxes: AsyncIterator[AsyncSandbox], timeout: float) -> list[AsyncSandbox]:
    """Drain a paginated listing, bounding each page fetch rather than the whole drain.

    Bounding the drain would cap total pagination time, so a healthy listing with enough pages
    fails once its cumulative time crosses the bound, and every retry restarts from page one.
    """
    collected: list[AsyncSandbox] = []
    pages = sandboxes.__aiter__()
    while True:
        try:
            collected.append(await _bounded("daytona.list", anext(pages), timeout))
        except StopAsyncIteration:
            return collected


@dataclass
class _PtyCreateState:
    marker: str
    saw_ambiguous_create: bool = False
    owns_session: bool = False


def _pty_result_summary(result: PtyResult | None) -> str:
    if result is None:
        return "unavailable"
    error = result.error[:200] if result.error else None
    return f"exit_code={result.exit_code}, error={error!r}"


def _resolve_daytona_allowed_addresses(allowed_addresses: list[str]) -> tuple[list[str], list[str]]:
    cidrs, domains = resolve_allowed_addresses(allowed_addresses)
    if cidrs and domains:
        raise ValueError("allowed addresses cannot mix domains and CIDRs")

    return cidrs, domains


class DaytonaProviderConfig(BaseModel):
    type: Literal["daytona"] = "daytona"
    DAYTONA_API_KEY: str
    DAYTONA_API_URL: str
    DAYTONA_TARGET: str

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> "DaytonaProviderConfig":
        api_key = _get_config_header(headers, "x-api-key", "daytona_api_key")
        api_url = _get_config_header(headers, "x-api-url", "daytona_api_url")
        target = _get_config_header(headers, "x-target", "daytona_target")
        if not api_key or not api_url or not target:
            raise MissingSandboxConfigError("Missing required headers: x-api-key, x-api-url, x-target")
        return cls(DAYTONA_API_KEY=api_key, DAYTONA_API_URL=api_url, DAYTONA_TARGET=target)

    @classmethod
    def from_env(cls) -> "DaytonaProviderConfig":
        """Build config from the DAYTONA_* environment variables; callers never supply creds."""
        api_key = os.environ.get("DAYTONA_API_KEY")
        api_url = os.environ.get("DAYTONA_API_URL")
        target = os.environ.get("DAYTONA_TARGET")
        missing = [
            name
            for name, value in (
                ("DAYTONA_API_KEY", api_key),
                ("DAYTONA_API_URL", api_url),
                ("DAYTONA_TARGET", target),
            )
            if not value
        ]
        if missing:
            raise MissingSandboxConfigError(f"Missing required environment variables: {', '.join(missing)}")
        return cls(DAYTONA_API_KEY=api_key, DAYTONA_API_URL=api_url, DAYTONA_TARGET=target)  # type: ignore[arg-type]

    def create_provider(self) -> SandboxProvider:
        return DaytonaSandboxProvider(self)


def _daytona_client(config: DaytonaProviderConfig, target: str) -> AsyncDaytona:
    return AsyncDaytona(
        config=DaytonaConfig(
            api_key=config.DAYTONA_API_KEY,
            api_url=config.DAYTONA_API_URL,
            target=target,
            connection_pool_maxsize=None,
        )
    )


def _daytona_gpu_type(gpu_type: str | None) -> GpuType | None:
    if gpu_type is None:
        return None
    try:
        member = GpuType(gpu_type)
    except ValueError:
        member = GpuType.UNKNOWN_DEFAULT_OPEN_API
    if member is GpuType.UNKNOWN_DEFAULT_OPEN_API:
        supported = ", ".join(t.value for t in GpuType if t is not GpuType.UNKNOWN_DEFAULT_OPEN_API)
        raise SandboxError(f"Unsupported Daytona GPU type: {gpu_type}. Supported types: {supported}")
    return member


def _get_config_header(headers: Mapping[str, str], *names: str) -> str | None:
    normalized_headers = {key.lower(): value for key, value in headers.items()}
    for name in names:
        value = normalized_headers.get(name.lower())
        if value:
            return value
    return None


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


def _message_contains(exc: BaseException, messages: tuple[str, ...]) -> bool:
    error = str(exc).lower()
    return any(message in error for message in messages)


def _is_delete_conflict(exc: DaytonaConflictError) -> bool:
    return _message_contains(exc, _DELETE_CONFLICT_MESSAGES)


def _is_not_found_error(exc: DaytonaError | ClientResponseError) -> bool:
    if isinstance(exc, ClientResponseError):
        return exc.status in _REMOVED_SANDBOX_CLIENT_STATUSES
    return (
        isinstance(exc, DaytonaNotFoundError)
        or exc.status_code == 404
        or (exc.code is not None and exc.code.upper() == "NOT_FOUND")
    )


def _is_failed_execute_command_error(exc: DaytonaError | ClientResponseError) -> bool:
    return isinstance(exc, DaytonaError) and str(exc).strip().lower() == _FAILED_EXECUTE_COMMAND_PREFIX


def _has_retryable_cause(exc: BaseException) -> bool:
    seen: set[int] = set()
    cause = exc.__cause__ or exc.__context__
    while cause is not None and id(cause) not in seen:
        if isinstance(cause, _RETRYABLE_DAYTONA_CAUSES):
            return True
        seen.add(id(cause))
        cause = cause.__cause__ or cause.__context__
    return False


def _provider_status_code(exc: DaytonaError | ClientResponseError) -> int | None:
    if isinstance(exc, ClientResponseError):
        return exc.status
    return exc.status_code


def _is_transient_daytona_error(exc: DaytonaError | ClientResponseError) -> bool:
    if isinstance(exc, _TRANSIENT_DAYTONA_ERRORS) or _has_retryable_cause(exc):
        return True
    if _provider_status_code(exc) in _RETRYABLE_PROVIDER_STATUSES:
        return True
    return _message_contains(exc, _TRANSPORT_ERROR_MESSAGES)


def _is_name_conflict_error(exc: DaytonaError) -> bool:
    return (
        isinstance(exc, DaytonaConflictError) or exc.status_code == 409 or _message_contains(exc, ("already exists",))
    )


def _is_ambiguous_pty_create_error(exc: DaytonaError | ClientResponseError) -> bool:
    if isinstance(exc, DaytonaRateLimitError) or _provider_status_code(exc) == 429:
        return False
    return _is_transient_daytona_error(exc)


def _parse_retry_after_seconds(value: object) -> float | None:
    try:
        seconds = float(str(value))
    except ValueError:
        return None

    if seconds < 0 or not math.isfinite(seconds):
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


def _provider_retry_before_sleep(retry_state: RetryCallState) -> None:
    try:
        outcome = retry_state.outcome
        next_action = retry_state.next_action
        fn = retry_state.fn
        assert outcome is not None
        assert next_action is not None
        assert fn is not None
        exc = outcome.exception()
        assert exc is not None

        delay = next_action.sleep
        status_code: int | None = None
        source: str | None = None
        provider_error = exc.__cause__
        if isinstance(provider_error, DaytonaError):
            status_code = provider_error.status_code
            source = _SOURCE_NAMES.get(provider_error.source)
        elif isinstance(provider_error, ClientResponseError):
            status_code = provider_error.status
        elif isinstance(provider_error, OpenApiException):
            raw_status = getattr(provider_error, "status", None)
            if type(raw_status) is int:
                status_code = raw_status

        category = "rate_limit" if _rate_limit_error(exc) is not None or status_code == 429 else "transient"
        fields: dict[str, object] = {
            "sandbox_provider": "daytona",
            "daytona_step": fn.__name__,
            "daytona_retry_attempt": retry_state.attempt_number,
            "daytona_retry_delay_s": delay,
            "daytona_retry_category": category,
        }
        if status_code is not None:
            fields["daytona_status_code"] = status_code
        if source is not None:
            fields["daytona_source"] = source

        message = "daytona.retry " + " ".join(f"{key}={value}" for key, value in fields.items())
        logger.warning(message, extra=fields)
    except Exception:
        pass


_PROVIDER_RETRY = retry(
    retry=retry_if_exception_type(SandboxConnectionError),
    stop=stop_after_attempt(len(_PROVIDER_RETRY_DELAYS_SECONDS) + 1),
    wait=_provider_retry_wait,
    before_sleep=_provider_retry_before_sleep,
    reraise=True,
)


class DaytonaSandbox(Sandbox):
    def __init__(self, sandbox: AsyncSandbox) -> None:
        self._sandbox = sandbox
        self.labels = dict(sandbox.labels)
        self.created_at = self._parse_created_at(sandbox.created_at)

    @property
    def id(self) -> str:
        return self._sandbox.id

    @property
    def name(self) -> str:
        return self._sandbox.name

    @property
    def state(self) -> str:
        return str(self._sandbox.state)

    def _parse_created_at(self, value: str | None) -> datetime | None:
        if value is None:
            return None

        try:
            created_at = datetime.fromisoformat(value)
        except ValueError as exc:
            raise SandboxError(f"Invalid Daytona creation time for {self._sandbox_ref}.") from exc

        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise SandboxError(f"Invalid Daytona creation time for {self._sandbox_ref}.")
        return created_at.astimezone(UTC)

    @property
    def _sandbox_ref(self) -> str:
        return f"name={self.name}, id={self.id}"

    def _removed_error(self) -> SandboxNotFoundError:
        return SandboxNotFoundError(f"Sandbox not found: {self._sandbox_ref}.")

    def _sandbox_error(self, exc: DaytonaError | ClientResponseError) -> SandboxError:
        if _is_not_found_error(exc):
            return self._removed_error()
        if _is_transient_daytona_error(exc) or _is_failed_execute_command_error(exc):
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
        # _command enforces an explicit deadline in-sandbox, so the transport gets that budget plus
        # a margin. Untimed stays unbounded: process.exec holds the request open for the whole
        # command, so a ceiling would abort long installs.
        transport_timeout = None if timeout is None else timeout + _TOOLBOX_CALL_TIMEOUT_SECONDS
        return await self._run_exec(_command(command, cwd, timeout), transport_timeout)

    @_PROVIDER_RETRY
    async def _control_exec(self, command: str) -> ExecResult:
        """Run a short internal probe (test -e / cat / rm) under the toolbox bound.

        Unlike the public exec these must stay bounded: they run in the PTY poll loop, where a
        stalled probe is exactly the hang the bound exists to break.
        """
        return await self._run_exec(_command(command, None, None), _TOOLBOX_CALL_TIMEOUT_SECONDS)

    async def _run_exec(self, full_command: str, transport_timeout: float | None) -> ExecResult:
        try:
            result = await _bounded("process.exec", self._sandbox.process.exec(full_command), transport_timeout)
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

        return ExecResult(exit_code=result.exit_code, output=result.result or "")

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = validate_command_env(env_vars)
        output: asyncio.Queue[str] = asyncio.Queue()
        exec_task = asyncio.create_task(self._exec_pty(_command(command, cwd, timeout), output, env))

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
        except _RETRYABLE_DAYTONA_CAUSES as exc:
            # Failures while consuming the stream surface raw, outside the SDK's error wrapping.
            raise SandboxConnectionError(f"Sandbox connection error for {self._sandbox_ref}: {exc}") from exc

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        try:
            stream = await self._sandbox.fs.download_file_stream(remote_path)
            async for chunk in stream:
                yield chunk
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        network_allow_list, domain_allow_list = _resolve_daytona_allowed_addresses(allowed_addresses)

        try:
            await _bounded(
                "update_network_settings",
                self._sandbox.update_network_settings(
                    network_allow_list=",".join(network_allow_list),
                    domain_allow_list=",".join(domain_allow_list),
                ),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def clear_egress_rules(self) -> None:
        try:
            await _bounded(
                "update_network_settings",
                self._sandbox.update_network_settings(
                    network_block_all=False,
                    network_allow_list="",
                    domain_allow_list="",
                ),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

    async def _exec_pty(self, command: str, output: asyncio.Queue[str], env_vars: dict[str, str]) -> ExecResult:
        session_id = f"{self.id}:exec-{uuid.uuid4().hex}"
        status_path = f"{_STATUS_DIR}/{uuid.uuid4().hex}.status"
        status_temp_path = f"{status_path}.tmp"
        # Keep only a bounded tail of the output for the ExecResult; the full stream is
        # forwarded through the queue, so retaining it all would grow without limit on
        # long-running, chatty commands.
        stdout: deque[str] = deque()
        stdout_bytes = 0
        handle: AsyncPtyHandle | None = None
        wait_task: asyncio.Task[PtyResult] | None = None
        create_state = _PtyCreateState(marker=uuid.uuid4().hex)
        pty_envs = {
            "TERM": "dumb",
            "LANG": "C.UTF-8",
            **env_vars,
            _PTY_CREATE_MARKER_ENV: create_state.marker,
        }

        async def on_data(data: bytes) -> None:
            nonlocal stdout_bytes
            text = data.decode("utf-8", errors="replace")
            stdout.append(text)
            stdout_bytes += len(text)
            while stdout_bytes > _PTY_STDOUT_TAIL_MAX_BYTES and len(stdout) > 1:
                stdout_bytes -= len(stdout.popleft())
            output.put_nowait(text)

        try:
            handle = await self._create_pty_session(session_id, on_data, pty_envs, create_state)
            if handle is None:
                session_info = await self._get_pty_create_session_info(session_id)
                expected_environment_matches = all(
                    session_info.envs.get(key) == value for key, value in pty_envs.items()
                )
                if (
                    session_info.id != session_id
                    or session_info.rows != _PTY_ROWS
                    or session_info.cols != _PTY_COLS
                    or not expected_environment_matches
                ):
                    raise SandboxError(
                        f"Daytona PTY create reconciliation did not match the attempted session for "
                        f"{self._sandbox_ref}: session_id={session_id}"
                    )
                create_state.owns_session = True
                handle = await self._connect_created_pty(session_id, on_data)
            else:
                create_state.owns_session = True

            await _bounded(
                "handle.send_input",
                handle.send_input(f"stty -echo; unset {_PTY_CREATE_MARKER_ENV}\n"),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
            await _bounded(
                "handle.send_input",
                handle.send_input(
                    f"mkdir -p {shlex.quote(_STATUS_DIR)}; {command}; "
                    f"printf '%s\\n' \"$?\" > {shlex.quote(status_temp_path)} "
                    f"&& mv {shlex.quote(status_temp_path)} {shlex.quote(status_path)}; exit\n"
                ),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
            wait_task = asyncio.create_task(handle.wait())

            reconnect_attempts = 0
            while True:
                done, _ = await asyncio.wait({wait_task}, timeout=_PTY_STATUS_POLL_SECONDS)
                try:
                    result = await self._control_exec(f"test -e {shlex.quote(status_path)}")
                except SandboxError:
                    status_exists = False
                    with suppress(SandboxError):
                        result = await self._control_exec(f"test -e {shlex.quote(status_path)}")
                        status_exists = result.exit_code == 0
                    if not status_exists:
                        raise
                    break
                if result.exit_code == 0:
                    break

                try:
                    await self._check_sandbox_alive()
                except SandboxError:
                    status_exists = False
                    with suppress(SandboxError):
                        result = await self._control_exec(f"test -e {shlex.quote(status_path)}")
                        status_exists = result.exit_code == 0
                    if not status_exists:
                        raise
                    break

                if not done:
                    continue

                reconnect_attempts += 1
                if reconnect_attempts == _PTY_STATUS_CHECK_ATTEMPTS:
                    raise SandboxConnectionError(
                        f"Daytona PTY command did not write an exit code for {self._sandbox_ref}: "
                        f"session_id={session_id}"
                    )

                wait_result: PtyResult | None = None
                with suppress(Exception):
                    wait_result = await wait_task
                if wait_result is not None and wait_result.exit_code not in (None, 0):
                    raise SandboxError(
                        f"Daytona PTY exited before writing command status for {self._sandbox_ref}: "
                        f"session_id={session_id}, {_pty_result_summary(wait_result)}"
                    )
                await _bounded("handle.disconnect", handle.disconnect(), _TOOLBOX_CALL_TIMEOUT_SECONDS)
                handle = await self._reconnect_pty(session_id, on_data, wait_result)
                wait_task = asyncio.create_task(handle.wait())

            result = await self._control_exec(f"cat {shlex.quote(status_path)}")
            if result.exit_code != 0 or not result.output:
                raise SandboxError(
                    f"Failed to read Daytona PTY exit code for {self._sandbox_ref}: status_path={status_path}"
                )
            return ExecResult(exit_code=int(result.output.strip()), output="".join(stdout))
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc
        finally:
            if wait_task:
                wait_task.cancel()
                with suppress(Exception, asyncio.CancelledError):
                    await wait_task
            if handle:
                with suppress(Exception):
                    await _bounded("handle.disconnect", handle.disconnect(), _TOOLBOX_CALL_TIMEOUT_SECONDS)
            if create_state.owns_session:
                with suppress(Exception):
                    await _bounded(
                        "process.kill_pty_session",
                        self._sandbox.process.kill_pty_session(session_id),
                        _TOOLBOX_CALL_TIMEOUT_SECONDS,
                    )
                with suppress(Exception):
                    await self._control_exec(f"rm -f {shlex.quote(status_path)} {shlex.quote(status_temp_path)}")

    @_PROVIDER_RETRY
    async def _create_pty_session(
        self,
        session_id: str,
        on_data: Callable[[bytes], Awaitable[None]],
        envs: dict[str, str],
        state: _PtyCreateState,
    ) -> AsyncPtyHandle | None:
        try:
            return await _bounded(
                "process.create_pty_session",
                self._sandbox.process.create_pty_session(
                    id=session_id,
                    on_data=on_data,
                    envs=envs,
                    pty_size=PtySize(rows=_PTY_ROWS, cols=_PTY_COLS),
                ),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
        except DaytonaConflictError as exc:
            if state.saw_ambiguous_create:
                return None
            raise self._sandbox_error(exc) from exc
        except _SANDBOX_OPERATION_ERRORS as exc:
            if _is_ambiguous_pty_create_error(exc):
                state.saw_ambiguous_create = True
            await self._check_sandbox_alive()
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def _get_pty_create_session_info(self, session_id: str) -> PtySessionInfo:
        try:
            return await _bounded(
                "process.get_pty_session_info",
                self._sandbox.process.get_pty_session_info(session_id),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
        except _SANDBOX_OPERATION_ERRORS as exc:
            await self._check_sandbox_alive()
            if isinstance(exc, DaytonaNotFoundError) or _provider_status_code(exc) == 404:
                raise SandboxError(
                    f"Daytona PTY session disappeared during create reconciliation for {self._sandbox_ref}: "
                    f"session_id={session_id}"
                ) from exc
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def _connect_created_pty(
        self,
        session_id: str,
        on_data: Callable[[bytes], Awaitable[None]],
    ) -> AsyncPtyHandle:
        try:
            return await _bounded(
                "process.connect_pty_session",
                self._sandbox.process.connect_pty_session(session_id, on_data),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
        except _SANDBOX_OPERATION_ERRORS as exc:
            await self._check_sandbox_alive()
            if isinstance(exc, DaytonaNotFoundError) or _provider_status_code(exc) == 404:
                raise SandboxError(
                    f"Daytona PTY session disappeared before create reconciliation connected for "
                    f"{self._sandbox_ref}: session_id={session_id}"
                ) from exc
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def _reconnect_pty(
        self,
        session_id: str,
        on_data: Callable[[bytes], Awaitable[None]],
        wait_result: PtyResult | None,
    ) -> AsyncPtyHandle:
        try:
            await _bounded(
                "process.get_pty_session_info",
                self._sandbox.process.get_pty_session_info(session_id),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
            return await _bounded(
                "process.connect_pty_session",
                self._sandbox.process.connect_pty_session(session_id, on_data),
                _TOOLBOX_CALL_TIMEOUT_SECONDS,
            )
        except (DaytonaNotFoundError, DaytonaConnectionError) as exc:
            await self._check_sandbox_alive()
            if isinstance(exc, DaytonaNotFoundError) or "not found" in str(exc).lower():
                raise SandboxError(
                    f"Daytona PTY session disappeared before command status was written for {self._sandbox_ref}: "
                    f"session_id={session_id}, wait_result=({_pty_result_summary(wait_result)}), state={self.state}"
                ) from exc
            raise self._sandbox_error(exc) from exc
        except _SANDBOX_OPERATION_ERRORS as exc:
            await self._check_sandbox_alive()
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def _check_sandbox_alive(self) -> None:
        try:
            await _bounded("refresh_data", self._sandbox.refresh_data(), _TOOLBOX_CALL_TIMEOUT_SECONDS)
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

        if self._sandbox.state in _DEAD_SANDBOX_STATES:
            if self._sandbox.state in _REMOVED_SANDBOX_STATES:
                raise self._removed_error()
            raise SandboxError(f"Sandbox is not running: {self._sandbox_ref}, state={self.state}.")


class DaytonaSandboxProvider(SandboxProvider):
    def __init__(self, config: DaytonaProviderConfig) -> None:
        self._config = config
        self._target = config.DAYTONA_TARGET.strip()
        if not self._target:
            raise MissingSandboxConfigError("DAYTONA_TARGET must not be blank")
        self._target_id: str | None = None
        self._target_id_lock = asyncio.Lock()
        self._target_api_configuration = DaytonaApiConfiguration(
            host=config.DAYTONA_API_URL,
            access_token=config.DAYTONA_API_KEY,
        )
        self._daytona = _daytona_client(config, self._target)
        self._daytona_by_target = {self._target: self._daytona}

    def _client_for_target(self, target: str) -> AsyncDaytona:
        if target not in self._daytona_by_target:
            self._daytona_by_target[target] = _daytona_client(self._config, target)
        return self._daytona_by_target[target]

    def _sandbox_error(self, exc: DaytonaError) -> SandboxError:
        if _is_not_found_error(exc):
            return SandboxNotFoundError(f"Sandbox not found: {exc}")
        if _is_transient_daytona_error(exc):
            return SandboxConnectionError(f"Daytona sandbox provider connection error: {exc}")
        return SandboxError(f"Daytona sandbox provider error: {exc}")

    async def _resolve_volumes(
        self,
        daytona: AsyncDaytona,
        mounts: list[VolumeMount],
        labels: Mapping[str, str],
    ) -> list[DaytonaVolumeMount]:
        """Map named volumes to Daytona mounts, resolving each name to its id.

        Daytona mounts by volume id rather than name, so a benchmark that only
        knows the name would otherwise have to resolve it itself and every
        caller would reimplement this.
        """
        resolved: list[DaytonaVolumeMount] = []
        for mount in mounts:
            if mount.read_only:
                raise SandboxError(
                    f"Daytona does not support read-only volume mounts; volume {mount.name!r} "
                    f"at {mount.mount_path!r} would be writable"
                )
            subpath = resolve_volume_subpath(mount, labels)
            try:
                volume = await daytona.volume.get(mount.name, create=mount.create_if_missing)
                state = volume.state.value
                try:
                    async with asyncio.timeout(_VOLUME_READY_TIMEOUT_SECONDS):
                        while volume.state != "ready":
                            if state not in _VOLUME_PENDING_STATES:
                                detail = f": {volume.error_reason}" if volume.error_reason else ""
                                raise SandboxError(
                                    f"Daytona volume {mount.name!r} is not ready (state={state!r}){detail}"
                                )
                            await asyncio.sleep(_VOLUME_READY_POLL_SECONDS)
                            volume = await daytona.volume.get(mount.name)
                            state = volume.state.value
                except TimeoutError as exc:
                    raise SandboxError(
                        f"Daytona volume {mount.name!r} did not become ready within "
                        f"{_VOLUME_READY_TIMEOUT_SECONDS:g}s (state={state!r})"
                    ) from exc
            except NotFoundException as exc:
                raise SandboxError(
                    f"Daytona volume {mount.name!r} does not exist (mount {mount.mount_path}); "
                    "create it or set create_if_missing"
                ) from exc
            except OpenApiException as exc:
                daytona_error = create_daytona_error(
                    f"Failed to resolve Daytona volume {mount.name!r}: {exc}",
                    status_code=getattr(exc, "status", None),
                    headers=getattr(exc, "headers", None),
                )
                raise self._sandbox_error(daytona_error) from exc
            except DaytonaError as exc:
                raise self._sandbox_error(exc) from exc
            resolved.append(
                DaytonaVolumeMount(
                    volume_id=volume.id,
                    mount_path=mount.mount_path,
                    subpath=subpath,
                )
            )
        return resolved

    @_PROVIDER_RETRY
    async def create_sandbox(self, request: SandboxCreateRequest) -> DaytonaSandbox:
        daytona = self._daytona
        if isinstance(request.source, TargetedSnapshotSource):
            daytona = self._client_for_target(request.source.target)
        resources = DaytonaResources(
            cpu=request.resources.vcpu,
            memory=request.resources.memory,
            disk=request.resources.disk,
            gpu=request.resources.gpu or None,
            gpu_type=_daytona_gpu_type(request.resources.gpu_type),
        )

        volume_mounts = (
            await self._resolve_volumes(daytona, request.volumes, request.labels) if request.volumes else None
        )

        match request.source:
            case ImageSource(image=image):
                params = CreateSandboxFromImageParams(
                    auto_stop_interval=request.auto_stop_interval,
                    auto_delete_interval=0,
                    name=request.name,
                    labels=request.labels,
                    image=image,
                    network_block_all=request.network_block_all,
                    resources=resources,
                    env_vars=request.env_vars,
                    secrets=request.sandbox_secrets or None,
                    volumes=volume_mounts,
                )
            case SnapshotSource(snapshot=snapshot) | TargetedSnapshotSource(snapshot=snapshot):
                if request.resources.gpu:
                    raise SandboxError(
                        "Daytona snapshot sandboxes use the snapshot's resources; GPUs cannot be requested"
                    )
                params = CreateSandboxFromSnapshotParams(
                    auto_stop_interval=request.auto_stop_interval,
                    auto_delete_interval=0,
                    name=request.name,
                    labels=request.labels,
                    snapshot=snapshot,
                    language="python",
                    network_block_all=request.network_block_all,
                    env_vars=request.env_vars,
                    secrets=request.sandbox_secrets or None,
                    volumes=volume_mounts,
                )
            case ComposeSource():
                raise SandboxError("ComposeSource must be unwrapped before provider.create_sandbox")

        try:
            inner = await daytona.create(params, timeout=request.create_timeout)
        except DaytonaError as exc:
            # A name conflict means an earlier attempt of this same request
            # created the sandbox but the response was lost (transport retry).
            # Recover it by name so the retry doesn't strand an orphan and fail
            # the request on the conflict.
            if _is_name_conflict_error(exc):
                existing = await self._find_existing_sandbox(request.name, daytona)
                if existing is not None:
                    return DaytonaSandbox(existing)
            elif _is_transient_daytona_error(exc):
                await self._delete_failed_sandbox(request.name, daytona)
            raise self._sandbox_error(exc) from exc

        return DaytonaSandbox(inner)

    async def _delete_failed_sandbox(self, name: str, daytona: AsyncDaytona) -> None:
        try:
            sandbox = await _bounded("daytona.get", daytona.get(name), _TOOLBOX_CALL_TIMEOUT_SECONDS)
        except DaytonaNotFoundError:
            return
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

        if sandbox.state in _FAILED_SANDBOX_STATES:
            await self.delete_sandbox(sandbox.id)

    async def _find_existing_sandbox(self, name: str, daytona: AsyncDaytona) -> AsyncSandbox | None:
        try:
            sandbox = await _bounded("daytona.get", daytona.get(name), _TOOLBOX_CALL_TIMEOUT_SECONDS)
        except DaytonaNotFoundError:
            return None
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

        try:
            if sandbox.state in _FAILED_SANDBOX_STATES:
                await self.delete_sandbox(sandbox.id)
                return None
            if sandbox.state in (SandboxState.DESTROYING, SandboxState.DESTROYED, SandboxState.STOPPED):
                return None
            await _bounded(
                "wait_for_sandbox_start",
                sandbox.wait_for_sandbox_start(timeout=0),
                _SANDBOX_START_TIMEOUT_SECONDS,
            )
            return sandbox
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def get_sandbox(self, instance_id: str) -> DaytonaSandbox:
        try:
            return DaytonaSandbox(
                await _bounded("daytona.get", self._daytona.get(instance_id), _TOOLBOX_CALL_TIMEOUT_SECONDS)
            )
        except DaytonaNotFoundError as exc:
            raise SandboxNotFoundError(f"Sandbox not found: id_or_name={instance_id}.") from exc
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            sandbox = await _bounded("daytona.get", self._daytona.get(instance_id), _TOOLBOX_CALL_TIMEOUT_SECONDS)
            if sandbox.state in _REMOVED_SANDBOX_STATES:
                return
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
            target_id = await self._resolve_target_id()
            daytona_query = ListSandboxesQuery(
                labels=query.labels,
                targets=[target_id],
                # Daytona applies created_at_before inclusively despite the name.
                created_at_before=query.created_at_lte,
                limit=query.page_size,
            )
            return await _collect_sandboxes(self._daytona.list(daytona_query), _TOOLBOX_CALL_TIMEOUT_SECONDS)
        except DaytonaError as exc:
            raise self._sandbox_error(exc) from exc

    async def _resolve_target_id(self) -> str:
        if self._target_id is not None:
            return self._target_id

        async with self._target_id_lock:
            if self._target_id is not None:
                return self._target_id

            try:
                async with DaytonaApiClient(self._target_api_configuration) as api_client:
                    regions = await OrganizationsApi(api_client).list_available_regions()
            except OpenApiException as exc:
                raise create_daytona_error(
                    f"Failed to resolve Daytona target: {exc}",
                    status_code=getattr(exc, "status", None),
                    headers=getattr(exc, "headers", None),
                ) from exc
            except _RETRYABLE_DAYTONA_CAUSES as exc:
                raise SandboxConnectionError(f"Daytona sandbox provider connection error: {exc}") from exc

            region = next((region for region in regions if region.name == self._target), None)
            region = region or next((region for region in regions if region.id == self._target), None)
            if region is None:
                raise SandboxError(f"Daytona target is not available: {self._target!r}.")

            self._target_id = region.id
            return self._target_id

    async def close(self) -> None:
        for daytona in self._daytona_by_target.values():
            await daytona.close()


def _command(command: str, cwd: str | None, timeout: float | None) -> str:
    command = f"sh -c {shlex.quote(command)}"
    if timeout is not None:
        command = f"timeout {timeout:g} {command}"
    if cwd:
        command = f"cd {shlex.quote(cwd)} && {command}"
    return command
