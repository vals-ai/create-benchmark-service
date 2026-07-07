from __future__ import annotations

import asyncio
import ipaddress
import shlex
import socket
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from contextlib import suppress
from typing import Any, Literal
from urllib.parse import urlparse

from aiohttp import ClientConnectionError, ClientResponseError
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
from pydantic import BaseModel
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential, wait_fixed

from benchmark_service.sandbox.types import (
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
_REMOVED_SANDBOX_CLIENT_STATUSES = (404, 502)
_FAILED_EXECUTE_COMMAND_PREFIX = "failed to execute command:"
_EGRESS_HOSTS_BEGIN = "# benchmark-service egress allowlist begin"
_EGRESS_HOSTS_END = "# benchmark-service egress allowlist end"
_EGRESS_HOSTS_TEMP_PATH = "/tmp/.benchmark-service-egress-hosts"
# Daytona sometimes flattens a transport failure into a bare DaytonaError message with no chained
# cause; these substrings recover those cases by text when no typed cause survives to match.
_TRANSPORT_ERROR_MESSAGES = (
    " is used by transport ",
    "[errno 32] broken pipe",
    "[errno 9] bad file descriptor",
    "502 bad gateway",
    "server disconnected",
)
_RETRYABLE_DAYTONA_CAUSES = (ClientConnectionError, ConnectionError, TimeoutError)
_FIXED_PROVIDER_WAIT = wait_fixed(2)
_RATE_LIMIT_WAIT = wait_exponential(multiplier=1, min=1, max=30)


def _parse_daytona_ipv4_network(value: str) -> ipaddress.IPv4Network | None:
    try:
        network = ipaddress.ip_network(value, strict=False)
    except ValueError:
        return None

    if isinstance(network, ipaddress.IPv4Network):
        return network

    raise ValueError(f"allowed address is an IPv6 CIDR which is not supported: {value}")


def _clear_egress_host_pins_command() -> str:
    sed_script = f"/^{_EGRESS_HOSTS_BEGIN}$/,/^{_EGRESS_HOSTS_END}$/d"
    return (
        f"sed {shlex.quote(sed_script)} /etc/hosts > {shlex.quote(_EGRESS_HOSTS_TEMP_PATH)}"
        f" && cat {shlex.quote(_EGRESS_HOSTS_TEMP_PATH)} > /etc/hosts"
        f" && rm -f {shlex.quote(_EGRESS_HOSTS_TEMP_PATH)}"
    )


def _replace_egress_host_pins_command(host_pins: dict[str, str]) -> str:
    if not host_pins:
        return _clear_egress_host_pins_command()

    hosts = "".join(f"{address} {host}\n" for host, address in sorted(host_pins.items()))
    block = f"{_EGRESS_HOSTS_BEGIN}\n{hosts}{_EGRESS_HOSTS_END}\n"
    return f"{_clear_egress_host_pins_command()} && printf %s {shlex.quote(block)} >> /etc/hosts"


def _resolve_daytona_allowed_addresses(allowed_addresses: list[str]) -> tuple[list[str], dict[str, str]]:
    # Normalize entries first so empty allowlists and blank values fail before reaching Daytona.
    values = [address.strip() for address in allowed_addresses]
    if not values or any(not value for value in values):
        raise ValueError("allowed addresses cannot be empty; use sandbox.clear_egress_rules to clear egress rules")

    cidrs: list[str] = []
    host_pins: dict[str, str] = {}

    for value in values:
        # Pass IPv4 CIDR inputs through directly because Daytona network rules are CIDR based.
        network = _parse_daytona_ipv4_network(value)
        if network is not None:
            cidrs.append(str(network))
            continue

        # Treat non-CIDR values as URLs or hosts and reject anything without a hostname.
        parsed = urlparse(value if "://" in value else f"//{value}")
        if not parsed.hostname:
            raise ValueError(f"allowed address is not a valid URL, host, or CIDR: {value}")

        # Resolve hosts to IPv4 addresses so URL entries become Daytona-compatible CIDR rules.
        try:
            address_infos = socket.getaddrinfo(parsed.hostname, 443, family=socket.AF_INET, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise ValueError(f"allowed address host did not resolve: {value}") from exc

        resolved_addresses = sorted(
            {address_info[4][0] for address_info in address_infos if isinstance(address_info[4][0], str)}
        )
        cidrs.extend(f"{address}/32" for address in resolved_addresses)

        # Pin the hostname to an allowed IPv4 address so DNS is not needed after egress narrows.
        if resolved_addresses:
            host_pins[parsed.hostname] = resolved_addresses[0]

    # Deduplicate CIDRs while preserving order before returning rules to Daytona.
    cidrs = list(dict.fromkeys(cidrs))
    if not cidrs:
        raise ValueError("allowed addresses did not resolve to Daytona-compatible CIDR rules")

    return cidrs, host_pins


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

    def create_provider(self) -> SandboxProvider:
        return DaytonaSandboxProvider(self)


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
        or (exc.error_code is not None and exc.error_code.upper() == "NOT_FOUND")
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


def _is_transient_daytona_error(exc: DaytonaError | ClientResponseError) -> bool:
    if isinstance(exc, _TRANSIENT_DAYTONA_ERRORS) or _has_retryable_cause(exc):
        return True
    return _message_contains(exc, _TRANSPORT_ERROR_MESSAGES)


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

    @_PROVIDER_RETRY
    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        allow_list, host_pins = await asyncio.to_thread(_resolve_daytona_allowed_addresses, allowed_addresses)
        await self._replace_egress_host_pins(host_pins)

        try:
            await self._sandbox.update_network_settings(
                network_block_all=False,
                network_allow_list=",".join(allow_list),
            )
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def clear_egress_rules(self) -> None:
        await self._replace_egress_host_pins({})

        try:
            await self._sandbox.update_network_settings(
                network_block_all=False,
                network_allow_list=None,
            )
        except _SANDBOX_OPERATION_ERRORS as exc:
            raise self._sandbox_error(exc) from exc

    async def _replace_egress_host_pins(self, host_pins: dict[str, str]) -> None:
        result = await self.exec(_replace_egress_host_pins_command(host_pins))
        if result.exit_code != 0:
            raise SandboxError("failed to update egress allowlist hosts")

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
    def __init__(self, config: DaytonaProviderConfig) -> None:
        self._daytona = AsyncDaytona(
            config=DaytonaConfig(
                api_key=config.DAYTONA_API_KEY,
                api_url=config.DAYTONA_API_URL,
                target=config.DAYTONA_TARGET,
                connection_pool_maxsize=None,
            )
        )

    def _sandbox_error(self, exc: DaytonaError) -> SandboxError:
        if _is_not_found_error(exc):
            return SandboxNotFoundError(f"Sandbox not found: {exc}")
        if _is_transient_daytona_error(exc):
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
