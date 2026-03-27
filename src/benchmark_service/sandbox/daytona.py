from __future__ import annotations

import asyncio
import logging
import shlex
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any, Self

from daytona import (
    AsyncDaytona,
    AsyncSandbox as DaytonaAsyncSandbox,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    DaytonaConfig,
    DaytonaNotFoundError,
    FileUpload,
    Resources,
    SandboxState,
    SessionExecuteRequest,
)
from daytona.common.errors import DaytonaError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from benchmark_service.sandbox.abstract import (
    ExecResult,
    InvalidSandboxConfigurationError,
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    SandboxFile,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxResources,
    SandboxSourceType,
)

logger = logging.getLogger(__name__)

_MAX_CREATE_RETRIES = 3
_CREATE_RETRY_WAIT_SECONDS = 120
_MAX_DELETE_RETRIES = 5
_DELETE_RETRY_WAIT_SECONDS = 2
_DELETE_READY_WAIT_TIMEOUT_SECONDS = 30

MAX_WEBSOCKET_RETRIES = 10

RETRIABLE_WEBSOCKET_ERRORS = [
    "no close frame",
    "1011",
    "timed out during opening handshake",
]

RETRIABLE_DELETE_ERRORS = [
    "state change in progress",
]

_WAIT_FOR_READY_BEFORE_DELETE_STATES = {
    SandboxState.CREATING,
    SandboxState.RESTORING,
    SandboxState.STARTING,
}


def _is_retriable_websocket_error(error: DaytonaError) -> bool:
    message = str(error).lower()
    return any(pattern in message for pattern in RETRIABLE_WEBSOCKET_ERRORS)


def _is_retriable_delete_error(error: DaytonaError) -> bool:
    message = str(error).lower()
    return any(pattern in message for pattern in RETRIABLE_DELETE_ERRORS)


def _is_retriable_delete_exception(error: BaseException) -> bool:
    return isinstance(error, DaytonaError) and _is_retriable_delete_error(error)


def _build_command(
    command: str,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float | None = None,
) -> str:
    inner = command
    if timeout is not None:
        inner = f"timeout {timeout} {inner}"
    if env:
        env_prefix = " ".join(f"{k}={shlex.quote(v)}" for k, v in env.items())
        inner = f"{env_prefix} {inner}"
    if cwd:
        inner = f"cd {shlex.quote(cwd)} && {inner}"
    return inner


class DaytonaSandbox(Sandbox):
    _inner: DaytonaAsyncSandbox

    def __init__(self, provider: DaytonaSandboxProvider, inner: DaytonaAsyncSandbox) -> None:
        super().__init__(provider=provider, id=inner.id, name=inner.name)
        self._inner = inner

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        full_cmd = _build_command(command, cwd=cwd, env=env, timeout=timeout)

        if on_stdout is not None or on_stderr is not None:
            return await self._exec_streaming(full_cmd, on_stdout, on_stderr)

        try:
            result = await self._inner.process.exec(full_cmd)
        except DaytonaError as e:
            raise SandboxError(str(e)) from e
        return ExecResult(
            exit_code=result.exit_code,
            stdout=result.result or "",
            stderr="",
        )

    async def _exec_streaming(
        self,
        command: str,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        session_id = f"{self.id}:{uuid.uuid4()}"
        try:
            try:
                await self._inner.process.create_session(session_id)
            except DaytonaError as e:
                raise SandboxError(str(e)) from e

            try:
                session_exec_resp = await self._inner.process.execute_session_command(
                    session_id,
                    SessionExecuteRequest(command=command, run_async=True),
                )
            except DaytonaError as e:
                raise SandboxError(str(e)) from e

            cmd_id = session_exec_resp.cmd_id
            if not cmd_id:
                raise SandboxError(f"Failed to execute command in session {session_id}")

            for attempt in range(1, MAX_WEBSOCKET_RETRIES + 1):
                try:
                    await self._inner.process.get_session_command_logs_async(
                        session_id=session_id,
                        command_id=cmd_id,
                        on_stdout=on_stdout or (lambda _: None),
                        on_stderr=on_stderr or (lambda _: None),
                    )
                    break
                except DaytonaError as e:
                    if not _is_retriable_websocket_error(e) or attempt == MAX_WEBSOCKET_RETRIES:
                        raise SandboxError(str(e)) from e
                    logger.warning(
                        f"WebSocket disconnected (attempt {attempt}/{MAX_WEBSOCKET_RETRIES}), reconnecting: {e}"
                    )

            try:
                cmd = await self._inner.process.get_session_command(session_id, cmd_id)
                exit_code = cmd.exit_code
            except Exception as e:
                logger.warning(f"Failed to get exit code for session {session_id}: {e}")
                exit_code = None

            return ExecResult(
                exit_code=exit_code,
                stdout="",
                stderr="",
            )

        finally:
            try:
                await self._inner.process.delete_session(session_id)
            except Exception:
                logger.warning(f"Failed to delete session {session_id}")

    async def upload_file(self, file: SandboxFile) -> None:
        try:
            await self._inner.fs.upload_file(file.content, file.remote_path)
        except DaytonaError as e:
            raise SandboxError(str(e)) from e

    async def upload_files(self, files: list[SandboxFile]) -> None:
        files_to_upload = [
            FileUpload(source=f.content, destination=f.remote_path)
            for f in files
        ]
        try:
            await self._inner.fs.upload_files(files_to_upload)
        except DaytonaError as e:
            raise SandboxError(str(e)) from e

    async def download_file(self, remote_path: str) -> bytes:
        try:
            return await self._inner.fs.download_file(remote_path)
        except DaytonaError as e:
            raise SandboxError(str(e)) from e

    async def create_folder(self, remote_path: str) -> None:
        try:
            await self._inner.fs.create_folder(remote_path, "755")
        except DaytonaError as e:
            raise SandboxError(str(e)) from e

    async def wait_until_ready(self) -> None:
        try:
            await self._inner.wait_for_sandbox_start(timeout=0)
        except DaytonaError as e:
            raise SandboxError(str(e)) from e


class DaytonaSandboxProvider(SandboxProvider):
    _daytona: AsyncDaytona

    def __init__(self, daytona: AsyncDaytona) -> None:
        self._daytona = daytona

    @classmethod
    async def from_headers(cls, headers: Mapping[str, str], **kwargs: Any) -> Self:
        api_key = headers.get("x-api-key")
        api_url = headers.get("x-api-url")
        target = headers.get("x-target")

        if not api_key or not api_url or not target:
            raise ValueError("Missing required headers: x-api-key, x-api-url, x-target")

        config = DaytonaConfig(api_key=api_key, api_url=api_url, target=target)
        return cls(AsyncDaytona(config=config))

    async def create_sandbox(self, request: SandboxCreateRequest) -> DaytonaSandbox:
        """Create a Daytona sandbox with retry logic.

        If creation times out but the sandbox exists on Daytona's side
        (e.g. still booting), the next attempt will find and reuse it by name.
        Retries up to 3 times with 120s between attempts.
        """
        self._validate_create_request(request)
        attempt_number = 0

        @retry(
            retry=retry_if_not_exception_type(InvalidSandboxConfigurationError),
            stop=stop_after_attempt(_MAX_CREATE_RETRIES),
            wait=wait_fixed(_CREATE_RETRY_WAIT_SECONDS),
            before_sleep=before_sleep_log(logger, logger.level or logging.WARNING),
            sleep=asyncio.sleep,
            reraise=True,
        )
        async def _create() -> DaytonaSandbox:
            nonlocal attempt_number
            attempt_number += 1

            if request.name and attempt_number > 1:
                try:
                    sandbox = await self.get_sandbox(request.name)
                    await sandbox.wait_until_ready()
                    return sandbox
                except SandboxNotFoundError:
                    pass

            try:
                return await self._do_create(request)
            except DaytonaError as e:
                raise SandboxError(str(e)) from e

        return await _create()

    def _validate_create_request(self, request: SandboxCreateRequest) -> None:
        if not request.source_id.strip():
            source_kind = request.source_type.value
            article = "an" if source_kind[0] in "aeiou" else "a"
            raise InvalidSandboxConfigurationError(
                f"{source_kind.capitalize()}-based sandbox requested without {article} {source_kind} name"
            )

    async def _do_create(self, request: SandboxCreateRequest) -> DaytonaSandbox:
        if request.source_type == SandboxSourceType.SNAPSHOT:
            params: CreateSandboxFromSnapshotParams | CreateSandboxFromImageParams = CreateSandboxFromSnapshotParams(
                snapshot=request.source_id,
                name=request.name,
                labels=request.labels or None,
                env_vars=request.env_vars or None,
                network_block_all=request.network_blocked,
                auto_delete_interval=request.auto_delete_interval,
                language="python",
            )
        else:
            params = CreateSandboxFromImageParams(
                image=request.source_id,
                name=request.name,
                labels=request.labels or None,
                env_vars=request.env_vars or None,
                network_block_all=request.network_blocked,
                auto_delete_interval=request.auto_delete_interval,
                resources=_to_daytona_resources(request.resources) if request.resources else None,
                language="python",
            )

        inner = await self._daytona.create(params, timeout=request.creation_timeout)
        return DaytonaSandbox(provider=self, inner=inner)

    async def get_sandbox(self, id: str) -> DaytonaSandbox:
        try:
            inner = await self._daytona.get(id)
        except DaytonaNotFoundError as e:
            raise SandboxNotFoundError(str(e)) from e
        return DaytonaSandbox(provider=self, inner=inner)

    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        @retry(
            retry=retry_if_exception(_is_retriable_delete_exception),
            stop=stop_after_attempt(_MAX_DELETE_RETRIES),
            wait=wait_fixed(_DELETE_RETRY_WAIT_SECONDS),
            before_sleep=before_sleep_log(logger, logger.level or logging.WARNING),
            sleep=asyncio.sleep,
            reraise=True,
        )
        async def _delete() -> None:
            try:
                inner = await self._daytona.get(sandbox.id)
                if inner.state in (SandboxState.DESTROYING, SandboxState.DESTROYED):
                    return

                if inner.state in _WAIT_FOR_READY_BEFORE_DELETE_STATES:
                    try:
                        await inner.wait_for_sandbox_start(timeout=_DELETE_READY_WAIT_TIMEOUT_SECONDS)
                    except DaytonaError as e:
                        logger.warning(f"Sandbox {sandbox.name} was not ready before delete attempt: {e}")
                    inner = await self._daytona.get(sandbox.id)
                    if inner.state in (SandboxState.DESTROYING, SandboxState.DESTROYED):
                        return
            except DaytonaNotFoundError:
                logger.warning(f"Sandbox {sandbox.name} already deleted")
                return

            try:
                await self._daytona.delete(inner)
            except DaytonaNotFoundError:
                logger.warning(f"Sandbox {sandbox.name} already deleted")
                return

        try:
            await _delete()
        except DaytonaError as e:
            raise SandboxError(str(e)) from e

    async def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[DaytonaSandbox]:
        q = query or SandboxQuery()
        page = 1

        while True:
            paginated = await self._daytona.list(
                labels=q.labels or None,
                limit=q.limit,
                page=page,
            )

            if not paginated.items:
                break

            for inner in paginated.items:
                if inner.state in (SandboxState.DESTROYING, SandboxState.DESTROYED):
                    continue
                yield DaytonaSandbox(provider=self, inner=inner)

            if paginated.page >= paginated.total_pages:
                break
            page = int(paginated.page) + 1

    async def close(self) -> None:
        await self._daytona.close()


def _to_daytona_resources(resources: SandboxResources) -> Resources:
    return Resources(
        cpu=resources.cpu,
        memory=resources.memory,
        disk=resources.disk,
    )
