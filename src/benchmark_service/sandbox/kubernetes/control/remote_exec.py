from __future__ import annotations

import asyncio
import base64
import binascii
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterable
from typing import Any, Protocol, cast

from aiohttp import WSMsgType
from kubernetes_asyncio import client
from kubernetes_asyncio.stream import WsApiClient

from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.types import SandboxConnectionError, SandboxError

_STDIN_CHANNEL = 0
_STDOUT_CHANNEL = 1
_STDERR_CHANNEL = 2
_STATUS_CHANNEL = 3
_CLOSE_CHANNEL = 255


class RemoteExecSession(Protocol):
    async def read_stdout(self) -> bytes: ...

    async def read_stderr(self) -> bytes: ...

    async def write_stdin(self, data: bytes) -> None: ...

    async def close_stdin(self) -> None: ...

    async def update(self, timeout: float) -> None: ...

    def is_open(self) -> bool: ...

    @property
    def return_code(self) -> int | None: ...

    async def close(self) -> None: ...


class RemoteExec(Protocol):
    async def open(
        self,
        pod_name: str,
        command: list[str],
        *,
        stdin: bool = False,
    ) -> RemoteExecSession: ...

    async def terminate(self, pod_name: str, command_id: str) -> None: ...

    async def close(self) -> None: ...


async def encode_base64_chunks(chunks: AsyncIterable[bytes]) -> AsyncGenerator[str, None]:
    carry = b""
    async for chunk in chunks:
        data = carry + chunk
        boundary = len(data) - (len(data) % 3)
        if boundary:
            yield base64.b64encode(data[:boundary]).decode("ascii")
        carry = data[boundary:]
    if carry:
        yield base64.b64encode(carry).decode("ascii")


async def decode_base64_chunks(chunks: AsyncIterable[bytes | str]) -> AsyncGenerator[bytes, None]:
    carry = b""
    try:
        async for chunk in chunks:
            encoded = chunk.encode("ascii") if isinstance(chunk, str) else chunk
            data = b"".join((carry + encoded).split())
            boundary = len(data) - (len(data) % 4)
            if boundary:
                yield base64.b64decode(data[:boundary], validate=True)
            carry = data[boundary:]
        if carry:
            yield base64.b64decode(carry, validate=True)
    except (UnicodeEncodeError, binascii.Error) as error:
        raise SandboxError("Remote file stream contained invalid base64") from error


class AiohttpRemoteExecSession:
    """Adapt Kubernetes channel frames to the remote-exec session contract."""

    def __init__(self, websocket: Any, request_context: Any) -> None:
        self._websocket = websocket
        self._request_context = request_context
        self._stdout: deque[bytes] = deque()
        self._stderr: deque[bytes] = deque()
        self._return_code: int | None = None
        self._closed = False
        self._released = False

    async def read_stdout(self) -> bytes:
        return self._stdout.popleft() if self._stdout else b""

    async def read_stderr(self) -> bytes:
        return self._stderr.popleft() if self._stderr else b""

    async def write_stdin(self, data: bytes) -> None:
        await self._websocket.send_bytes(bytes([_STDIN_CHANNEL]) + data)

    async def close_stdin(self) -> None:
        await self._websocket.send_bytes(bytes([_CLOSE_CHANNEL, _STDIN_CHANNEL]))

    async def update(self, timeout: float) -> None:
        try:
            message = await self._websocket.receive(timeout=timeout)
        except TimeoutError:
            return
        if message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
            self._closed = True
            if self._return_code is None:
                self._return_code = 1
            return
        if message.type != WSMsgType.BINARY:
            return
        data = cast(bytes, message.data)
        if not data:
            return
        channel, payload = data[0], data[1:]
        if channel == _STDOUT_CHANNEL:
            self._stdout.append(payload)
        elif channel == _STDERR_CHANNEL:
            self._stderr.append(payload)
        elif channel == _STATUS_CHANNEL:
            self._return_code = WsApiClient.parse_error_data(payload)
            self._closed = True

    def is_open(self) -> bool:
        return not self._closed or bool(self._stdout) or bool(self._stderr)

    @property
    def return_code(self) -> int | None:
        return self._return_code

    async def close(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            await self._request_context.__aexit__(None, None, None)
        finally:
            self._closed = True


class KubernetesRemoteExec:
    """Open Kubernetes v5 exec WebSockets for one configured container."""

    def __init__(self, settings: KubernetesControlSettings) -> None:
        self.settings = settings
        self._api_client = WsApiClient(heartbeat=30)
        self._core = client.CoreV1Api(self._api_client)

    async def open(
        self,
        pod_name: str,
        command: list[str],
        *,
        stdin: bool = False,
    ) -> RemoteExecSession:
        try:
            request: Any = cast(Any, self._core).connect_get_namespaced_pod_exec(
                pod_name,
                self.settings.namespace,
                command=command,
                container=self.settings.sandbox_container_name,
                stderr=True,
                stdin=stdin,
                stdout=True,
                tty=False,
                _preload_content=False,
                _headers={"sec-websocket-protocol": "v5.channel.k8s.io"},
            )
            request_context: Any = await request
            websocket: Any = await request_context.__aenter__()
        except Exception as error:
            raise SandboxConnectionError(f"Could not open Kubernetes exec stream: {error}") from error
        return AiohttpRemoteExecSession(websocket, request_context)

    async def terminate(self, pod_name: str, command_id: str) -> None:
        session: RemoteExecSession | None = None
        pid_file = f"/tmp/{command_id}.pid"
        shell_command = (
            "terminate_tree() { "
            'target_pid="$1"; child_pids=""; '
            'if [ -r "/proc/$target_pid/task/$target_pid/children" ]; then '
            'read -r child_pids < "/proc/$target_pid/task/$target_pid/children" || true; fi; '
            'for child_pid in $child_pids; do terminate_tree "$child_pid"; done; '
            'kill -TERM "$target_pid" 2>/dev/null || true; }; '
            f"if read -r command_pid < {pid_file} && [ \"$command_pid\" -gt 1 ] 2>/dev/null; then "
            'terminate_tree "$command_pid"; fi'
        )
        try:
            async with asyncio.timeout(10):
                session = await self.open(
                    pod_name,
                    ["sh", "-lc", shell_command],
                )
                while session.is_open():
                    await session.update(0.1)
                    await asyncio.sleep(0)
        except (SandboxError, TimeoutError):
            pass
        finally:
            if session is not None:
                await session.close()

    async def close(self) -> None:
        await self._api_client.close()
