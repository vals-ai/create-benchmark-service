from __future__ import annotations

import codecs
import shlex
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable

from benchmark_service.sandbox.kubernetes.control.agent import PodDataPlane
from benchmark_service.sandbox.kubernetes.control.remote_exec import (
    RemoteExec,
    RemoteExecSession,
    decode_base64_chunks,
    encode_base64_chunks,
)
from benchmark_service.sandbox.kubernetes.control.resource_data import PodEndpoint
from benchmark_service.sandbox.kubernetes.control.resources import sandbox_name
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandEvent,
    CommandExitEvent,
    CommandOutputEvent,
    CommandRequest,
)
from benchmark_service.sandbox.types import (
    SandboxError,
    validate_command_env,
)


class SandboxDataPlane:
    """Run commands and file transfers against one ready sandbox Pod."""

    def __init__(
        self,
        settings: KubernetesControlSettings,
        remote_exec: RemoteExec | None,
        pod_agent: PodDataPlane | None,
        ready_pod_name: Callable[[str], Awaitable[str]],
        ready_pod_endpoint: Callable[[str], Awaitable[PodEndpoint]],
    ) -> None:
        self.settings = settings
        self.remote_exec = remote_exec
        self.pod_agent = pod_agent
        self._ready_pod_name = ready_pod_name
        self._ready_pod_endpoint = ready_pod_endpoint

    def _remote_exec(self) -> RemoteExec:
        if self.remote_exec is None:
            raise SandboxError("Kubernetes remote exec is not configured")

        return self.remote_exec

    def _shell_command(self, request: CommandRequest, command_id: str) -> str:
        env = validate_command_env(request.env_vars)
        command = request.command
        pid_file = f"/tmp/{command_id}.pid"
        if request.timeout is not None:
            command = f"timeout {request.timeout:g} sh -lc {shlex.quote(command)}"
        if env:
            assignments = " ".join(f"{name}={shlex.quote(value)}" for name, value in sorted(env.items()))
            command = f"env {assignments} sh -lc {shlex.quote(command)}"
        if request.cwd:
            command = f"cd {shlex.quote(request.cwd)} && {command}"

        return (
            f"SANDBOX_COMMAND_ID={shlex.quote(command_id)}; export SANDBOX_COMMAND_ID; "
            f"SANDBOX_COMMAND_PID_FILE={shlex.quote(pid_file)}; "
            'printf \'%s\\n\' "$$" > "$SANDBOX_COMMAND_PID_FILE"; '
            "trap 'exit 143' TERM INT; "
            "trap 'rm -f \"$SANDBOX_COMMAND_PID_FILE\"' EXIT; "
            f"{command}"
        )

    async def _stream_session(
        self,
        session: RemoteExecSession,
    ) -> AsyncGenerator[CommandEvent, None]:
        stdout_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        stderr_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        while session.is_open():
            await session.update(0.1)
            stdout = await session.read_stdout()
            stderr = await session.read_stderr()
            if stdout and (text := stdout_decoder.decode(stdout)):
                yield CommandOutputEvent(type="stdout", data=text)
            if stderr and (text := stderr_decoder.decode(stderr)):
                yield CommandOutputEvent(type="stderr", data=text)
        if text := stdout_decoder.decode(b"", final=True):
            yield CommandOutputEvent(type="stdout", data=text)
        if text := stderr_decoder.decode(b"", final=True):
            yield CommandOutputEvent(type="stderr", data=text)
        yield CommandExitEvent(type="exit", exit_code=session.return_code if session.return_code is not None else 1)

    async def command(
        self,
        instance_id: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandEvent, None]:
        """Run one command through the configured Pod data plane."""
        resource_name = sandbox_name(instance_id)
        if self.pod_agent is not None:
            endpoint = await self._ready_pod_endpoint(resource_name)
            async for event in self.pod_agent.command(endpoint.ip, resource_name, request):
                yield event
            return

        pod_name = await self._ready_pod_name(instance_id)
        command_id = f"sandbox-command-{uuid.uuid4().hex}"
        session = await self._remote_exec().open(
            pod_name,
            ["sh", "-lc", self._shell_command(request, command_id)],
        )
        stream_completed = False
        try:
            async for event in self._stream_session(session):
                yield event
            stream_completed = True
        finally:
            try:
                await session.close()
            finally:
                if not stream_completed:
                    await self._remote_exec().terminate(pod_name, command_id)

    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None:
        """Stream a bounded file upload into one sandbox."""

        async def limited_chunks() -> AsyncGenerator[bytes, None]:
            uploaded = 0
            async for chunk in chunks:
                uploaded += len(chunk)
                if uploaded > self.settings.upload_limit_bytes:
                    raise SandboxError(f"Upload exceeded {self.settings.upload_limit_bytes} bytes")
                yield chunk

        resource_name = sandbox_name(instance_id)
        if self.pod_agent is not None:
            endpoint = await self._ready_pod_endpoint(resource_name)
            await self.pod_agent.upload_file(
                endpoint.ip,
                resource_name,
                remote_path,
                limited_chunks(),
            )
            return

        pod_name = await self._ready_pod_name(instance_id)
        parent = remote_path.rpartition("/")[0] or "."
        shell_command = f"mkdir -p {shlex.quote(parent)} && base64 -d > {shlex.quote(remote_path)}"
        session = await self._remote_exec().open(pod_name, ["sh", "-lc", shell_command], stdin=True)
        try:
            async for encoded in encode_base64_chunks(limited_chunks()):
                await session.write_stdin(encoded.encode("ascii"))
            await session.close_stdin()
            async for event in self._stream_session(session):
                if isinstance(event, CommandExitEvent) and event.exit_code != 0:
                    raise SandboxError(f"Could not upload file: {remote_path}")
        finally:
            await session.close()

    async def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
        """Stream one sandbox file without buffering it in the control service."""
        resource_name = sandbox_name(instance_id)
        if self.pod_agent is not None:
            endpoint = await self._ready_pod_endpoint(resource_name)
            async for chunk in self.pod_agent.stream_download(endpoint.ip, resource_name, remote_path):
                yield chunk
            return

        pod_name = await self._ready_pod_name(instance_id)
        session = await self._remote_exec().open(
            pod_name,
            ["sh", "-lc", f"base64 {shlex.quote(remote_path)}"],
        )

        async def encoded_chunks() -> AsyncGenerator[bytes, None]:
            while session.is_open():
                await session.update(0.1)
                if stdout := await session.read_stdout():
                    yield stdout
                if stderr := await session.read_stderr():
                    raise SandboxError(f"Could not download file {remote_path}: {stderr.decode(errors='replace')}")

        try:
            async for chunk in decode_base64_chunks(encoded_chunks()):
                yield chunk
            if session.return_code not in {None, 0}:
                raise SandboxError(f"Could not download file: {remote_path}")
        finally:
            await session.close()

    async def close(self) -> None:
        """Close the configured Pod transport clients."""
        if self.remote_exec is not None:
            await self.remote_exec.close()
        if self.pod_agent is not None:
            await self.pod_agent.close()
