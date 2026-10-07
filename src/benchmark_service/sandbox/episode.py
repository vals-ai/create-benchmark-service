"""CBS-owned whole-episode process supervision for process-group sandboxes."""

from __future__ import annotations

import asyncio
import shlex
import uuid
from collections.abc import AsyncGenerator, Mapping
from pathlib import Path

from benchmark_service.sandbox.types import (
    ControlledWorkload,
    ControlledWorkloadResult,
    LINUX_CGROUP_V2_V1,
    LINUX_PROCESS_GROUP_V1,
    Sandbox,
    SandboxError,
)


_EPISODE_PYTHON = "python3"


async def controlled_episode_workload(
    sandbox: Sandbox,
    command: str,
    *,
    cwd: str | None = None,
    env_vars: Mapping[str, str] | None = None,
) -> ControlledWorkload:
    """Prepare supervision before constructing the native workload; never yield afterwards."""
    if sandbox.generation_containment == LINUX_CGROUP_V2_V1:
        return sandbox.controlled_workload(command, cwd=cwd, env_vars=env_vars)
    if sandbox.generation_containment != LINUX_PROCESS_GROUP_V1:
        raise SandboxError("Episode requires supported generation containment")
    directory = f"/tmp/cbs-episode-{uuid.uuid4().hex}"
    result = await sandbox.exec(f"mkdir {shlex.quote(directory)}")
    if result.exit_code:
        raise SandboxError(f"Episode directory preparation failed: {result.output}")
    script = f"{directory}/supervisor.py"
    await sandbox.upload_file(script, Path(__file__).with_name("episode_supervisor.py").read_bytes())
    launch = shlex.join([_EPISODE_PYTHON, script, directory, command])
    native = sandbox.controlled_workload(launch, cwd=cwd, env_vars=env_vars)
    return _EpisodeWorkload(sandbox, native, directory)


class _EpisodeWorkload(ControlledWorkload):
    def __init__(self, sandbox: Sandbox, native: ControlledWorkload, directory: str):
        self._sandbox = sandbox
        self._native = native
        self._directory = directory
        self._native_wait = asyncio.create_task(native.wait())
        self._stop_task: asyncio.Task[None] | None = None

    async def _state(self) -> str:
        directory = shlex.quote(self._directory)
        result = await self._sandbox.exec(
            f"if test -f {directory}/DRAINED; then echo DRAINED; "
            f"elif test -f {directory}/READY; then echo READY; else echo PENDING; fi"
        )
        if result.exit_code:
            raise SandboxError(f"Episode state lookup failed: {result.output}")
        return result.output.strip()

    async def output(self) -> AsyncGenerator[str, None]:
        async for chunk in self._native.output():
            yield chunk

    async def wait(self) -> ControlledWorkloadResult:
        result = await asyncio.shield(self._native_wait)
        state = await self._state()
        if state == "DRAINED":
            return result
        if state == "PENDING" and result.result.exit_code != 0:
            # READY precedes Popen; an absent READY means the launch never began.
            return result
        raise SandboxError("Episode native owner absent without confirmed descendant drain")

    async def _stop(self) -> None:
        while True:
            state = await self._state()
            if state == "DRAINED":
                break
            if self._native_wait.done():
                # Recheck state after native completion; the earlier snapshot can predate READY.
                await self.wait()
                return
            if state == "PENDING":
                await asyncio.sleep(0.05)
                continue
            client = (
                "import socket,sys\n"
                "s=socket.socket(socket.AF_UNIX)\n"
                "try:\n"
                "    s.connect(sys.argv[1])\n"
                "    s.sendall(b'STOP')\n"
                "    print(s.recv(7).decode())\n"
                "except (ConnectionRefusedError, FileNotFoundError, BrokenPipeError, ConnectionResetError):\n"
                "    print('RACED')\n"
                "s.close()\n"
            )
            control = shlex.join([_EPISODE_PYTHON, "-c", client, f"{self._directory}/control"])
            request = await self._sandbox.exec(control)
            state = await self._state()
            if state == "DRAINED":
                break
            raise SandboxError(f"Episode stop did not confirm descendant drain: {request.output}")
        await self._native.kill()

    async def kill(self) -> None:
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
        await asyncio.shield(self._stop_task)
