"""Real local process-group regressions for the Compose controlled workload adapter."""

import asyncio
import os
from pathlib import Path
from typing import cast

import pytest

from benchmark_service.sandbox import (
    ComposeSandbox,
    ComposeSource,
    ControlledWorkload,
    ControlledWorkloadResult,
    ExecResult,
    ImageSource,
    LINUX_PROCESS_GROUP_V1,
    Sandbox,
    SandboxError,
)


_DOCKER_SHIM = """#!/bin/sh
if [ "$1" = compose ]; then
    shift
    if [ "$1" = ps ]; then
        [ "$2" = -q ] && [ "$3" = main ] || exit 2
        printf 'local-service\n'
        exit 0
    fi
fi
[ "$1" = exec ] || exit 2
shift
while [ "$1" = -e ] || [ "$1" = -w ] || [ "$1" = -T ]; do
    option=$1
    shift
    if [ "$option" = -T ]; then continue; fi
    if [ "$option" = -w ]; then cd "$1" || exit 2; fi
    shift
done
case "$1" in main|local-service) ;; *) exit 2 ;; esac
shift
exec "$@"
"""


class _LocalWorkload(ControlledWorkload):
    def __init__(self, command: str, env: dict[str, str]) -> None:
        self._command = command
        self._env = env
        self._chunks: asyncio.Queue[str | None] = asyncio.Queue()
        self._started = asyncio.Event()
        self._process: asyncio.subprocess.Process
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> ExecResult:
        self._process = await asyncio.create_subprocess_shell(
            self._command,
            env=self._env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        self._started.set()
        assert self._process.stdout is not None
        chunks: list[str] = []
        while chunk := await self._process.stdout.read(4096):
            text = chunk.decode()
            chunks.append(text)
            await self._chunks.put(text)
        code = await self._process.wait()
        await self._chunks.put(None)
        return ExecResult(exit_code=code, output="".join(chunks))

    async def output(self):
        while (chunk := await self._chunks.get()) is not None:
            yield chunk

    async def wait(self) -> ControlledWorkloadResult:
        result = await self._task
        return ControlledWorkloadResult(result, asyncio.get_running_loop().time())

    async def kill(self) -> None:
        await self._started.wait()
        if self._process.returncode is None:
            self._process.kill()
        await self._process.wait()


class _LocalOuter:
    def __init__(self, docker_path: Path) -> None:
        self.env = {"PATH": f"{docker_path}:{os.environ['PATH']}", "HOME": str(docker_path)}
        self.labels = None
        self.created_at = None
        self.id = "outer"
        self.name = "outer"
        self.state = "running"

    async def probe_generation_containment(self) -> None:
        return None


    async def exec(self, command: str, *, timeout: float | None = None) -> ExecResult:
        process = await asyncio.create_subprocess_shell(
            command,
            env=self.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
        return ExecResult(exit_code=await process.wait(), output=output.decode())

    def controlled_workload(
        self, command: str, *, cwd: str | None = None, env_vars: dict[str, str] | None = None
    ) -> ControlledWorkload:
        return _LocalWorkload(command, {**self.env, **(env_vars or {})})

    @property
    def provider_metadata(self) -> dict[str, str]:
        return {}


@pytest.fixture
def compose_sandbox(tmp_path: Path) -> ComposeSandbox:
    docker = tmp_path / "docker"
    docker.write_text(_DOCKER_SHIM)
    docker.chmod(0o700)
    return ComposeSandbox(
        cast(Sandbox, _LocalOuter(tmp_path)),
        ComposeSource(outer=ImageSource(image="local")),
    )


@pytest.mark.parametrize("code", [0, 7])
async def test_compose_controlled_natural_exit_preserves_status_and_output(
    compose_sandbox: ComposeSandbox, code: int
) -> None:
    assert compose_sandbox.generation_containment == LINUX_PROCESS_GROUP_V1
    await compose_sandbox.probe_generation_containment()
    workload = compose_sandbox.controlled_workload(f'printf "agent output\\n"; exit {code}')
    try:
        completed = await asyncio.wait_for(workload.wait(), 5)
        output = "".join([chunk async for chunk in workload.output()])

        assert completed.result.exit_code == code
        assert completed.result.output == "agent output\n"
        assert output == completed.result.output
    finally:
        await asyncio.wait_for(workload.kill(), 5)



@pytest.mark.parametrize("code", [0, 7])
async def test_compose_foreground_exit_stops_child_holding_stdout(
    compose_sandbox: ComposeSandbox, code: int
) -> None:
    workload = compose_sandbox.controlled_workload(
        f'sleep 30 & child=$!; printf "child:%s\nparentcomplete\n" "$child"; exit {code}'
    )
    try:
        completed = await asyncio.wait_for(workload.wait(), 5)
        lines = completed.result.output.splitlines()
        child = int(lines[0].removeprefix("child:"))
        assert completed.result.exit_code == code
        assert lines[1] == "parentcomplete"
        assert "".join([chunk async for chunk in workload.output()]) == completed.result.output
        assert (
            await compose_sandbox.exec(
                f"test ! -e /proc/{child}/stat || "
                f"test $(cut -d ' ' -f 3 /proc/{child}/stat) = Z"
            )
        ).exit_code == 0
        assert (await compose_sandbox.exec("printf service-alive")).output == "service-alive"
    finally:
        await asyncio.wait_for(workload.kill(), 5)


async def test_compose_controlled_deadline_stops_group_but_keeps_service(
    compose_sandbox: ComposeSandbox,
) -> None:
    service = await asyncio.create_subprocess_exec("sleep", "30", start_new_session=True)
    try:
        workload = compose_sandbox.controlled_workload('printf "started\n"; sh -c "sleep 10 & wait"')
        try:
            stream = workload.output()
            try:
                first = await asyncio.wait_for(anext(stream), 5)
                assert "started\n" in first
                await asyncio.wait_for(workload.kill(), 5)
                await asyncio.wait_for(workload.kill(), 5)
                assert service.returncode is None
                assert (await compose_sandbox.exec("printf service-alive")).output == "service-alive"
                completed = await asyncio.wait_for(workload.wait(), 5)
                assert completed.result.exit_code != 0
                assert "started\n" in completed.result.output
            finally:
                await stream.aclose()
        finally:
            await asyncio.wait_for(workload.kill(), 5)
    finally:
        service.terminate()
        await service.wait()


async def test_compose_controlled_kill_before_launch_does_not_wait_for_output(
    compose_sandbox: ComposeSandbox,
) -> None:
    workload = compose_sandbox.controlled_workload("exec sleep 10")
    try:
        await asyncio.wait_for(workload.kill(), 5)
        assert (await compose_sandbox.exec("printf service-alive")).output == "service-alive"
    finally:
        await asyncio.wait_for(workload.kill(), 5)


async def test_compose_client_exits_before_service_admission_cannot_confirm_stop(
    tmp_path: Path,
) -> None:
    docker = tmp_path / "docker"
    docker.write_text(_DOCKER_SHIM)
    docker.chmod(0o700)

    class LostClientOuter(_LocalOuter):
        def controlled_workload(
            self, command: str, *, cwd: str | None = None, env_vars: dict[str, str] | None = None
        ) -> ControlledWorkload:
            return _LocalWorkload("exit 137", {**self.env, **(env_vars or {})})

    sandbox = ComposeSandbox(
        cast(Sandbox, LostClientOuter(tmp_path)),
        ComposeSource(outer=ImageSource(image="local")),
    )
    workload = sandbox.controlled_workload("printf must-not-run")

    with pytest.raises(SandboxError, match="without a service process-group marker"):
        await asyncio.wait_for(workload.kill(), 5)
    with pytest.raises(SandboxError, match="without a service process-group marker"):
        await asyncio.wait_for(workload.wait(), 5)
    assert (await sandbox.exec("printf service-alive")).output == "service-alive"
