"""Exercise Docker's controlled exec lifecycle against real local process groups without a daemon."""

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmark_service.sandbox.local.docker import DockerSandbox, _ContainerInfo
from benchmark_service.sandbox.types import SandboxConnectionError, SandboxError


class _LocalStream:
    def __init__(self, execution: "_LocalExec", chunk_size: int) -> None:
        self._execution = execution
        self._chunk_size = chunk_size

    async def __aenter__(self) -> "_LocalStream":
        await self._start()
        return self

    async def _start(self) -> None:
        if self._execution.process is not None:
            return
        self._execution.process = await asyncio.create_subprocess_exec(
            *self._execution.command,
            cwd=self._execution.workdir,
            env={**os.environ, **self._execution.environment},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def read_out(self) -> SimpleNamespace | None:
        await self._start()
        process = self._execution.process
        assert process is not None and process.stdout is not None
        chunk = await process.stdout.read(self._chunk_size)
        return SimpleNamespace(data=chunk) if chunk else None

    async def close(self) -> None:
        process = self._execution.process
        if process is not None and process.returncode is not None:
            await process.wait()


class _LocalExec:
    def __init__(self, command: list[str], environment: dict[str, str], workdir: str | None, chunk_size: int) -> None:
        self.command = command
        self.environment = environment
        self.workdir = workdir
        self.chunk_size = chunk_size
        self.process: asyncio.subprocess.Process | None = None

    def start(self) -> _LocalStream:
        return _LocalStream(self, self.chunk_size)

    async def inspect(self) -> dict[str, Any]:
        process = self.process
        assert process is not None
        return {"Running": process.returncode is None, "ExitCode": process.returncode}


class _LocalContainer:
    def __init__(self, chunk_size: int = 4096) -> None:
        self.chunk_size = chunk_size
        self.executions: list[_LocalExec] = []

    async def exec(
        self, command: list[str], *, environment: dict[str, str] | None = None, workdir: str | None = None
    ) -> _LocalExec:
        execution = _LocalExec(command, environment or {}, workdir, self.chunk_size)
        self.executions.append(execution)
        return execution

    async def show(self) -> dict[str, Any]:
        return {"Id": "local-proof", "Created": "2026-01-01T00:00:00Z", "State": {"Status": "running"}, "Config": {"Labels": {}}}


def _sandbox(container: _LocalContainer) -> DockerSandbox:
    info = _ContainerInfo.model_validate(
        {"Id": "local-proof", "Created": "2026-01-01T00:00:00Z", "State": {"Status": "running"}, "Config": {"Labels": {}}}
    )
    return DockerSandbox(container, info)  # type: ignore[arg-type]


async def test_controlled_status_stream_utf8_and_container_reuse(tmp_path: Path) -> None:
    container = _LocalContainer(chunk_size=1)
    sandbox = _sandbox(container)
    await sandbox.probe_generation_containment()
    workload = sandbox.controlled_workload("printf '\\316\\261'; printf ' stderr' >&2; exit 7", cwd=str(tmp_path))
    output = "".join([chunk async for chunk in workload.output()])
    result = await workload.wait()
    assert result.result.exit_code == 7
    assert output == "α stderr" == result.result.output
    assert result.absence_confirmed_at <= asyncio.get_running_loop().time()
    assert (await sandbox.exec("pwd", cwd=str(tmp_path))).output.strip() == str(tmp_path)


async def test_controlled_stream_arrives_before_status_finishes() -> None:
    sandbox = _sandbox(_LocalContainer())
    workload = sandbox.controlled_workload("printf ready; sleep 0.2; printf later")
    stream = workload.output()
    assert await asyncio.wait_for(anext(stream), timeout=1) == "ready"
    assert "".join([chunk async for chunk in stream]) == "later"
    assert (await workload.wait()).result.output == "readylater"


async def test_controlled_output_eof_before_command_exits() -> None:
    workload = _sandbox(_LocalContainer()).controlled_workload("exec 1>&- 2>&-; sleep 0.5")
    stream = workload.output()
    assert await asyncio.wait_for(anext(stream, None), timeout=0.2) is None
    assert (await asyncio.wait_for(workload.wait(), timeout=2)).result.exit_code == 0


async def test_controlled_read_failure_stops_running_group(monkeypatch: pytest.MonkeyPatch) -> None:
    read_out = _LocalStream.read_out

    async def fail_read(stream: _LocalStream) -> SimpleNamespace | None:
        if stream._execution.command[0] == "setsid":
            raise OSError("read transport lost")
        return await read_out(stream)

    monkeypatch.setattr(_LocalStream, "read_out", fail_read)
    container = _LocalContainer()
    workload = _sandbox(container).controlled_workload("sleep 1.5")
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await asyncio.wait_for(anext(workload.output(), None), timeout=1)
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await asyncio.wait_for(workload.wait(), timeout=1)
    assert container.executions[0].process is not None
    assert container.executions[0].process.returncode is not None
    await workload.kill()


async def test_controlled_inspect_failure_before_marker_stops_group(monkeypatch: pytest.MonkeyPatch) -> None:
    create_exec = _LocalContainer.exec
    inspect = _LocalExec.inspect

    async def delayed_marker(container: _LocalContainer, command: list[str], **kwargs: Any) -> _LocalExec:
        if command[0] == "setsid":
            command = [*command[:-1], "sleep 0.3; " + command[-1]]
        return await create_exec(container, command, **kwargs)

    async def fail_inspect(execution: _LocalExec) -> dict[str, Any]:
        if execution.command[0] == "setsid":
            raise OSError("inspect transport lost")
        return await inspect(execution)

    monkeypatch.setattr(_LocalContainer, "exec", delayed_marker)
    monkeypatch.setattr(_LocalExec, "inspect", fail_inspect)
    container = _LocalContainer()
    workload = _sandbox(container).controlled_workload("sleep 1.5")
    reader = asyncio.create_task(anext(workload.output(), None))
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await asyncio.wait_for(workload.wait(), timeout=0.2)
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await asyncio.wait_for(reader, timeout=0.2)
    await asyncio.wait_for(workload.kill(), timeout=1)
    assert container.executions[0].process is not None
    assert container.executions[0].process.returncode is not None


async def test_controlled_wait_keeps_delayed_final_output(monkeypatch: pytest.MonkeyPatch) -> None:
    read_out = _LocalStream.read_out

    async def delayed_read(stream: _LocalStream) -> SimpleNamespace | None:
        message = await read_out(stream)
        if message is not None and stream._execution.command[0] == "setsid":
            await asyncio.sleep(1.5)
        return message

    monkeypatch.setattr(_LocalStream, "read_out", delayed_read)
    sandbox = _sandbox(_LocalContainer())
    workload = sandbox.controlled_workload("printf tail; exit 7")

    async def collect() -> str:
        return "".join([chunk async for chunk in workload.output()])

    output = asyncio.create_task(collect())
    result = await asyncio.wait_for(workload.wait(), timeout=4)
    assert result.result.exit_code == 7
    assert result.result.output == "tail"
    assert await output == "tail"


async def test_controlled_kill_before_output_stops_group_but_keeps_container(tmp_path: Path) -> None:
    sandbox = _sandbox(_LocalContainer())
    sentinel = str(tmp_path) + "/child-finished"
    command = f"(sleep 2; touch {sentinel}) & sleep 2"
    workload = sandbox.controlled_workload(command)
    reader = asyncio.create_task(anext(workload.output(), None))
    await asyncio.wait_for(workload.kill(), timeout=3)
    await workload.kill()
    assert await asyncio.wait_for(reader, timeout=3) is None
    result = await asyncio.wait_for(workload.wait(), timeout=3)
    assert result.result.exit_code != 0
    assert result.absence_confirmed_at <= asyncio.get_running_loop().time()
    assert result.result.output == ""
    assert (await sandbox.exec(f"test ! -e {sentinel}")).exit_code == 0
    assert (await sandbox.exec("printf usable")).output == "usable"


async def test_controlled_wait_cancellation_does_not_cancel_command() -> None:
    sandbox = _sandbox(_LocalContainer())
    workload = sandbox.controlled_workload('sleep 0.2; printf %s "$CUSTOM"', env_vars={"CUSTOM": "ok"})
    pending = asyncio.create_task(workload.wait())
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    result = await asyncio.wait_for(workload.wait(), timeout=3)
    assert result.result.exit_code == 0
    assert result.result.output == "ok"
    assert "".join([chunk async for chunk in workload.output()]) == "ok"


async def test_controlled_result_tail_is_finite_without_output_consumer() -> None:
    sandbox = _sandbox(_LocalContainer())
    workload = sandbox.controlled_workload("head -c 80000 /dev/zero | tr '\\000' x")
    result = await asyncio.wait_for(workload.wait(), timeout=3)
    assert result.result.exit_code == 0
    assert result.result.output == "x" * (64 * 1024)


async def test_controlled_launch_failure_wakes_output_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    container = _LocalContainer()

    async def fail_launch(*_args: object, **_kwargs: object) -> None:
        raise OSError("local transport unavailable")

    monkeypatch.setattr(container, "exec", fail_launch)
    workload = _sandbox(container).controlled_workload("printf never-started")
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await asyncio.wait_for(anext(workload.output(), None), timeout=1)
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await workload.wait()


async def test_controlled_unconfirmed_group_absence_is_error(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox = _sandbox(_LocalContainer())
    workload = sandbox.controlled_workload("sleep 2")
    from benchmark_service.sandbox.local import docker

    monkeypatch.setattr(docker, "stop_command", lambda *_args: "exit 1")
    with pytest.raises(SandboxError, match="process group did not stop"):
        await asyncio.wait_for(workload.kill(), timeout=3)
    # A failed confirmation leaves the process alive; this test-created group is cleaned up locally.
    marker = workload._marker  # pyright: ignore[reportPrivateUsage]
    group_id = int((await sandbox.exec(f"cat {marker}")).output.strip())
    assert (await sandbox.exec(f"kill -s KILL -- -{group_id}")).exit_code == 0
    await sandbox.exec(f"rm -f {marker}")
    with pytest.raises(SandboxError, match="process group did not stop"):
        await asyncio.wait_for(workload.wait(), timeout=3)
