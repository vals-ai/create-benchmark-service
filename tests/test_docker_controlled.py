"""Exercise Docker's controlled exec lifecycle against real local process groups without a daemon."""

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from benchmark_service.sandbox._process_group import cleanup_command, stop_command
from benchmark_service.sandbox.local.docker import DockerSandboxProvider
from benchmark_service.sandbox.types import Sandbox, SandboxConnectionError, SandboxError


class _LocalStream:
    def __init__(self, execution: "_LocalExec", chunk_size: int) -> None:
        self.execution = execution
        self._chunk_size = chunk_size
        self._reads: dict[int, asyncio.Task[bytes]] = {}
        self._finished: set[int] = set()

    async def __aenter__(self) -> "_LocalStream":
        await self._start()
        return self

    async def _start(self) -> None:
        if self.execution.process is not None:
            return
        self.execution.process = await asyncio.create_subprocess_exec(
            *self.execution.command,
            cwd=self.execution.workdir,
            env={**os.environ, **self.execution.environment},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def read_out(self) -> SimpleNamespace | None:
        await self._start()
        process = self.execution.process
        assert process is not None and process.stdout is not None and process.stderr is not None
        for channel, pipe in ((1, process.stdout), (2, process.stderr)):
            if channel not in self._finished and channel not in self._reads:
                self._reads[channel] = asyncio.create_task(pipe.read(self._chunk_size))
        while self._reads:
            done, _ = await asyncio.wait(self._reads.values(), return_when=asyncio.FIRST_COMPLETED)
            for channel, task in list(self._reads.items()):
                if task in done:
                    del self._reads[channel]
                    data = task.result()
                    if data:
                        return SimpleNamespace(stream=channel, data=data)
                    self._finished.add(channel)
        return None

    async def close(self) -> None:
        process = self.execution.process
        for task in self._reads.values():
            task.cancel()
        if self._reads:
            await asyncio.gather(*self._reads.values(), return_exceptions=True)
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
        return {
            "Id": "local-proof",
            "Created": "2026-01-01T00:00:00Z",
            "State": {"Status": "running"},
            "Config": {"Labels": {"io.vals.cbs.managed": "true"}},
        }


class _LocalContainers:
    def __init__(self, container: _LocalContainer) -> None:
        self._container = container

    def container(self, _instance_id: str) -> _LocalContainer:
        return self._container


async def _sandbox(container: _LocalContainer) -> Sandbox:
    client = SimpleNamespace(containers=_LocalContainers(container))
    with patch("benchmark_service.sandbox.local.docker.Docker", return_value=client):
        provider = DockerSandboxProvider()
    return await provider.get_sandbox("local-proof")


def _record_control_dirs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from benchmark_service.sandbox.local import docker

    observed: list[str] = []
    original = docker.stop_command

    def record(group_id: int, control_dir: str) -> str:
        observed.append(control_dir)
        return original(group_id, control_dir)

    monkeypatch.setattr(docker, "stop_command", record)
    return observed


async def _assert_control_cleaned(container: _LocalContainer, control_dir: str) -> None:
    script = ["/bin/sh", "-c", cleanup_command(control_dir)]
    deadline = asyncio.get_running_loop().time() + 3
    while True:
        for execution in container.executions:
            if execution.command == script and execution.process is not None:
                assert await asyncio.wait_for(execution.process.wait(), 3) == 0
                assert not Path(control_dir).exists()
                return
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)


async def test_controlled_status_stream_utf8_and_container_reuse(tmp_path: Path) -> None:
    container = _LocalContainer(chunk_size=1)
    sandbox = await _sandbox(container)
    await sandbox.probe_generation_containment()
    workload = sandbox.controlled_workload(
        "printf '\\316'; printf ' stderr' >&2; sleep 0.05; printf '\\261'; exit 7",
        cwd=str(tmp_path),
    )
    output = "".join([chunk async for chunk in workload.output()])
    result = await workload.wait()
    assert result.result.exit_code == 7
    assert output == result.result.output
    assert output.count("α") == 1
    assert output.replace("α", "") == " stderr"
    assert result.absence_confirmed_at <= asyncio.get_running_loop().time()
    assert (await sandbox.exec("pwd", cwd=str(tmp_path))).output.strip() == str(tmp_path)


async def test_controlled_stream_arrives_before_status_finishes() -> None:
    sandbox = await _sandbox(_LocalContainer())
    workload = sandbox.controlled_workload("printf ready; sleep 0.2; printf later")
    stream = workload.output()
    assert await asyncio.wait_for(anext(stream), timeout=1) == "ready"
    assert "".join([chunk async for chunk in stream]) == "later"
    assert (await workload.wait()).result.output == "readylater"



async def test_controlled_read_failure_stops_running_group(monkeypatch: pytest.MonkeyPatch) -> None:
    read_out = _LocalStream.read_out

    async def fail_read(stream: _LocalStream) -> SimpleNamespace | None:
        if stream.execution is container.executions[0]:
            raise OSError("read transport lost")
        return await read_out(stream)

    monkeypatch.setattr(_LocalStream, "read_out", fail_read)
    container = _LocalContainer()
    control_dirs = _record_control_dirs(monkeypatch)
    workload = (await _sandbox(container)).controlled_workload("sleep 1.5")
    try:
        with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
            await asyncio.wait_for(anext(workload.output(), None), timeout=1)
        with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
            await asyncio.wait_for(workload.wait(), timeout=1)
        assert container.executions[0].process is not None
        assert container.executions[0].process.returncode is not None
    finally:
        await workload.kill()
        await _assert_control_cleaned(container, control_dirs[-1])


async def test_controlled_inspect_failure_during_startup_stops_group(monkeypatch: pytest.MonkeyPatch) -> None:
    create_exec = _LocalContainer.exec
    inspect = _LocalExec.inspect

    async def delayed_launch(container: _LocalContainer, command: list[str], **kwargs: Any) -> _LocalExec:
        if not container.executions:
            command = [*command[:-1], "sleep 0.3; " + command[-1]]
        return await create_exec(container, command, **kwargs)

    async def fail_inspect(execution: _LocalExec) -> dict[str, Any]:
        if execution is container.executions[0]:
            raise OSError("inspect transport lost")
        return await inspect(execution)

    monkeypatch.setattr(_LocalContainer, "exec", delayed_launch)
    monkeypatch.setattr(_LocalExec, "inspect", fail_inspect)
    container = _LocalContainer()
    control_dirs = _record_control_dirs(monkeypatch)
    workload = (await _sandbox(container)).controlled_workload("sleep 1.5")
    reader = asyncio.create_task(anext(workload.output(), None))
    try:
        with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
            await asyncio.wait_for(workload.wait(), timeout=0.2)
        with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
            await asyncio.wait_for(reader, timeout=0.2)
        await asyncio.wait_for(workload.kill(), timeout=1)
        assert container.executions[0].process is not None
        assert container.executions[0].process.returncode is not None
    finally:
        await asyncio.wait_for(workload.kill(), timeout=3)
        await _assert_control_cleaned(container, control_dirs[-1])


async def test_controlled_wait_keeps_delayed_final_output(monkeypatch: pytest.MonkeyPatch) -> None:
    read_out = _LocalStream.read_out

    async def delayed_read(stream: _LocalStream) -> SimpleNamespace | None:
        message = await read_out(stream)
        if message is not None and stream.execution is container.executions[0]:
            await asyncio.sleep(1.5)
        return message

    monkeypatch.setattr(_LocalStream, "read_out", delayed_read)
    container = _LocalContainer()
    sandbox = await _sandbox(container)
    workload = sandbox.controlled_workload("printf tail; exit 7")

    async def collect() -> str:
        return "".join([chunk async for chunk in workload.output()])

    output = asyncio.create_task(collect())
    result = await asyncio.wait_for(workload.wait(), timeout=4)
    assert result.result.exit_code == 7
    assert result.result.output == "tail"
    assert await output == "tail"


async def test_controlled_kill_preserves_output_during_delayed_drain(monkeypatch: pytest.MonkeyPatch) -> None:
    container = _LocalContainer()
    sandbox = await _sandbox(container)
    read_out = _LocalStream.read_out
    captured = asyncio.Event()
    release = asyncio.Event()

    async def delayed_read(stream: _LocalStream) -> SimpleNamespace | None:
        message = await read_out(stream)
        if message is not None and stream.execution is container.executions[0]:
            captured.set()
            await release.wait()
        return message

    monkeypatch.setattr(_LocalStream, "read_out", delayed_read)
    control_dirs = _record_control_dirs(monkeypatch)
    workload = sandbox.controlled_workload("printf tail; sleep 30")
    try:
        await asyncio.wait_for(captured.wait(), 3)
        await asyncio.wait_for(workload.kill(), 3)
        process = container.executions[0].process
        assert process is not None and process.returncode is not None
        control_dir = control_dirs[-1]
        assert Path(control_dir).is_dir()
        release.set()
        result = await asyncio.wait_for(workload.wait(), 3)
        assert result.result.output == "tail"
        await _assert_control_cleaned(container, control_dir)
    finally:
        release.set()
        await asyncio.wait_for(workload.kill(), 3)


async def test_controlled_kill_before_output_stops_group_but_keeps_container(tmp_path: Path) -> None:
    container = _LocalContainer()
    sandbox = await _sandbox(container)
    sentinel = str(tmp_path) + "/child-finished"
    command = f"(sleep 2; touch {sentinel}) & sleep 2"
    workload = sandbox.controlled_workload(command)

    async def collect() -> str:
        return "".join([chunk async for chunk in workload.output()])

    reader = asyncio.create_task(collect())
    try:
        await asyncio.wait_for(workload.kill(), timeout=3)
        await workload.kill()
        result = await asyncio.wait_for(workload.wait(), timeout=3)
        assert await asyncio.wait_for(reader, timeout=3) == result.result.output
        assert result.result.exit_code != 0
        assert result.absence_confirmed_at <= asyncio.get_running_loop().time()
        await asyncio.sleep(2.1)
        assert (await sandbox.exec(f"test ! -e {sentinel}")).exit_code == 0
        assert (await sandbox.exec("printf usable")).output == "usable"
    finally:
        await asyncio.wait_for(workload.kill(), 3)
        if not reader.done():
            await reader


async def test_controlled_wait_cancellation_does_not_cancel_command() -> None:
    sandbox = await _sandbox(_LocalContainer())
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
    sandbox = await _sandbox(_LocalContainer())
    workload = sandbox.controlled_workload("head -c 80000 /dev/zero | tr '\\000' x; printf 'final frame'")
    result = await asyncio.wait_for(workload.wait(), timeout=3)
    streamed = "".join([chunk async for chunk in workload.output()])
    expected = "x" * 80_000 + "final frame"
    assert result.result.exit_code == 0
    assert streamed == expected
    assert 0 < len(result.result.output) < len(expected)
    assert result.result.output == expected[-len(result.result.output):]


async def test_controlled_launch_failure_wakes_output_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    container = _LocalContainer()

    async def fail_launch(*_args: object, **_kwargs: object) -> None:
        raise OSError("local transport unavailable")

    monkeypatch.setattr(container, "exec", fail_launch)
    workload = (await _sandbox(container)).controlled_workload("printf never-started")
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await asyncio.wait_for(anext(workload.output(), None), timeout=1)
    with pytest.raises(SandboxConnectionError, match="Local Docker connection failed"):
        await workload.wait()


async def test_controlled_unconfirmed_group_absence_is_error(monkeypatch: pytest.MonkeyPatch) -> None:
    container = _LocalContainer()
    sandbox = await _sandbox(container)
    confirmations: list[tuple[int, str]] = []
    workload = sandbox.controlled_workload("sleep 2")
    from benchmark_service.sandbox.local import docker

    def fail_confirmation(group_id: int, control_dir: str) -> str:
        confirmations.append((group_id, control_dir))
        return "exit 1"

    monkeypatch.setattr(docker, "stop_command", fail_confirmation)
    try:
        with pytest.raises(SandboxError, match="process group did not stop"):
            await asyncio.wait_for(workload.kill(), timeout=3)
    finally:
        group_id, control_dir = confirmations[-1]
        confirmed = await sandbox.exec(stop_command(group_id, control_dir))
        assert confirmed.exit_code == 0
        try:
            with pytest.raises(SandboxError, match="process group did not stop"):
                await asyncio.wait_for(workload.wait(), timeout=3)
        finally:
            process = container.executions[0].process
            assert process is not None
            await asyncio.wait_for(process.wait(), 3)
            removed = await sandbox.exec(cleanup_command(control_dir))
            assert removed.exit_code == 0
