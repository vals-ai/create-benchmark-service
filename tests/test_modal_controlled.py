"""Exercise the Modal SDK process surface against real local process groups, without a cloud sandbox."""

import asyncio
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from grpclib import GRPCError, Status
from grpclib.exceptions import StreamTerminatedError

from benchmark_service.sandbox.modal import ModalSandbox
from benchmark_service.sandbox import modal as modal_module
from benchmark_service.sandbox.types import LINUX_PROCESS_GROUP_V1, SandboxConnectionError, SandboxError


class _LocalProcess:
    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self.stdout = self._output()
        self.wait = SimpleNamespace(aio=self._wait)

    async def _wait(self) -> int:
        while self.process.returncode is None:
            await asyncio.sleep(0.01)
        return self.process.returncode

    async def _output(self) -> AsyncGenerator[str, None]:
        assert self.process.stdout is not None
        while chunk := await self.process.stdout.read(4096):
            yield chunk.decode()


class _LocalSdkSandbox:
    object_id = "local-modal-vm"

    def __init__(self) -> None:
        self.exec = SimpleNamespace(aio=self._exec)
        self.poll = SimpleNamespace(aio=self._poll)

    async def _exec(
        self, *args: str, env: dict[str, str] | None = None, text: bool = True
    ) -> _LocalProcess:
        process = await asyncio.create_subprocess_exec(
            *args, env={**os.environ, **(env or {})},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        return _LocalProcess(process)

    async def _poll(self) -> None:
        return None


class _TransportFailingSdkSandbox(_LocalSdkSandbox):
    def __init__(self, error: Exception, *, on_wait: bool) -> None:
        super().__init__()
        self._error = error
        self._on_wait = on_wait
        self._launched = False

    async def _exec(
        self, *args: str, env: dict[str, str] | None = None, text: bool = True
    ) -> _LocalProcess:
        process = await super()._exec(*args, env=env, text=text)
        if self._launched:
            return process
        self._launched = True
        if self._on_wait:
            process.wait = SimpleNamespace(aio=self._fail_wait(process))
        else:
            process.stdout = self._fail_after_first_chunk(process.stdout)
        return process

    def _fail_wait(self, process: _LocalProcess) -> Any:
        async def wait() -> int:
            await process.process.wait()
            raise self._error

        return wait

    async def _fail_after_first_chunk(self, output: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        async for chunk in output:
            yield chunk
            raise self._error


_RAW_TRANSPORT_ERRORS = pytest.mark.parametrize(
    ("error", "expected"),
    [
        (StreamTerminatedError("Connection lost"), SandboxConnectionError),
        (ConnectionResetError("reset by peer"), SandboxConnectionError),
        (GRPCError(Status.UNAVAILABLE, "unavailable"), SandboxConnectionError),
        (GRPCError(Status.FAILED_PRECONDITION, "rejected"), SandboxError),
    ],
    ids=["stream-terminated", "socket", "grpc-unavailable", "grpc"],
)


@_RAW_TRANSPORT_ERRORS
async def test_controlled_output_maps_raw_transport_errors(error: Exception, expected: type[SandboxError]) -> None:
    vm = ModalSandbox(cast(Any, _TransportFailingSdkSandbox(error, on_wait=False)))
    workload = vm.controlled_workload('printf "started\\n"')
    chunks: list[str] = []
    try:
        with pytest.raises(expected) as raised:
            async for chunk in workload.output():
                chunks.append(chunk)
        assert type(raised.value) is expected
        assert raised.value.__cause__ is error
        assert chunks == ["started\n"]
        with pytest.raises(expected):
            await asyncio.wait_for(workload.wait(), 5)
    finally:
        await asyncio.wait_for(workload.kill(), 5)
        await asyncio.wait_for(cast(Any, workload)._cleanup_task, 5)


@_RAW_TRANSPORT_ERRORS
async def test_controlled_wait_maps_raw_transport_errors(error: Exception, expected: type[SandboxError]) -> None:
    vm = ModalSandbox(cast(Any, _TransportFailingSdkSandbox(error, on_wait=True)))
    workload = vm.controlled_workload('printf "started\\n"')
    try:
        with pytest.raises(expected) as raised:
            await asyncio.wait_for(workload.wait(), 5)
        assert type(raised.value) is expected
        assert raised.value.__cause__ is error
    finally:
        await asyncio.wait_for(workload.kill(), 5)
        await asyncio.wait_for(cast(Any, workload)._cleanup_task, 5)


@pytest.fixture
def sandbox() -> tuple[ModalSandbox, _LocalSdkSandbox]:
    sdk = _LocalSdkSandbox()
    return ModalSandbox(cast(Any, sdk)), sdk


@pytest.mark.parametrize("code", [0, 7])
async def test_natural_exit_preserves_output_exit_status_and_vm(
    sandbox: tuple[ModalSandbox, _LocalSdkSandbox], tmp_path: Path, code: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    original_cleanup = modal_module._ModalControlledWorkload._cleanup  # pyright: ignore[reportPrivateUsage]

    async def delayed_cleanup(self: modal_module._ModalControlledWorkload) -> None:  # pyright: ignore[reportPrivateUsage]
        cleanup_started.set()
        await allow_cleanup.wait()
        await original_cleanup(self)

    monkeypatch.setattr(modal_module._ModalControlledWorkload, "_cleanup", delayed_cleanup)  # pyright: ignore[reportPrivateUsage]
    vm, _ = sandbox
    assert vm.generation_containment == LINUX_PROCESS_GROUP_V1
    await vm.probe_generation_containment()
    workload = vm.controlled_workload(
        'printf "%s:%s\n" "$VALUE" "$PWD"; printf "agent-stderr\n" >&2; exit ' + str(code),
        cwd=str(tmp_path), env_vars={"VALUE": "hello"},
    )
    try:
        assert workload.agent_absence_confirmed_at is None
        waiting = asyncio.create_task(workload.wait())
        await asyncio.wait_for(cleanup_started.wait(), 5)
        receipt = workload.agent_absence_confirmed_at
        assert receipt is not None
        assert not waiting.done()
        allow_cleanup.set()
        completed = await asyncio.wait_for(waiting, 5)
        assert completed.absence_confirmed_at == receipt
        assert completed.result.exit_code == code
        assert completed.result.output == f"hello:{tmp_path}\nagent-stderr\n"
        assert "".join([chunk async for chunk in workload.output()]) == completed.result.output
        assert (await vm.exec("printf vm-alive")).output == "vm-alive"
    finally:
        allow_cleanup.set()
        await workload.kill()


async def test_deadline_kills_group_child_and_reuses_vm(
    sandbox: tuple[ModalSandbox, _LocalSdkSandbox]
) -> None:
    vm, _ = sandbox
    workload = vm.controlled_workload("sh -c 'sleep 30 & child=$!; echo child:$child; wait'")
    stream = workload.output()
    try:
        first = await asyncio.wait_for(anext(stream), 5)
        child = int(first.strip().split("child:")[1])
        assert workload.agent_absence_confirmed_at is None
        await asyncio.wait_for(workload.kill(), 5)
        receipt = workload.agent_absence_confirmed_at
        assert receipt is not None
        completed = await asyncio.wait_for(workload.wait(), 5)
        assert completed.absence_confirmed_at == receipt
        assert completed.result.exit_code != 0
        assert first in completed.result.output
        assert "Killed" not in completed.result.output
        await asyncio.wait_for(workload.kill(), 5)
        assert (await vm.exec(f"test ! -e /proc/{child}/stat || "
                              f"test $(cut -d ' ' -f 3 /proc/{child}/stat) = Z")).exit_code == 0
        assert (await vm.exec("printf vm-alive")).output == "vm-alive"
    finally:
        await asyncio.wait_for(workload.kill(), 5)
        await stream.aclose()


async def test_natural_exit_keeps_background_child_holding_stdout(
    sandbox: tuple[ModalSandbox, _LocalSdkSandbox], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(modal_module, "_OUTPUT_DRAIN_SECONDS", 0.2)
    vm, _ = sandbox
    workload = vm.controlled_workload('sleep 30 & echo "child:$!"; exit 3')
    child: int | None = None
    try:
        completed = await asyncio.wait_for(workload.wait(), 5)
        child = int(completed.result.output.strip().removeprefix("child:"))

        assert completed.result.exit_code == 3
        assert "".join([chunk async for chunk in workload.output()]) == completed.result.output
        assert (await vm.exec(f"kill -0 {child}")).exit_code == 0
    finally:
        await asyncio.wait_for(workload.kill(), 5)
        if child is not None:
            await vm.exec(f"kill -KILL {child}")


async def test_wait_and_kill_concurrently_then_early_kill_without_output(
    sandbox: tuple[ModalSandbox, _LocalSdkSandbox]
) -> None:
    vm, _ = sandbox
    workload = vm.controlled_workload('printf "started\\n"; exec sleep 30')
    output = workload.output()
    try:
        assert await asyncio.wait_for(anext(output), 5) == "started\n"
        waiting = asyncio.create_task(workload.wait())
        killing = asyncio.create_task(workload.kill())
        completed, _ = await asyncio.wait_for(asyncio.gather(waiting, killing), 5)
        assert completed.result.exit_code != 0
    finally:
        await asyncio.wait_for(workload.kill(), 5)
        await output.aclose()
    early = vm.controlled_workload("exec sleep 30")
    try:
        await asyncio.wait_for(early.kill(), 5)
        assert (await asyncio.wait_for(early.wait(), 5)).result.exit_code != 0
        assert (await vm.exec("printf reused")).output == "reused"
    finally:
        await asyncio.wait_for(early.kill(), 5)


async def test_active_consumer_gets_full_stream_with_bounded_result_tail(
    sandbox: tuple[ModalSandbox, _LocalSdkSandbox]
) -> None:
    vm, _ = sandbox
    workload = vm.controlled_workload(
        "python3 -c 'import sys; sys.stdout.write(\"x\" * 2000000 + \"final frame\")'"
    )

    async def consume() -> str:
        return "".join([chunk async for chunk in workload.output()])

    streamed, completed = await asyncio.wait_for(
        asyncio.gather(consume(), workload.wait()), 10
    )
    expected = "x" * 2_000_000 + "final frame"
    assert streamed == expected
    assert completed.result.exit_code == 0
    assert 0 < len(completed.result.output) < len(expected)
    assert completed.result.output == expected[-len(completed.result.output):]
