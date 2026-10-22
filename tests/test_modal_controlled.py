"""Exercise the Modal SDK process surface against real local process groups, without a cloud sandbox."""

import asyncio
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from benchmark_service.sandbox.modal import ModalSandbox
from benchmark_service.sandbox.types import LINUX_PROCESS_GROUP_V1


class _LocalProcess:
    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self.stdout = self._output()
        self.wait = SimpleNamespace(aio=self.process.wait)

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


@pytest.fixture
def sandbox() -> tuple[ModalSandbox, _LocalSdkSandbox]:
    sdk = _LocalSdkSandbox()
    return ModalSandbox(cast(Any, sdk)), sdk


@pytest.mark.parametrize("code", [0, 7])
async def test_natural_exit_preserves_output_exit_status_and_vm(
    sandbox: tuple[ModalSandbox, _LocalSdkSandbox], tmp_path: Path, code: int
) -> None:
    vm, _ = sandbox
    assert vm.generation_containment == LINUX_PROCESS_GROUP_V1
    await vm.probe_generation_containment()
    workload = vm.controlled_workload(
        'printf "%s:%s\n" "$VALUE" "$PWD"; printf "agent-stderr\n" >&2; exit ' + str(code),
        cwd=str(tmp_path), env_vars={"VALUE": "hello"},
    )
    try:
        completed = await asyncio.wait_for(workload.wait(), 5)
        assert completed.result.exit_code == code
        assert completed.result.output == f"hello:{tmp_path}\nagent-stderr\n"
        assert "".join([chunk async for chunk in workload.output()]) == completed.result.output
        assert (await vm.exec("printf vm-alive")).output == "vm-alive"
    finally:
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
        await asyncio.wait_for(workload.kill(), 5)
        completed = await asyncio.wait_for(workload.wait(), 5)
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
