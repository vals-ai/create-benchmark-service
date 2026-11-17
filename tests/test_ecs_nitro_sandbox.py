from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from ecs_nitro_agents import free_port
from ecs_nitro_fakes import FakeEc2, FakeEcs, make_config, run_task_params

from benchmark_service.sandbox.ecs_nitro.network import SecurityGroups
from benchmark_service.sandbox.ecs_nitro.sandbox import EcsNitroSandbox
from benchmark_service.sandbox.types import SandboxError, SandboxNotFoundError


@pytest.mark.asyncio
async def test_stopped_task_reports_sandbox_loss() -> None:
    ecs = FakeEcs()
    config = make_config(free_port())

    async def describe(task_arn: str) -> Mapping[str, Any] | None:
        return ecs.tasks.get(task_arn.rsplit("/", 1)[-1])

    def sandbox(task: Mapping[str, Any]) -> EcsNitroSandbox:
        network = SecurityGroups(config, FakeEc2(), poll_seconds=0.01, delete_timeout=1)
        return EcsNitroSandbox(task, config=config, network=network, describe=describe)

    task = (await ecs.call("RunTask", run_task_params()))["tasks"][0]
    running = sandbox(task)
    with pytest.raises(SandboxError) as unreachable:
        await running.exec("true")
    assert not isinstance(unreachable.value, SandboxNotFoundError)
    await ecs.call("StopTask", {"task": task["taskArn"]})
    with pytest.raises(SandboxNotFoundError):
        await running.exec("true")
    with pytest.raises(SandboxNotFoundError):
        await running.upload_file("/tmp/x", b"x")
    with pytest.raises(SandboxNotFoundError):
        await sandbox(ecs.tasks[task["taskArn"].rsplit("/", 1)[-1]]).download_file("/tmp/x")
