from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import pytest
from ecs_nitro_agents import KEY, ready_agent

from benchmark_service.sandbox.ecs_nitro.client import AgentClient
from benchmark_service.sandbox.ecs_nitro.config import agent_token
from benchmark_service.sandbox.types import SandboxError


@pytest.mark.asyncio
async def test_agent_rejects_a_token_for_another_sandbox() -> None:
    client = await ready_agent("owner")
    forged = AgentClient("127.0.0.1", int(client._base.rsplit(":", 1)[1]), agent_token(KEY, "other"))  # pyright: ignore[reportPrivateUsage]
    assert not await forged.healthy()
    with pytest.raises(SandboxError, match="401"):
        await forged.upload("/tmp/forged", b"x")
    assert [item async for item in client.run("printf ok", None, None, {})] == [b"ok", 0]


@pytest.mark.asyncio
async def test_command_with_closed_output_still_times_out() -> None:
    client = await ready_agent("closed-output")
    started = time.monotonic()
    items = [item async for item in client.run("exec 1>&- 2>&-; sleep 30", None, 1, {})]
    assert items == [124]
    assert time.monotonic() - started < 10


@pytest.mark.asyncio
async def test_disconnected_client_kills_silent_command(tmp_path: Any) -> None:
    client = await ready_agent("disconnect")
    pid_file = tmp_path / "pid"

    async def consume() -> None:
        async for _ in client.run(f"echo $$ > {pid_file}; exec sleep 30", None, None, {}):
            pass

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(consume(), 1)
    pid = int(pid_file.read_text())
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("silent command survived its client")
