import asyncio
from typing import cast

import pytest

from benchmark_service.sandbox import Sandbox
from benchmark_service.utils import stream_command


class _StreamingSandbox:
    def __init__(self) -> None:
        self.id = "sandbox-1"
        self.cancelled = False

    async def exec(self, command: str, cwd: str | None = None, on_stdout=None, on_stderr=None, **kwargs):
        if on_stdout is not None:
            on_stdout("first line")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


@pytest.mark.asyncio
async def test_stream_command_cancels_exec_task_when_generator_closes() -> None:
    sandbox = _StreamingSandbox()
    generator = stream_command(cast(Sandbox, sandbox), "echo hi", "/tmp")

    assert await anext(generator) == "first line"

    await generator.aclose()

    assert sandbox.cancelled is True
