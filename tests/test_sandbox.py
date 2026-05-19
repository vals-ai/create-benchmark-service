from types import SimpleNamespace
from typing import Any, cast

from benchmark_service.sandbox import DaytonaSandbox


class _Process:
    command: str | None = None

    async def exec(self, command: str) -> SimpleNamespace:
        self.command = command
        return SimpleNamespace(exit_code=0, result="")


class _Inner:
    id = "sandbox-id"
    name = "sandbox-name"
    state = "started"

    def __init__(self) -> None:
        self.process = _Process()


async def test_daytona_command_applies_timeout_inside_cwd() -> None:
    inner = _Inner()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", cwd="/workspace", timeout=60)

    assert inner.process.command == "cd /workspace && timeout 60 pytest"
