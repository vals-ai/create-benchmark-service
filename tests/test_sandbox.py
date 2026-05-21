from types import SimpleNamespace
from typing import Any, cast

from benchmark_service.sandbox import DaytonaSandbox


class _Process:
    command: str | None = None

    async def exec(self, command: str) -> SimpleNamespace:
        self.command = command
        return SimpleNamespace(exit_code=0, result="")


class _Fs:
    async def download_file_stream(self, remote_path: str) -> Any:
        assert remote_path == "/tmp/result.txt"

        async def chunks() -> Any:
            yield b"hello"
            yield b" world"

        return chunks()


class _Inner:
    id = "sandbox-id"
    name = "sandbox-name"
    state = "started"

    def __init__(self) -> None:
        self.process = _Process()
        self.fs = _Fs()


async def test_daytona_command_applies_timeout_inside_cwd() -> None:
    inner = _Inner()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", cwd="/workspace", timeout=60)

    assert inner.process.command == "cd /workspace && timeout 60 pytest"


async def test_daytona_command_preserves_fractional_timeout() -> None:
    inner = _Inner()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", timeout=0.5)

    assert inner.process.command == "timeout 0.5 pytest"


async def test_daytona_download_file_streams_content() -> None:
    sandbox = DaytonaSandbox(cast(Any, _Inner()))

    assert await sandbox.download_file("/tmp/result.txt") == b"hello world"
