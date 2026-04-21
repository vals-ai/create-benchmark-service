"""Utility functions for benchmark services."""

from collections.abc import AsyncGenerator

from benchmark_service.sandbox import Sandbox


async def stream_command(
    sandbox: Sandbox,
    command: str,
    cwd: str,
    ignore_error: bool = False,
) -> AsyncGenerator[str, None]:
    result = await sandbox.exec(command, cwd=cwd, on_stdout=lambda _text: None, on_stderr=lambda _text: None)
    output = result.stdout + result.stderr
    for line in output.splitlines():
        yield line
    if result.exit_code != 0 and not ignore_error:
        raise ValueError(f"Command failed with exit code {result.exit_code}")
