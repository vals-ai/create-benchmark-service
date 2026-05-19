"""Utility functions for benchmark services.

This module provides common helper functions for interacting with sandboxes.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator

from benchmark_service.sandbox import Sandbox

logger = logging.getLogger(__name__)


async def stream_command(
    sandbox: Sandbox, command: str, cwd: str, ignore_error: bool = False
) -> AsyncGenerator[str, None]:
    """Execute a command in a sandbox and stream output line by line in real-time.

    Args:
        sandbox: The sandbox instance
        command: Command to execute
        cwd: Working directory
        ignore_error: Whether to ignore non-zero exit codes

    Yields:
        Output lines from the command as they are produced
    """
    output_queue: asyncio.Queue[str] = asyncio.Queue()

    def on_output(text: str) -> None:
        if text.strip():
            output_queue.put_nowait(text)

    exec_task = asyncio.create_task(sandbox.exec(command, cwd=cwd, on_output=on_output))
    while not exec_task.done():
        try:
            yield await asyncio.wait_for(output_queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            continue

    while not output_queue.empty():
        yield output_queue.get_nowait()

    result = await exec_task
    if result.exit_code != 0 and not ignore_error:
        raise ValueError(f"Command failed with exit code {result.exit_code}")
