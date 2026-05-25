"""Utility functions for benchmark services.

This module provides common helper functions for interacting with sandboxes.
"""

import logging
from collections.abc import AsyncGenerator

from benchmark_service.sandbox import Sandbox, SandboxCommandError

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
    try:
        async for text in sandbox.stream_command(command, cwd=cwd):
            if text.strip():
                yield text
    except SandboxCommandError as exc:
        if not ignore_error:
            raise ValueError(f"Command failed with exit code {exc.exit_code}") from exc
