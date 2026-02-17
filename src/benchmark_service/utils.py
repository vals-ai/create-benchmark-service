"""
Utility functions for benchmark services.

This module provides common helper functions for interacting with sandboxes.
"""

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator

from daytona import AsyncSandbox, SessionExecuteRequest

logger = logging.getLogger(__name__)


async def stream_command(
    sandbox: AsyncSandbox, command: str, cwd: str, ignore_error: bool = False
) -> AsyncGenerator[str, None]:
    """
    Execute a command in a sandbox and stream output line by line in real-time.

    Args:
        sandbox: The sandbox instance
        command: Command to execute
        cwd: Working directory
        ignore_error: Whether to ignore non-zero exit codes

    Yields:
        Output lines from the command as they are produced
    """
    session_id = f"{sandbox.id}-{str(uuid.uuid4())}"

    try:
        await sandbox.process.create_session(session_id)

        session_exec_resp = await sandbox.process.execute_session_command(
            session_id, SessionExecuteRequest(command=f"cd {cwd} && {command}", run_async=True)
        )

        cmd_id = session_exec_resp.cmd_id
        if not cmd_id:
            raise ValueError(f"Failed to execute command in session {session_id}")

        output_queue: asyncio.Queue[str] = asyncio.Queue()

        # Queue lines as they arrive
        def on_output(text: str) -> None:
            if text.strip():
                output_queue.put_nowait(text)

        # Start command with streaming logs
        log_task = asyncio.create_task(
            sandbox.process.get_session_command_logs_async(
                session_id=session_id,
                command_id=cmd_id,
                on_stdout=on_output,
                on_stderr=on_output,
            )
        )

        # Yield lines as they arrive in queue
        while not log_task.done():
            try:
                line = await asyncio.wait_for(output_queue.get(), timeout=0.1)
                yield line
            except asyncio.TimeoutError:
                # Keep polling on timeout error
                continue

        # Drain queue after command completes
        while not output_queue.empty():
            yield output_queue.get_nowait()

        cmd = await sandbox.process.get_session_command(session_id, cmd_id)
        if cmd.exit_code != 0 and not ignore_error:
            raise ValueError(f"Command failed with exit code {cmd.exit_code}")

    finally:
        try:
            await sandbox.process.delete_session(session_id)
        except Exception:
            # NOTE: If we kill the sandbox this sometimes errors
            logger.error(f"Caught failure to delete session `{session_id}`")
            pass
