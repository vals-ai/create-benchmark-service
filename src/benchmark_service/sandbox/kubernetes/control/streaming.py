"""Encode command events as cancellation-safe NDJSON HTTP streams."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

from benchmark_service.sandbox.kubernetes.control.errors import error_detail
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandErrorEvent,
    CommandEvent,
    CommandExitEvent,
)
from benchmark_service.sandbox.types import SandboxConnectionError, SandboxError


async def command_events_to_ndjson(
    stream: AsyncGenerator[CommandEvent, None],
    *,
    request_id: str,
    heartbeat_seconds: float,
) -> AsyncGenerator[bytes, None]:
    """Encode command events and keep an idle HTTP stream alive."""
    pending_event: asyncio.Task[CommandEvent] | None = None
    terminal_received = False
    try:
        while True:
            if pending_event is None:
                pending_event = asyncio.create_task(anext(stream))
            completed, _ = await asyncio.wait((pending_event,), timeout=heartbeat_seconds)
            if not completed:
                yield b"\n"
                continue
            try:
                event = pending_event.result()
            except StopAsyncIteration:
                if terminal_received:
                    return
                raise SandboxConnectionError("Command stream ended without a terminal event") from None
            pending_event = None
            terminal_received = isinstance(event, (CommandExitEvent, CommandErrorEvent))
            yield f"{event.model_dump_json()}\n".encode()
            if terminal_received:
                return
    except SandboxError as error:
        _, detail = error_detail(error, request_id)
        event = CommandErrorEvent(
            type="error",
            code=detail.code,
            message=detail.message,
            request_id=request_id,
        )
        yield f"{event.model_dump_json()}\n".encode()
    finally:
        if pending_event is not None:
            pending_event.cancel()
            await asyncio.gather(pending_event, return_exceptions=True)
        await stream.aclose()
