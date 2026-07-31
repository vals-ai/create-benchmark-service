"""Cancellation-safe execution for blocking operations."""

import asyncio
from collections.abc import Callable
from typing import TypeVar

_T = TypeVar("_T")


async def run_blocking(operation: Callable[[], _T]) -> _T:
    """Keep cancellation from detaching blocking work from its caller."""
    worker = asyncio.create_task(asyncio.to_thread(operation))
    cancellation: asyncio.CancelledError | None = None
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
        except Exception:  # noqa: BLE001
            break

    try:
        result = worker.result()
    except BaseException:  # noqa: BLE001
        if cancellation is not None:
            raise cancellation from None
        raise

    if cancellation is not None:
        raise cancellation
    return result
