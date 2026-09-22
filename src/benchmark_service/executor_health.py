"""Liveness of the event loop's default thread executor.

Every blocking call the framework and its services make (``asyncio.to_thread``,
``run_blocking``, the Descope key exchange) runs on one executor per process. If
its threads stop returning, every request that touches it queues forever while
the loop itself keeps accepting connections and answering ``/health`` — a
process that looks healthy and completes nothing. ``ExecutorLiveness`` submits
a no-op on a fixed cadence; once a probe has waited past the budget the process
logs every thread's stack and exits so the uvicorn supervisor (or the container
orchestrator) replaces it. The budget is long enough that an executor merely
saturated by a burst of real work does not trip it.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
import traceback
from collections.abc import Callable

import sentry_sdk

logger = logging.getLogger(__name__)

PROBE_INTERVAL_ENV = "BENCHMARK_SERVICE_EXECUTOR_PROBE_INTERVAL_S"
STALL_BUDGET_ENV = "BENCHMARK_SERVICE_EXECUTOR_STALL_BUDGET_S"
DEFAULT_PROBE_INTERVAL_S = 15.0
DEFAULT_STALL_BUDGET_S = 600.0

_SENTRY_FLUSH_TIMEOUT_S = 5.0


def _noop() -> None:
    return None


def dump_threads() -> str:
    """Every thread's current stack, for the log line that explains a stall."""
    names = {thread.ident: thread.name for thread in threading.enumerate()}
    sections = [
        f"Thread {names.get(ident, '?')} ({ident}):\n" + "".join(traceback.format_stack(frame))
        for ident, frame in sys._current_frames().items()  # pyright: ignore[reportPrivateUsage]
    ]
    return "\n".join(sections)


def terminate_stalled_process(waited_s: float) -> None:
    """Record the stall with a thread dump, then hard-exit.

    A graceful shutdown would wait for the wedged handlers' connections to close, which they
    never do; ``os._exit`` lets the supervisor start a fresh worker immediately.
    """
    logger.critical(
        "thread executor has not returned work for %.0fs; exiting so this worker is replaced. Thread stacks:\n%s",
        waited_s,
        dump_threads(),
    )
    sentry_sdk.flush(timeout=_SENTRY_FLUSH_TIMEOUT_S)
    for handler in logging.getLogger().handlers:
        handler.flush()
    os._exit(1)


class ExecutorLiveness:
    """Probe the default executor on a cadence and call ``on_stall`` once a probe outlives the budget."""

    def __init__(
        self,
        *,
        probe_interval_s: float,
        stall_budget_s: float,
        on_stall: Callable[[float], None] = terminate_stalled_process,
    ) -> None:
        self._probe_interval_s = probe_interval_s
        self._stall_budget_s = stall_budget_s
        self._on_stall = on_stall
        self._task: asyncio.Task[None] | None = None

    @classmethod
    def from_env(cls) -> ExecutorLiveness:
        return cls(
            probe_interval_s=float(os.environ.get(PROBE_INTERVAL_ENV) or DEFAULT_PROBE_INTERVAL_S),
            stall_budget_s=float(os.environ.get(STALL_BUDGET_ENV) or DEFAULT_STALL_BUDGET_S),
        )

    def start(self) -> None:
        self._task = asyncio.create_task(self._probe_forever())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()

    async def _probe_forever(self) -> None:
        while True:
            started_at = time.monotonic()
            probe = asyncio.ensure_future(asyncio.to_thread(_noop))
            try:
                await asyncio.wait_for(asyncio.shield(probe), timeout=self._stall_budget_s)
            except TimeoutError:
                self._on_stall(time.monotonic() - started_at)
                await probe
            await asyncio.sleep(self._probe_interval_s)
