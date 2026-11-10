"""Sandbox-loss recovery for task attempts, shared by every task client."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TypeVar
from uuid import uuid4

from benchmark_service.observability import correlation_scope
from benchmark_service.sandbox import SandboxNotFoundError, SandboxProvider, SandboxProviderConfig
from benchmark_service.schemas import RetrieveTaskResponse

_RecoveryResult = TypeVar("_RecoveryResult")

_OUTAGE_ID_ENV = "VALKYRIE_SANDBOX_OUTAGE_ID"
_OUTAGE_STARTED_ENV = "VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH"


class SandboxProviders:
    """Sandbox providers created on first use, one per distinct provider config."""

    def __init__(self) -> None:
        self._providers: dict[str, SandboxProvider] = {}

    def get(self, provider: SandboxProviderConfig) -> SandboxProvider:
        """Return the provider for this config, creating it the first time."""
        key = provider.model_dump_json()
        if key not in self._providers:
            self._providers[key] = provider.create_provider()
        return self._providers[key]

    async def close(self) -> None:
        """Close every provider created so far."""
        for provider in self._providers.values():
            await provider.close()


@dataclass(frozen=True)
class SandboxRecoveryAttempt:
    """One bounded invocation of a durable sandbox-backed task.

    ``environment`` is empty for the initial attempt. After a provider-confirmed
    sandbox loss it carries stable outage metadata until the replacement has
    completed benchmark setup and calls ``mark_replacement_ready``. If that
    setup fails, the next replacement receives the same outage identity rather
    than double-crediting one interruption.
    """

    number: int
    outage_id: str | None
    outage_started_epoch: float | None
    _state: _SandboxRecoveryState = field(repr=False, compare=False)

    @property
    def max_attempts(self) -> int:
        """Current overall cap after applying the cached benchmark policy."""
        return self._state.max_attempts

    @property
    def sandbox_loss_retry_available(self) -> bool:
        """Whether this attempt may recover a typed provider sandbox loss."""
        return self._state.recover_sandbox_loss and self.number < self._state.max_attempts

    @property
    def environment(self) -> dict[str, str]:
        """Environment metadata to add to this attempt's sandbox."""
        if self.outage_id is None or self.outage_started_epoch is None:
            return {}
        return {
            _OUTAGE_ID_ENV: self.outage_id,
            _OUTAGE_STARTED_ENV: str(self.outage_started_epoch),
        }

    def mark_replacement_ready(self) -> None:
        """Acknowledge that benchmark setup persisted this outage metadata."""
        self._state.mark_ready(self.outage_id)

    async def retrieve_task(self) -> RetrieveTaskResponse:
        """Load and cache the task payload and its recovery policy."""
        return await self._state.retrieve_task()


@dataclass
class _SandboxRecoveryState:
    run_id: str
    task_id: str
    load_task: Callable[[], Awaitable[RetrieveTaskResponse]] = field(repr=False)
    default_max_attempts: int
    outage_id: str | None = None
    outage_started_epoch: float | None = None
    consecutive_attempt_errors: int = 0
    task: RetrieveTaskResponse | None = None
    recover_sandbox_loss: bool = False
    max_attempts: int = field(init=False)

    def __post_init__(self) -> None:
        self.max_attempts = self.default_max_attempts

    def attempt(self, number: int) -> SandboxRecoveryAttempt:
        return SandboxRecoveryAttempt(
            number=number,
            outage_id=self.outage_id,
            outage_started_epoch=self.outage_started_epoch,
            _state=self,
        )

    async def retrieve_task(self) -> RetrieveTaskResponse:
        if self.task is None:
            self.task = await self.load_task()
            policy = self.task.sandbox_recovery
            self.recover_sandbox_loss = policy is not None
            if policy is not None:
                self.max_attempts = policy.max_sandbox_attempts
        return self.task

    def record_loss(self, attempt_number: int) -> None:
        self.consecutive_attempt_errors = 0
        if self.outage_id is not None:
            return
        self.outage_id = f"{self.run_id}:{self.task_id}:{attempt_number}:{uuid4().hex}"
        self.outage_started_epoch = time.time()

    def record_attempt_error(self) -> None:
        self.consecutive_attempt_errors += 1

    def mark_ready(self, outage_id: str | None) -> None:
        if outage_id is None or outage_id != self.outage_id:
            return
        self.outage_id = None
        self.outage_started_epoch = None


async def run_with_sandbox_recovery(
    load_task: Callable[[], Awaitable[RetrieveTaskResponse]],
    task_id: str,
    run_id: str,
    operation: Callable[[SandboxRecoveryAttempt], Awaitable[_RecoveryResult]],
    *,
    retryable_attempt_errors: tuple[type[Exception], ...] = (),
    default_max_attempts: int = 1,
    retry_delay_s: float = 2.0,
    on_retry: Callable[[SandboxRecoveryAttempt, Exception], Awaitable[None] | None] | None = None,
    attempt_id: str | None = None,
    email: str | None = None,
) -> _RecoveryResult:
    """Run a task attempt with benchmark-declared sandbox-loss recovery.

    ``load_task`` loads the task payload and its recovery policy the first time an attempt asks for it.

    ``SandboxRecoveryAttempt.retrieve_task`` lazily loads and caches the
    task payload so successful durable evaluation resumes need no task
    request. ``SandboxNotFoundError`` is retried only when the benchmark
    explicitly opts in. Callers may
    additionally name setup errors that require a fresh sandbox. These use
    ``default_max_attempts`` both as the fallback total cap for benchmarks
    without a recovery policy and as their consecutive-attempt cap. Every
    attempt still counts against the benchmark's declared overall cap.
    Synchronous and asynchronous ``on_retry`` callbacks are both
    supported and complete before the retry delay begins.

    The final exception is re-raised unchanged when the applicable attempt
    budget is exhausted.
    """
    if not run_id:
        raise ValueError("run_id must be non-empty")
    if not task_id:
        raise ValueError("task_id must be non-empty")
    if not 1 <= default_max_attempts <= 20:
        raise ValueError("default_max_attempts must be between 1 and 20")
    if retry_delay_s < 0:
        raise ValueError("retry_delay_s must be non-negative")
    if any(issubclass(error_type, SandboxNotFoundError) for error_type in retryable_attempt_errors):
        raise ValueError("SandboxNotFoundError is controlled only by SandboxRecoveryPolicy")

    with correlation_scope(run_id=run_id, task_id=task_id, attempt_id=attempt_id, email=email):
        state = _SandboxRecoveryState(
            run_id=run_id,
            task_id=task_id,
            load_task=load_task,
            default_max_attempts=default_max_attempts,
        )

        attempt_number = 1
        while attempt_number <= state.max_attempts:
            attempt = state.attempt(attempt_number)
            try:
                return await operation(attempt)
            except SandboxNotFoundError as exc:
                if state.task is None:
                    await state.retrieve_task()
                if not attempt.sandbox_loss_retry_available:
                    raise
                state.record_loss(attempt_number)
                retry_error: Exception = exc
            except Exception as exc:
                if (
                    not isinstance(exc, retryable_attempt_errors)
                    or attempt_number >= state.max_attempts
                    or state.consecutive_attempt_errors >= default_max_attempts - 1
                ):
                    raise
                state.record_attempt_error()
                retry_error = exc

            if on_retry is not None:
                callback_result = on_retry(attempt, retry_error)
                if inspect.isawaitable(callback_result):
                    await callback_result
            if retry_delay_s:
                await asyncio.sleep(retry_delay_s)
            attempt_number += 1

        raise AssertionError("sandbox recovery loop exited without a result")
