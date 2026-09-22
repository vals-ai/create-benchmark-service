"""Tests for the bounded stream prelude and executor liveness on ``/health``.

A stream handler runs several steps (auth, request frame, dataset access, provider
connect, sandbox lookup) before the benchmark's own heartbeating generator starts.
None of them emits an application message, so a stall there is invisible to the
client until its idle budget expires. These tests pin the two ways the framework
now surfaces such a stall: the handler fails the stream with the stage it stuck
at, and a process whose thread executor stopped returning work exits so it is replaced.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.executor_health import ExecutorLiveness, terminate_stalled_process
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.types import Sandbox, SandboxCreateRequest, SandboxProvider, SandboxQuery
from benchmark_service.schemas import StreamChunk, StreamMessageChunk, StreamResultChunk
from tests.conftest import StubBenchmark
from tests.test_app import ProviderSelectionSandbox

_REQUEST = {"task_id": "task-1", "instance_id": "i-1"}
_DAYTONA_HEADERS = {"DAYTONA_API_KEY": "key", "DAYTONA_API_URL": "url", "DAYTONA_TARGET": "target"}


class _HangingLookupProvider(SandboxProvider):
    """A provider whose sandbox lookup never returns, as a wedged process would present."""

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        raise AssertionError("unreachable")

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def delete_sandbox(self, instance_id: str) -> None:
        pass

    def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        if False:
            yield ProviderSelectionSandbox()


class _InstantLookupProvider(_HangingLookupProvider):
    async def get_sandbox(self, instance_id: str) -> Sandbox:
        return ProviderSelectionSandbox()


def _app_with_provider(
    monkeypatch: pytest.MonkeyPatch, provider: SandboxProvider, benchmark: type[StubBenchmark], prelude_timeout_s: str
) -> BenchmarkServiceApp:
    def create_provider(_config: DaytonaProviderConfig) -> SandboxProvider:
        return provider

    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", create_provider)
    monkeypatch.setenv("AUTH_DISABLED", "true")
    monkeypatch.setenv("BENCHMARK_SERVICE_STREAM_PRELUDE_TIMEOUT_S", prelude_timeout_s)
    return BenchmarkServiceApp(benchmark)


def test_setup_task_stalled_before_the_benchmark_fails_the_stream_naming_the_stage(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The client receives an error chunk instead of an open, silent socket, and the log says where
    the handler was stuck plus what every thread was doing at the time."""
    app = _app_with_provider(monkeypatch, _HangingLookupProvider(), StubBenchmark, "0.1")

    with caplog.at_level(logging.ERROR, logger="benchmark_service.app"):
        with TestClient(app) as client:
            with client.websocket_connect("/ws/setup-task", headers=_DAYTONA_HEADERS) as ws:
                ws.send_json(_REQUEST)
                chunk = ws.receive_json()

    assert chunk["type"] == "error"
    assert "setup-task did not reach the benchmark within 0s (stalled at 'get_sandbox')" in chunk["data"]
    stall_logs = [record.getMessage() for record in caplog.records if "prelude stalled" in record.getMessage()]
    assert len(stall_logs) == 1
    assert "at 'get_sandbox'" in stall_logs[0]
    assert "MainThread" in stall_logs[0]


def test_prelude_budget_does_not_apply_once_the_benchmark_is_streaming(monkeypatch: pytest.MonkeyPatch) -> None:
    """A benchmark whose chunks are further apart than the prelude budget still completes: the
    budget covers only the silent steps before its generator takes over."""

    class SlowBenchmark(StubBenchmark):
        async def setup_task(
            self, task_id: str, sandbox: Sandbox, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk, None]:
            yield StreamMessageChunk(type="message", data="starting")
            await asyncio.sleep(0.3)
            yield StreamResultChunk(type="result", data={"task_id": task_id})

    app = _app_with_provider(monkeypatch, _InstantLookupProvider(), SlowBenchmark, "0.1")

    with TestClient(app) as client:
        with client.websocket_connect("/ws/setup-task", headers=_DAYTONA_HEADERS) as ws:
            ws.send_json(_REQUEST)
            chunks: list[dict[str, Any]] = [ws.receive_json(), ws.receive_json()]

    assert [chunk["type"] for chunk in chunks] == ["message", "result"]


async def test_executor_liveness_reports_a_probe_that_outlives_the_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    """A returning executor never trips the stall hook; one that stops returning trips it once the
    outstanding probe has waited past the budget, reporting how long it waited."""
    stalls: list[float] = []

    healthy = ExecutorLiveness(probe_interval_s=0.01, stall_budget_s=0.02, on_stall=stalls.append)
    healthy.start()
    await asyncio.sleep(0.1)
    healthy.stop()
    assert stalls == []

    async def never_returns(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        await asyncio.Event().wait()

    monkeypatch.setattr("benchmark_service.executor_health.asyncio.to_thread", never_returns)
    wedged = ExecutorLiveness(probe_interval_s=0.01, stall_budget_s=0.05, on_stall=stalls.append)
    wedged.start()
    await asyncio.sleep(0.02)
    try:
        assert stalls == []
        await asyncio.sleep(0.1)
        assert len(stalls) == 1 and stalls[0] >= 0.05
    finally:
        wedged.stop()


def test_terminate_stalled_process_dumps_threads_then_hard_exits(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    exit_codes: list[int] = []
    monkeypatch.setattr(os, "_exit", exit_codes.append)

    with caplog.at_level(logging.CRITICAL, logger="benchmark_service.executor_health"):
        terminate_stalled_process(612.0)

    assert exit_codes == [1]
    (record,) = caplog.records
    assert "has not returned work for 612s" in record.getMessage()
    assert "Thread MainThread" in record.getMessage()


def test_app_probes_the_executor_for_its_whole_lifespan(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every worker process runs the probe while serving and stops it on shutdown, so a wedge is
    caught without any request having to hit the process first."""
    monkeypatch.setenv("AUTH_DISABLED", "true")
    app = BenchmarkServiceApp(StubBenchmark)
    liveness = app._executor_liveness  # pyright: ignore[reportPrivateUsage]

    with TestClient(app):
        task = liveness._task  # pyright: ignore[reportPrivateUsage]
        assert task is not None and not task.done()

    assert task.cancelled()
