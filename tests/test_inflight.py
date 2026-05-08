"""Tests for InflightMiddleware."""

import asyncio
import json

import httpx
import pytest
from fastapi import FastAPI

from benchmark_service.inflight import InflightMiddleware


@pytest.mark.asyncio
async def test_inflight_increments_during_request_and_decrements_after() -> None:
    test_app = FastAPI()
    handler_started = asyncio.Event()
    can_finish = asyncio.Event()

    @test_app.get("/slow")
    async def slow() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        handler_started.set()
        await can_finish.wait()
        return {"ok": "yes"}

    @test_app.get("/health")
    async def health() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        return {"status": "ok"}

    mw = InflightMiddleware(test_app, service_name="test", emit_interval_s=3600)

    transport = httpx.ASGITransport(app=mw)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert mw.current_count() == 0

        request_task = asyncio.create_task(client.get("/slow"))
        await handler_started.wait()
        assert mw.current_count() == 1

        can_finish.set()
        resp = await request_task
        assert resp.status_code == 200
        assert mw.current_count() == 0

    await mw.aclose()


def test_inflight_decrements_on_exception() -> None:
    from fastapi.testclient import TestClient

    test_app = FastAPI()

    @test_app.get("/boom")
    async def boom() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        raise RuntimeError("kaboom")

    mw = InflightMiddleware(test_app, service_name="test", emit_interval_s=3600)
    with TestClient(mw, raise_server_exceptions=False) as client:
        resp = client.get("/boom")
        assert resp.status_code == 500
    assert mw.current_count() == 0


@pytest.mark.asyncio
async def test_inflight_counts_concurrent_requests() -> None:
    test_app = FastAPI()
    can_finish = asyncio.Event()
    started = asyncio.Semaphore(0)

    @test_app.get("/wait")
    async def wait() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        started.release()
        await can_finish.wait()
        return {"ok": "yes"}

    mw = InflightMiddleware(test_app, service_name="test", emit_interval_s=3600)

    transport = httpx.ASGITransport(app=mw)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        tasks = [asyncio.create_task(client.get("/wait")) for _ in range(3)]
        for _ in range(3):
            await started.acquire()
        assert mw.current_count() == 3
        can_finish.set()
        await asyncio.gather(*tasks)

    assert mw.current_count() == 0
    await mw.aclose()


def test_emit_once_writes_emf_json(capsys: pytest.CaptureFixture[str]) -> None:
    test_app = FastAPI()
    mw = InflightMiddleware(test_app, service_name="proof-bench", emit_interval_s=3600)
    mw._inflight = 7  # pyright: ignore[reportPrivateUsage]
    mw._emit_once()  # pyright: ignore[reportPrivateUsage]

    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1])
    assert payload["ServiceName"] == "proof-bench"
    assert payload["InFlightRequests"] == 7
    assert payload["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "Vals/BenchmarkServices"
    metric = payload["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]
    assert metric == {"Name": "InFlightRequests", "Unit": "Count"}


def test_benchmarkserviceapp_installs_inflight_middleware() -> None:
    from benchmark_service.app import BenchmarkServiceApp
    from tests.conftest import StubBenchmark

    app = BenchmarkServiceApp(StubBenchmark)
    found = any(
        getattr(m, "cls", None) is InflightMiddleware
        for m in app.user_middleware
    )
    assert found, "InflightMiddleware not in app.user_middleware"
