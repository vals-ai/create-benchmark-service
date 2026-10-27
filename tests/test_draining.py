"""Task-wide admission and retries before benchmark work starts."""

import asyncio
import select
import socket
import sqlite3
import subprocess
import sys
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import uvicorn
import websockets
from starlette.types import Message, Scope

import benchmark_service.client as client_module
from benchmark_service import BenchmarkServiceApp, BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.draining import DRAIN_HEADER, DRAIN_STATE_PATH_ENV, DrainMiddleware, DrainState
from benchmark_service.schemas import EvaluateResponseRequest, StreamChunk, StreamMessageChunk, StreamResultChunk
from benchmark_service.v1_schemas import V1PayloadType
from conftest import StubBenchmark


def configure(state: DrainState, accepting: bool, revision: int) -> None:
    state.set_admission(accepting=accepting, revision=revision, store_id=state.store_id)


@pytest.fixture
def state(tmp_path: Path) -> DrainState:
    result = DrainState.create(tmp_path / "drain.sqlite")
    configure(result, True, 0)
    return result


def test_control_revision_and_completion_are_safe_to_repeat(state: DrainState) -> None:
    assert state.admit("one")
    assert state.admit("two")
    configure(state, False, 2)
    configure(state, True, 1)
    assert not state.admit("three")
    state.complete("one")
    state.complete("one")
    assert state.snapshot().in_flight == 1
    assert not state.snapshot().drained
    with pytest.raises(ValueError, match="Conflicting"):
        configure(state, True, 2)
    with pytest.raises(ValueError, match="different store"):
        state.set_admission(accepting=True, revision=3, store_id="old-incarnation")
    state.complete("two")
    assert state.snapshot().drained
    configure(state, True, 3)
    assert state.admit("four")


def test_new_store_is_unknown_until_control_applies(tmp_path: Path) -> None:
    state = DrainState.create(tmp_path / "state")
    assert not state.admit("work")
    assert not state.snapshot().drained
    configure(state, False, 0)
    assert state.snapshot().drained
    with pytest.raises(FileExistsError):
        DrainState.create(state.path)


def test_missing_replaced_and_locked_stores_cannot_report_idle(state: DrainState) -> None:
    state.path.unlink()
    with pytest.raises(sqlite3.OperationalError):
        state.snapshot()
    with pytest.raises(sqlite3.OperationalError):
        DrainState(state.path)
    assert not state.path.exists()
    replacement = DrainState.create(state.path)
    configure(replacement, False, 7)
    with pytest.raises(RuntimeError, match="identity changed"):
        state.snapshot()
    connection = sqlite3.connect(replacement.path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            replacement.snapshot()
    finally:
        connection.close()


_WORKER = """
import sys
from benchmark_service.draining import DrainState
state = DrainState(sys.argv[1])
accepted = state.admit(sys.argv[2])
print(accepted, flush=True)
sys.stdin.readline()
if accepted:
    state.complete(sys.argv[2])
"""


def test_multiple_processes_share_admission_and_crash_rows_block_retirement(state: DrainState) -> None:
    workers = [
        subprocess.Popen(
            [sys.executable, "-c", _WORKER, str(state.path), name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        for name in ("one", "two")
    ]
    try:
        for worker in workers:
            assert worker.stdout is not None
            assert select.select([worker.stdout], [], [], 10)[0], "Worker did not report admission"
            assert worker.stdout.readline().strip() == "True"
        configure(state, False, 1)
        late = subprocess.run(
            [sys.executable, "-c", _WORKER, str(state.path), "late"],
            input="\n",
            text=True,
            capture_output=True,
            timeout=10,
            check=True,
        )
        assert late.stdout.strip() == "False"
        assert state.snapshot().in_flight == 2
        workers[0].communicate("\n", timeout=10)
        workers[1].kill()
        workers[1].wait(timeout=10)
        assert state.snapshot().in_flight == 1
        assert not state.snapshot().drained
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
            worker.communicate(timeout=10)


async def test_cancellation_after_admission_commit_does_not_leak_work(
    state: DrainState, monkeypatch: pytest.MonkeyPatch
) -> None:
    committed = threading.Event()
    release = threading.Event()
    original = state.admit

    def delayed_admit(request_id: str) -> bool:
        accepted = original(request_id)
        committed.set()
        assert release.wait(5)
        return accepted

    monkeypatch.setattr(state, "admit", delayed_admit)
    entered = False

    async def app(_scope: Scope, _receive: Any, _send: Any) -> None:
        nonlocal entered
        entered = True

    async def receive() -> Message:
        return {"type": "http.request"}

    async def send(_message: Message) -> None:
        pass

    task = asyncio.create_task(DrainMiddleware(app, state)({"type": "http", "path": "/work"}, receive, send))
    try:
        assert await asyncio.to_thread(committed.wait, 5)
        task.cancel()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not entered
    assert state.snapshot().in_flight == 0


class SlowBenchmark(StubBenchmark):
    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.http_calls: list[str | None] = []
        self.ws_calls = 0
        return await super().load_datasets()

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        self.http_calls.append(request.response)
        if request.response == "hold":
            self.started.set()
            await self.release.wait()
        return {"response": request.response}

    async def stream_evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        self.ws_calls += 1
        yield StreamMessageChunk(type="message", data="started")
        if request.eval_resume_state == {"hold": True}:
            self.started.set()
            await self.release.wait()
        yield StreamResultChunk(type="result", data={"finished": True})


@asynccontextmanager
async def serve(app: BenchmarkServiceApp) -> AsyncGenerator[str, None]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", ws="websockets"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                    raise RuntimeError("Server exited before startup")
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        if hasattr(app, "service") and isinstance(app.service, SlowBenchmark):
            app.service.release.set()
        server.should_exit = True
        await asyncio.wait_for(task, timeout=10)
        sock.close()


@pytest.fixture
async def running(
    state: DrainState, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[tuple[str, SlowBenchmark], None]:
    monkeypatch.setenv(DRAIN_STATE_PATH_ENV, str(state.path))
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    monkeypatch.setattr(client_module, "_DRAIN_RETRY_DELAY_S", 0.01)
    app = BenchmarkServiceApp(SlowBenchmark)
    async with serve(app) as url:
        yield url, cast(SlowBenchmark, app.service)


async def test_http_retry_does_not_close_another_active_request(
    running: tuple[str, SlowBenchmark], state: DrainState, monkeypatch: pytest.MonkeyPatch
) -> None:
    url, service = running
    async with BenchmarkServiceClient(url, {}) as client:
        old = asyncio.create_task(client.evaluate_response("task-1", "hold"))
        await asyncio.wait_for(service.started.wait(), 5)
        configure(state, False, 1)
        assert state.snapshot().in_flight == 1
        assert (await client.health_check()).status == "ok"
        retry_client_created = asyncio.Event()
        original = client._new_http_client  # pyright: ignore[reportPrivateUsage]

        def fresh(*, fresh_connections: bool = False) -> httpx.AsyncClient:
            if fresh_connections:
                retry_client_created.set()
            return original(fresh_connections=fresh_connections)

        monkeypatch.setattr(client, "_new_http_client", fresh)
        new = asyncio.create_task(client.evaluate_response("task-1", "new"))
        await asyncio.wait_for(retry_client_created.wait(), 5)
        assert service.http_calls == ["hold"]
        configure(state, True, 2)
        assert await asyncio.wait_for(new, 5) == {"response": "new"}
        assert not old.done()
        service.release.set()
        assert await asyncio.wait_for(old, 5) == {"response": "hold"}
        assert service.http_calls == ["hold", "new"]
    configure(state, False, 3)
    assert state.snapshot().drained


async def test_real_websocket_denial_retries_before_payload_and_preserves_existing_stream(
    running: tuple[str, SlowBenchmark], state: DrainState, monkeypatch: pytest.MonkeyPatch
) -> None:
    url, service = running
    original = websockets.connect
    attempts = 0

    def connect(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 3:
            configure(state, True, 2)
        return original(*args, **kwargs)

    monkeypatch.setattr(websockets, "connect", connect)
    async with BenchmarkServiceClient(url, {}) as client:
        old = asyncio.create_task(client.resume_evaluation("task-1", {"hold": True}))
        await asyncio.wait_for(service.started.wait(), 5)
        configure(state, False, 1)
        assert not state.snapshot().drained
        assert await asyncio.wait_for(client.resume_evaluation("task-1", {}), 5) == {"finished": True}
        assert attempts == 3
        assert service.ws_calls == 2
        assert not old.done()
        service.release.set()
        assert await asyncio.wait_for(old, 5) == {"finished": True}
    configure(state, False, 3)
    assert state.snapshot().drained


@pytest.mark.parametrize("marked", [False, True])
async def test_v1_does_not_retry_ordinary_503_or_ambiguous_failure_after_rejection(
    marked: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[str] = []
    monkeypatch.setattr(client_module, "_DRAIN_RETRY_DELAY_S", 0.001)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if len(requests) == 1:
            return httpx.Response(503, headers={DRAIN_HEADER: "1"} if marked else {}, json={"detail": "unavailable"})
        raise httpx.ReadTimeout("response lost after submission", request=request)

    async with BenchmarkServiceClient("http://test", {}) as client:
        await client._http_client.aclose()  # pyright: ignore[reportPrivateUsage]
        client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(respond))  # pyright: ignore[reportPrivateUsage]

        def fresh(*, fresh_connections: bool = False) -> httpx.AsyncClient:
            return httpx.AsyncClient(transport=httpx.MockTransport(respond))

        monkeypatch.setattr(client, "_new_http_client", fresh)
        expected = httpx.ReadTimeout if marked else BenchmarkServiceError
        with pytest.raises(expected):
            await client.v1_evaluate("run", "task", "answer", "text.v1", V1PayloadType.TEXT)
    assert requests == ["/v1/evaluate"] * (2 if marked else 1)


async def test_rejection_retry_window_is_bounded(
    running: tuple[str, SlowBenchmark], state: DrainState, monkeypatch: pytest.MonkeyPatch
) -> None:
    url, service = running
    configure(state, False, 1)
    monkeypatch.setattr(client_module, "_DRAIN_RETRY_WINDOW_S", 0.05)
    async with BenchmarkServiceClient(url, {}) as client:
        with pytest.raises(BenchmarkServiceError, match="503"):
            await asyncio.wait_for(client.evaluate_response("task-1", "never"), 2)
    assert not service.http_calls


async def test_websocket_failure_after_payload_is_not_replayed(
    running: tuple[str, SlowBenchmark], monkeypatch: pytest.MonkeyPatch
) -> None:
    url, service = running
    attempts = 0
    original = websockets.connect

    def connect(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(websockets, "connect", connect)
    async with BenchmarkServiceClient(url, {}) as client:

        def fail(_message: str) -> None:
            raise RuntimeError("consumer failed after request started")

        with pytest.raises(RuntimeError, match="consumer failed"):
            await client.resume_evaluation("task-1", {}, on_message=fail)
    assert attempts == 1
    assert service.ws_calls == 1
