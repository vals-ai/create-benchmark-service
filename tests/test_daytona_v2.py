from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from benchmark_service.sandbox import (
    DaytonaProviderConfig,
    DaytonaV2ProviderConfig,
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.sandbox import daytona_v2
from benchmark_service.sandbox.daytona_v2 import DaytonaV2Sandbox, DaytonaV2SandboxProvider, v2_resources
from benchmark_service.vals.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]

API = "https://api.daytona.test"
TOOLBOX = "https://toolbox.daytona.test"
Handler = Callable[[httpx.Request], httpx.Response]


def _provider(handler: Handler, *, spot: bool = False) -> DaytonaV2SandboxProvider:
    return DaytonaV2SandboxProvider(
        DaytonaV2ProviderConfig(DAYTONA_V2_API_KEY="key", DAYTONA_V2_API_URL=API, DAYTONA_V2_SPOT=spot),
        transport=httpx.MockTransport(handler),
    )


def _request(vcpu: float, **overrides: Any) -> SandboxCreateRequest:
    fields: dict[str, Any] = {
        "source": ImageSource(image="python:3.13-slim"),
        "resources": Resources(vcpu=vcpu, memory=1, disk=3),
        "name": "tts-task-1",
        "labels": {"run": "r1"},
        "env_vars": {"A": "1"},
        "auto_stop_interval": 15,
        "create_timeout": 120,
    }
    fields.update(overrides)
    return SandboxCreateRequest(**fields)


@pytest.fixture(autouse=True)
def no_poll_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)


@pytest.mark.parametrize(
    ("vcpu", "cores", "cores_max"),
    [
        (0.25, 0.125, 0.5),
        (0.5, 0.25, 0.5),
        (1, 0.5, 0.5),
        (1.0000000001, 0.50000000005, 1.0),
        (1.5, 0.75, 1.0),
        (3, 1.5, 1.5),
        (4, 2, 2.0),
    ],
)
def test_v2_resources_reserves_fractional_cores_and_caps_them(vcpu: float, cores: float, cores_max: float) -> None:
    assert v2_resources(Resources(vcpu=vcpu, memory=2, disk=3)) == {
        "cpu": cores,
        "cores_max": cores_max,
        "memory_mib": 2048,
        "memory_max_mib": 2048,
        "disk_mib": 3072,
    }


def test_config_parses_from_mapping_and_v0_stays_distinct() -> None:
    config = sandbox_provider_config_from_mapping({"type": "daytona_v2", "DAYTONA_V2_API_KEY": "key"})
    assert isinstance(config, DaytonaV2ProviderConfig)
    assert config == DaytonaV2ProviderConfig(DAYTONA_V2_API_KEY="key")
    assert config.DAYTONA_V2_API_URL == "https://api.daytona.com"
    assert isinstance(config.create_provider(), DaytonaV2SandboxProvider)
    v0 = sandbox_provider_config_from_mapping(
        {"type": "daytona", "DAYTONA_API_KEY": "k", "DAYTONA_API_URL": "u", "DAYTONA_TARGET": "us"}
    )
    assert isinstance(v0, DaytonaProviderConfig)


def test_config_rejects_plain_http_api_url() -> None:
    with pytest.raises(ValueError, match="https"):
        DaytonaV2ProviderConfig(DAYTONA_V2_API_KEY="key", DAYTONA_V2_API_URL="http://api.daytona.com")


def test_grading_provider_selects_v2_only_when_opted_in(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("DAYTONA_API_KEY", "DAYTONA_API_URL", "DAYTONA_TARGET"):
        monkeypatch.setenv(name, "v0")
    monkeypatch.delenv("GRADING_SANDBOX_PROVIDER", raising=False)
    assert isinstance(_grading_provider_config(), DaytonaProviderConfig)

    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "daytona_v2")
    monkeypatch.setenv("DAYTONA_V2_API_KEY", "v2-key")
    monkeypatch.setenv("DAYTONA_V2_REGION", "us")
    monkeypatch.setenv("DAYTONA_V2_SPOT", "true")
    assert _grading_provider_config() == DaytonaV2ProviderConfig(
        DAYTONA_V2_API_KEY="v2-key", DAYTONA_V2_REGION="us", DAYTONA_V2_SPOT=True
    )

    monkeypatch.delenv("DAYTONA_V2_API_KEY")
    with pytest.raises(MissingSandboxConfigError, match="DAYTONA_V2_API_KEY"):
        _grading_provider_config()


async def test_create_sandbox_sends_fractional_cores_and_waits_for_running() -> None:
    calls: list[httpx.Request] = []
    statuses = iter(["booting", "running"])

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path
        if request.method == "POST" and path == "/images":
            return httpx.Response(201, json={"id": "img-1", "status": "pulling"})
        if path == "/images/img-1":
            return httpx.Response(200, json={"id": "img-1", "status": "ready"})
        if request.method == "POST" and path == "/sandboxes":
            return httpx.Response(201, json={"id": "sb-1", "status": "pending"})
        if path == "/sandboxes/sb-1":
            return httpx.Response(200, json={"id": "sb-1", "name": "tts-task-1", "status": next(statuses)})
        raise AssertionError(f"unexpected {request.method} {path}")

    sandbox = await _provider(handler, spot=True).create_sandbox(_request(0.25))

    assert (sandbox.id, sandbox.state) == ("sb-1", "running")
    assert all(call.headers["Authorization"] == "Bearer key" for call in calls)
    assert json.loads(calls[0].content) == {"source": {"type": "registry", "ref": "python:3.13-slim"}}
    create = next(call for call in calls if call.method == "POST" and call.url.path == "/sandboxes")
    assert create.headers["Idempotency-Key"] == "tts-task-1"
    assert json.loads(create.content) == {
        "image": "img-1",
        "name": "tts-task-1",
        "labels": {"run": "r1"},
        "env": {"A": "1"},
        "cpu": 0.125,
        "cores_max": 0.5,
        "memory_mib": 1024,
        "memory_max_mib": 1024,
        "disk_mib": 3072,
        "spot": True,
        "create_timeout_seconds": 120,
        "wait_seconds": 0,
        "lifecycle_policy": {"idle_expiry_seconds": 900, "on_idle_timeout": "delete"},
    }


async def test_failed_sandbox_is_deleted_and_raises() -> None:
    deleted: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/sandboxes":
            return httpx.Response(201, json={"id": "sb-2", "status": "failed", "error": "no capacity"})
        if request.method == "DELETE":
            deleted.append(request.url.path)
            return httpx.Response(204)
        raise AssertionError(f"unexpected {request.method} {request.url.path}")

    request = _request(0.5, source=daytona_v2.SnapshotSource(snapshot="tts-image"))
    with pytest.raises(SandboxError, match="no capacity"):
        await _provider(handler).create_sandbox(request)
    assert deleted == ["/sandboxes/sb-2"]


async def test_sandbox_is_deleted_when_startup_polling_fails() -> None:
    deleted: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/sandboxes":
            return httpx.Response(201, json={"id": "sb-3", "status": "pending"})
        if request.method == "GET":
            return httpx.Response(403, text="forbidden")
        deleted.append(request.url.path)
        return httpx.Response(204)

    request = _request(0.5, source=daytona_v2.SnapshotSource(snapshot="tts-image"))
    with pytest.raises(SandboxError, match="HTTP 403"):
        await _provider(handler).create_sandbox(request)
    assert deleted == ["/sandboxes/sb-3"]


async def test_unsupported_features_fail_before_any_api_call() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request expected")

    request = _request(1, resources=Resources(vcpu=1, memory=1, disk=1, gpu=1))
    with pytest.raises(SandboxError, match="gpu"):
        await _provider(handler).create_sandbox(request)


def _sandbox(handler: Handler) -> DaytonaV2Sandbox:
    client = httpx.AsyncClient(base_url=API, transport=httpx.MockTransport(handler))
    return DaytonaV2Sandbox({"id": "sb-1", "status": "running", "toolbox_proxy_url": f"{TOOLBOX}/"}, client)


def _process_handler(
    exit_code: int,
    created: list[dict[str, Any]],
    signals: list[dict[str, Any]] | None = None,
    final: dict[str, Any] | None = None,
) -> Handler:
    pages = {
        None: {"frames": [{"channel": "stdout", "data": "hi "}], "nextCursor": "c1", "eof": False},
        "c1": {"frames": [{"channel": "stderr", "data": "err"}], "nextCursor": "c2", "eof": True},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/sb-1/processes":
            created.append(json.loads(request.content))
            return httpx.Response(201, json={"id": "p1", "state": "running"})
        if path == "/sb-1/processes/p1/logs":
            assert request.url.params["encoding"] == "text"
            return httpx.Response(200, json=pages[request.url.params.get("cursor")])
        if path == "/sb-1/processes/p1":
            return httpx.Response(200, json=final or {"id": "p1", "state": "exited", "exitCode": exit_code})
        if signals is not None and path == "/sb-1/processes/p1/signals":
            signals.append(json.loads(request.content))
            return httpx.Response(202, json={})
        raise AssertionError(f"unexpected {request.method} {path}")

    return handler


async def test_exec_collects_logs_and_exit_code() -> None:
    created: list[dict[str, Any]] = []
    result = await _sandbox(_process_handler(3, created)).exec("echo hi", cwd="/work", timeout=5)
    assert (result.exit_code, result.output) == (3, "hi err")
    assert created == [{"kind": "exec", "shellCommand": "echo hi", "cwd": "/work", "timeoutMs": 5000}]


@pytest.mark.parametrize(
    ("final", "exit_code"),
    [
        ({"state": "terminal", "signal": "SIGKILL", "reason": "timed_out"}, 124),
        ({"state": "terminal", "signal": "SIGKILL"}, 137),
        ({"state": "terminal", "signal": "TERM"}, 143),
    ],
)
async def test_exec_maps_timed_out_and_signalled_processes_to_exit_codes(final: dict[str, Any], exit_code: int) -> None:
    sandbox = _sandbox(_process_handler(0, [], final={"id": "p1", **final}))
    result = await asyncio.wait_for(sandbox.exec("sleep 30", timeout=3), 5)
    assert result.exit_code == exit_code


async def test_command_streams_output_and_raises_on_failure() -> None:
    created: list[dict[str, Any]] = []
    chunks: list[str] = []
    with pytest.raises(SandboxCommandError):
        async for chunk in _sandbox(_process_handler(2, created)).command("run", env_vars={"K": "v"}):
            chunks.append(chunk)
    assert chunks == ["hi ", "err"]
    assert created[0]["env"] == {"K": "v"}


STOP_SIGNAL = {"signal": "SIGTERM", "escalateAfterMs": 5000, "escalateTo": "SIGKILL"}


async def test_closing_command_stops_remote_process() -> None:
    signals: list[dict[str, Any]] = []
    stream = _sandbox(_process_handler(0, [], signals)).command("serve")
    assert await anext(stream) == "hi "
    await stream.aclose()
    assert signals == [STOP_SIGNAL]


async def test_cancelling_exec_stops_remote_process() -> None:
    signals: list[dict[str, Any]] = []
    polled = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/sb-1/processes":
            return httpx.Response(201, json={"id": "p1", "state": "running"})
        if request.url.path == "/sb-1/processes/p1/logs":
            polled.set()
            return httpx.Response(200, json={"frames": [], "nextCursor": None, "eof": False})
        assert request.url.path == "/sb-1/processes/p1/signals"
        signals.append(json.loads(request.content))
        return httpx.Response(202, json={})

    task = asyncio.create_task(_sandbox(handler).exec("sleep 100"))
    await polled.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert signals == [STOP_SIGNAL]


async def test_file_transfer_uses_toolbox_paths() -> None:
    seen: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.url.params["path"]))
        if request.method == "POST":
            assert b'name="file"' in request.content and b"payload" in request.content
            return httpx.Response(200, json={})
        return httpx.Response(200, content=b"data")

    sandbox = _sandbox(handler)
    await sandbox.upload_file("/tmp/in.txt", b"payload")
    assert await sandbox.download_file("/tmp/out.txt") == b"data"
    assert seen == [("POST", "/sb-1/files/upload", "/tmp/in.txt"), ("GET", "/sb-1/files/download", "/tmp/out.txt")]


async def test_missing_file_is_not_a_lost_sandbox() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/sandboxes/sb-1":
            return httpx.Response(200, json={"id": "sb-1", "status": "running"})
        return httpx.Response(404, text="no such file")

    with pytest.raises(SandboxError, match="/tmp/missing not found") as raised:
        await _sandbox(handler).download_file("/tmp/missing")
    assert not isinstance(raised.value, SandboxNotFoundError)


async def test_file_transfer_on_lost_sandbox_raises_not_found() -> None:
    sandbox = _sandbox(lambda request: httpx.Response(404, text="not found"))
    with pytest.raises(SandboxNotFoundError):
        await sandbox.upload_file("/tmp/in.txt", b"payload")


async def test_list_sandboxes_pages_by_labels_and_skips_deleted() -> None:
    searches: list[httpx.QueryParams] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/sandboxes/search":
            searches.append(request.url.params)
            if "cursor" in request.url.params:
                return httpx.Response(200, json={"sandboxes": [{"id": "b"}, {"id": "gone"}], "next_cursor": None})
            return httpx.Response(200, json={"sandboxes": [{"id": "a"}, {"id": "going"}], "next_cursor": "n1"})
        sandbox_id = request.url.path.rsplit("/", 1)[-1]
        status = {"gone": "deleted", "going": "deleting"}.get(sandbox_id, "running")
        return httpx.Response(200, json={"id": sandbox_id, "status": status})

    found = [
        sandbox.id async for sandbox in _provider(handler).list_sandboxes(daytona_v2.SandboxQuery(labels={"run": "r1"}))
    ]
    assert found == ["a", "b"]
    assert json.loads(searches[0]["labels"]) == {"run": "r1"}
    assert searches[1]["cursor"] == "n1"
