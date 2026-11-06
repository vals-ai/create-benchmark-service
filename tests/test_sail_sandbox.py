import asyncio
import base64
import json
from collections.abc import AsyncGenerator, Callable
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from benchmark_service import (
    ExecResult,
    ImageSource,
    Resources,
    SailProviderConfig,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    SnapshotSource,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.sandbox.sail import SailSandbox, SailSandboxProvider
from benchmark_service.vals.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]


def request(**changes: Any) -> SandboxCreateRequest:
    return SandboxCreateRequest.model_validate(
        {
            "source": ImageSource(image="sail:debian"),
            "resources": Resources(vcpu=2, memory=4, disk=20),
            "name": "test-run",
            "labels": {"run-id": "run-1"},
            "env_vars": {"EXAMPLE": "value"},
            "auto_stop_interval": 10,
            "create_timeout": 10,
            **changes,
        }
    )


def info(**changes: Any) -> dict[str, Any]:
    return {
        "sailbox_id": "sb_123",
        "name": "test-run",
        "status": "running",
        "app_id": "app_123",
        "created_at": "2026-11-01T00:00:00Z",
        "env": {"CBS_SANDBOX_MANAGED": "1", "CBS_SANDBOX_LABELS": '{"run-id":"run-1"}'},
        **changes,
    }


def events(*items: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, text="\n".join(json.dumps(item) for item in items) + "\n")


def output(data: bytes, kind: str = "stdout") -> dict[str, Any]:
    return {"type": kind, "data": base64.b64encode(data).decode()}


@pytest.fixture
async def provider() -> AsyncGenerator[SailSandboxProvider, None]:
    instance = SailSandboxProvider(SailProviderConfig(SAIL_API_KEY="test-key"))
    await instance.client.aclose()
    yield instance
    await instance.close()


def transport(
    provider: SailSandboxProvider,
    handler: Callable[[httpx.Request], Any],
) -> None:
    provider.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://sailbox-api.sailresearch.com/v1",
        headers=provider.client.headers,
    )


def test_config_round_trip_and_grading_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    config = sandbox_provider_config_from_mapping({"type": "sail", "SAIL_API_KEY": "test-key"})
    assert isinstance(config, SailProviderConfig)
    assert "test-key" not in repr(config)
    assert sandbox_provider_config_from_mapping(config.model_dump()) == config
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "sail")
    monkeypatch.setenv("SAIL_API_KEY", "test-key")
    assert _grading_provider_config() == config


@pytest.mark.parametrize(
    "image,expected",
    [
        ("sail:debian", {"base": "BASE_IMAGE_DEBIAN"}),
        ("sail:devbox", {"base": "BASE_IMAGE_DEVBOX"}),
        ("ubuntu:22.04", {"oci": {"ref": "docker.io/library/ubuntu:22.04"}}),
        ("example/task:v1", {"oci": {"ref": "docker.io/example/task:v1"}}),
        ("ghcr.io/example/task:v1", {"oci": {"ref": "ghcr.io/example/task:v1"}}),
    ],
)
async def test_create_maps_image_resources_and_durable_labels(
    provider: SailSandboxProvider,
    image: str,
    expected: dict[str, Any],
) -> None:
    calls: list[httpx.Request] = []

    def handle(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        if req.url.path == "/v1/apps/find":
            return httpx.Response(200, json={"id": "app_123"})
        return httpx.Response(200, json={"sailbox_id": "sb_123", "status": "running"})

    transport(provider, handle)
    sandbox = await provider.create_sandbox(request(source=ImageSource(image=image), network_block_all=True))
    body = json.loads(calls[-1].content)
    assert body["image"] == expected
    assert body["vcpu_count"] == 2
    assert body["memory_limit_gib"] == 16
    assert body["state_disk_limit_gib"] == 20
    assert body["max_lifetime_seconds"] > 0
    assert body["egress_policy"] == {"allowlist": []}
    assert body["env"]["EXAMPLE"] == "value"
    assert json.loads(body["env"]["CBS_SANDBOX_LABELS"]) == {"run-id": "run-1"}
    assert calls[-1].headers["Idempotency-Key"]
    assert sandbox.labels == {"run-id": "run-1"}


@pytest.mark.parametrize(
    "changes",
    [
        {"source": SnapshotSource(snapshot="daytona-snapshot")},
        {"source": ImageSource(image="123.dkr.ecr.us-east-1.amazonaws.com/private:v1")},
        {"resources": Resources(vcpu=1, memory=4, disk=20, gpu=1)},
        {"resources": Resources(vcpu=65, memory=4, disk=20)},
        {"env_vars": {"CBS_SANDBOX_MANAGED": "0"}},
        {"sandbox_secrets": {"SECRET": "secret-name"}},
        {"volumes": [{"name": "fixture", "mount_path": "/data"}]},
    ],
)
async def test_unsupported_requests_fail_before_any_api_call(
    provider: SailSandboxProvider,
    changes: dict[str, Any],
) -> None:
    calls: list[httpx.Request] = []
    transport(provider, lambda req: calls.append(req) or httpx.Response(500))
    with pytest.raises(SandboxError):
        await provider.create_sandbox(request(**changes))
    assert calls == []


async def test_failed_creation_is_deleted(provider: SailSandboxProvider) -> None:
    calls: list[str] = []

    def handle(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.path)
        if req.url.path.endswith("/apps/find"):
            return httpx.Response(200, json={"id": "app_123"})
        if req.url.path.endswith("/terminate"):
            return httpx.Response(200, json={})
        return httpx.Response(200, json={"sailbox_id": "sb_failed", "status": "failed"})

    transport(provider, handle)
    with pytest.raises(SandboxError, match="failed to start"):
        await provider.create_sandbox(request())
    assert calls[-1] == "/v1/sailboxes/sb_failed/terminate"


async def test_cancelled_creation_retains_ownership_until_late_allocation_is_deleted(
    provider: SailSandboxProvider,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()
    deleted: list[str] = []

    async def handle(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/apps/find"):
            return httpx.Response(200, json={"id": "app_123"})
        if req.url.path.endswith("/terminate"):
            deleted.append(req.url.path)
            return httpx.Response(200, json={})
        started.set()
        await release.wait()
        return httpx.Response(200, json={"sailbox_id": "sb_late", "status": "running"})

    transport(provider, handle)
    creating = asyncio.create_task(provider.create_sandbox(request()))
    await started.wait()
    creating.cancel()
    with pytest.raises(asyncio.CancelledError):
        await creating
    release.set()
    await provider.close()
    assert deleted == ["/v1/sailboxes/sb_late/terminate"]


async def test_command_decodes_split_unicode_and_preserves_nonzero_exit(provider: SailSandboxProvider) -> None:
    calls: list[dict[str, Any]] = []
    euro = "€".encode()

    def handle(req: httpx.Request) -> httpx.Response:
        calls.append(json.loads(req.content))
        return events(output(euro[:1]), output(euro[1:]), output(b"oops", "stderr"), {"type": "exit", "return_code": 7})

    transport(provider, handle)
    result = await SailSandbox(provider, info()).exec("printf test", cwd="/a b", timeout=1.2)
    assert result.exit_code == 7
    assert result.output == "€oops"
    assert calls[0]["command"] == ["/bin/sh", "-c", "cd '/a b' && printf test"]
    assert calls[0]["timeout"] == 2


async def test_stream_without_exit_fails_without_reexecuting(provider: SailSandboxProvider) -> None:
    calls: list[httpx.Request] = []
    transport(provider, lambda req: calls.append(req) or events(output(b"partial")))
    with pytest.raises(SandboxConnectionError, match="before its exit"):
        await SailSandbox(provider, info()).exec("mutate")
    assert len(calls) == 1


async def test_files_are_binary_and_egress_policy_uses_document_envelope(provider: SailSandboxProvider) -> None:
    calls: list[httpx.Request] = []

    def handle(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        return httpx.Response(200, content=b"\x00\xff")

    transport(provider, handle)
    sandbox = SailSandbox(provider, info())
    await sandbox.upload_file("/a b/?x", b"\x00\xff")
    assert calls[-1].url.params["path"] == "/a b/?x"
    assert calls[-1].content == b"\x00\xff"
    assert await sandbox.download_file("/a b/?x") == b"\x00\xff"
    await sandbox.block_all_egress()
    assert json.loads(calls[-1].content) == {"document": {"allowlist": []}}
    await sandbox.modify_egress_rules(["api.example.com", "192.0.2.0/24"])
    assert set(json.loads(calls[-1].content)["document"]["allowlist"]) == {"api.example.com", "192.0.2.0/24"}
    await sandbox.clear_egress_rules()
    assert json.loads(calls[-1].content) == {"document": {}}


async def test_listing_pages_and_filters_managed_labels_and_creation_time(provider: SailSandboxProvider) -> None:
    offsets: list[str] = []

    def handle(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/apps/find"):
            return httpx.Response(200, json={"id": "app_123"})
        offsets.append(req.url.params["offset"])
        if req.url.params["offset"] == "0":
            return httpx.Response(
                200, json={"data": [info(env={}), info(created_at="2026-11-03T00:00:00Z")], "has_more": True}
            )
        return httpx.Response(200, json={"data": [info()], "has_more": False})

    transport(provider, handle)
    boxes = [
        box
        async for box in provider.list_sandboxes(
            SandboxQuery(
                labels={"run-id": "run-1"},
                page_size=2,
                created_at_lte=datetime(2026, 11, 2, tzinfo=timezone.utc),
            )
        )
    ]
    assert offsets == ["0", "2"]
    assert [box.id for box in boxes] == ["sb_123"]


@pytest.mark.parametrize(
    "status,expected", [(401, SandboxError), (404, SandboxNotFoundError), (503, SandboxConnectionError)]
)
async def test_http_failures_do_not_expose_response_secrets(
    provider: SailSandboxProvider,
    status: int,
    expected: type[Exception],
) -> None:
    transport(provider, lambda req: httpx.Response(status, text="secret-content"))
    with pytest.raises(expected) as error:
        await provider.get_sandbox("sb_123")
    assert "secret-content" not in str(error.value)


async def test_provider_clients_keep_tenant_credentials_separate(provider: SailSandboxProvider) -> None:
    authorizations: list[str] = []

    def handle(req: httpx.Request) -> httpx.Response:
        authorizations.append(req.headers["Authorization"])
        return httpx.Response(200, json=info())

    transport(provider, handle)
    other = SailSandboxProvider(SailProviderConfig(SAIL_API_KEY="tenant-two"))
    await other.client.aclose()
    transport(other, handle)
    try:
        await asyncio.gather(provider.get_sandbox("sb_1"), other.get_sandbox("sb_2"))
    finally:
        await other.close()
    assert authorizations == ["Bearer test-key", "Bearer tenant-two"]


async def test_controlled_workload_reports_foreground_status_after_confirmed_stop(
    provider: SailSandboxProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = SailSandbox(provider, info())

    async def command(*args: Any, **kwargs: Any) -> AsyncGenerator[str, None]:
        yield "agent output"
        raise SandboxCommandError(137)

    stop = AsyncMock(side_effect=[ExecResult(exit_code=0, output="3"), ExecResult(exit_code=0)])
    monkeypatch.setattr(sandbox, "command", command)
    monkeypatch.setattr(sandbox, "exec", stop)
    workload = sandbox.controlled_workload("run-agent")
    chunks = [chunk async for chunk in workload.output()]
    result = await workload.wait()
    assert chunks == ["agent output"]
    assert result.result == ExecResult(exit_code=3, output="agent output")
    assert result.absence_confirmed_at == workload.agent_absence_confirmed_at
    assert "kill -KILL" not in stop.call_args_list[-1].args[0]
