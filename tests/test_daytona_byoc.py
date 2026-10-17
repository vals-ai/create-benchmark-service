"""BYOC must not send an opted-in run to another Daytona region."""

from collections.abc import AsyncGenerator, AsyncIterator
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from daytona import AsyncDaytona, AsyncSandbox, DaytonaNotFoundError
from daytona.common.errors import DaytonaError
from daytona_api_client_async import OrganizationsApi, SandboxClass
from daytona_api_client_async.exceptions import ApiException
from daytona_api_client_async.models.region import Region
from daytona_api_client_async.models.region_type import RegionType
from daytona_api_client_async.models.sandbox_state import SandboxState
from fastapi.testclient import TestClient
from pydantic import ValidationError

from benchmark_service import (
    ComposeSource,
    DaytonaProviderConfig,
    ExecResult,
    ImageSource,
    Resources,
    Sandbox,
    SandboxCreateRequest,
    SandboxProvider,
    SandboxQuery,
    SandboxSource,
    TargetedSnapshotSource,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.context import current_sandbox_provider
from benchmark_service.sandbox import SandboxError
from benchmark_service.sandbox.daytona import DaytonaSandbox, DaytonaSandboxProvider
from benchmark_service.schemas import (
    EvaluateInstanceRequest,
    EvaluateResponseRequest,
    SetupTaskRequest,
    StreamChunk,
    StreamResultChunk,
)
from benchmark_service.vals.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]
from tests.conftest import StubBenchmark

REGION_ID = "valsmith-byoc_dev"
ORGANIZATION_ID = "11111111-1111-1111-1111-111111111111"
SECRET: dict[str, object] = {
    "DAYTONA_API_KEY": "test-key-must-not-appear-in-errors",
    "DAYTONA_API_URL": "https://app.daytona.io/api",
    "DAYTONA_TARGET": REGION_ID,
    "DAYTONA_ORGANIZATION_ID": ORGANIZATION_ID,
}


def region(**changes: object) -> Region:
    return Region.model_validate(
        {
            "id": REGION_ID,
            "name": "valsmith-byoc",
            "organization_id": ORGANIZATION_ID,
            "region_type": "custom",
            "created_at": "2026-10-05T00:00:00Z",
            "updated_at": "2026-10-05T00:00:00Z",
            **changes,
        }
    )


def sandbox(target: str = REGION_ID) -> AsyncSandbox:
    # The cloud call returns a sandbox. No toolbox or event connection is used here.
    return AsyncSandbox.model_construct(
        id="sandbox-id",
        name="byoc-task",
        organization_id=ORGANIZATION_ID,
        user="root",
        env={},
        labels={},
        public=False,
        network_block_all=False,
        target=target,
        cpu=1,
        gpu=0,
        memory=2,
        disk=5,
        state=SandboxState.STARTED,
    )


def request(source: SandboxSource | None = None) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=source or ImageSource(image="python:3.12-slim"),
        resources=Resources(vcpu=1, memory=2, disk=5),
        name="byoc-task",
        labels={},
        env_vars={},
        auto_stop_interval=10,
        create_timeout=60,
    )


@pytest.fixture
async def provider() -> AsyncIterator[SandboxProvider]:
    config = sandbox_provider_config_from_mapping({**SECRET, "type": "daytona-byoc"})
    async with config.create_provider() as instance:
        yield instance


@pytest.fixture
def region_lookup(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    lookup = AsyncMock(return_value=region())
    monkeypatch.setattr(OrganizationsApi, "get_region_by_id", lookup)
    return lookup


@pytest.fixture
def create(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    create = AsyncMock(return_value=sandbox())
    monkeypatch.setattr(AsyncDaytona, "create", create)
    return create


@pytest.mark.parametrize("missing", ["DAYTONA_TARGET", "DAYTONA_ORGANIZATION_ID", "DAYTONA_API_KEY"])
@pytest.mark.parametrize("value", [None, "", "  "])
def test_byoc_rejects_incomplete_configuration_without_exposing_keys(missing: str, value: object) -> None:
    with pytest.raises(ValidationError) as error:
        sandbox_provider_config_from_mapping({**SECRET, missing: value, "type": "daytona-byoc"})

    assert "test-key-must-not-appear-in-errors" not in str(error.value)


def test_managed_daytona_keeps_its_existing_configuration() -> None:
    config = sandbox_provider_config_from_mapping({**SECRET, "type": "daytona"})

    assert type(config) is DaytonaProviderConfig


async def test_byoc_grading_environment_uses_the_checked_provider(
    monkeypatch: pytest.MonkeyPatch,
    create: AsyncMock,
) -> None:
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "daytona-byoc")
    for name, value in SECRET.items():
        monkeypatch.setenv(name, str(value))

    async with _grading_provider_config().create_provider() as provider:
        with pytest.raises(SandboxError, match="snapshot.*region"):
            await provider.create_sandbox(request(TargetedSnapshotSource(snapshot="managed", target="us")))

    create.assert_not_awaited()


@pytest.mark.parametrize("snapshot", [False, True])
async def test_byoc_creates_in_verified_region_and_reuses_verification(
    provider: SandboxProvider, region_lookup: AsyncMock, create: AsyncMock, snapshot: bool
) -> None:
    source = TargetedSnapshotSource(snapshot="fixture", target=REGION_ID) if snapshot else None
    first = await provider.create_sandbox(request(source))
    second = await provider.create_sandbox(request(source))

    assert first.provider_metadata["target"] == REGION_ID
    assert second.id == "sandbox-id"
    region_lookup.assert_awaited_once_with(REGION_ID)
    assert create.await_count == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"region_type": RegionType.SHARED},
        {"region_type": RegionType.DEDICATED},
        {"region_type": RegionType.UNKNOWN_DEFAULT_OPEN_API},
        {"organization_id": "other-org"},
        {"organization_id": None},
        {"id": "different-region"},
    ],
)
async def test_byoc_rejects_unowned_or_noncustom_region_before_creation(
    provider: SandboxProvider, region_lookup: AsyncMock, create: AsyncMock, changes: dict[str, object]
) -> None:
    region_lookup.return_value = region(**changes)

    with pytest.raises(SandboxError, match="BYOC"):
        await provider.create_sandbox(request())

    create.assert_not_awaited()


async def test_byoc_region_lookup_failure_does_not_fall_back_or_cache_success(
    provider: SandboxProvider, region_lookup: AsyncMock, create: AsyncMock
) -> None:
    region_lookup.side_effect = [ApiException(status=403), region()]

    with pytest.raises(SandboxError, match="BYOC"):
        await provider.create_sandbox(request())

    create.assert_not_awaited()
    result = await provider.create_sandbox(request())
    assert result.provider_metadata["target"] == REGION_ID
    assert region_lookup.await_count == 2


async def test_byoc_rejects_snapshot_region_override_before_admission_or_creation(
    provider: SandboxProvider, region_lookup: AsyncMock, create: AsyncMock
) -> None:
    source = TargetedSnapshotSource(snapshot="managed-fixture", target="us")

    with pytest.raises(SandboxError, match="snapshot.*region"):
        await provider.check_admission(source, request().resources)

    with pytest.raises(SandboxError, match="snapshot.*region"):
        await provider.create_sandbox(request(source))

    region_lookup.assert_not_awaited()
    create.assert_not_awaited()


async def test_byoc_refuses_wrong_actual_placement_without_deleting_it(
    provider: SandboxProvider,
    region_lookup: AsyncMock,
    create: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del region_lookup
    misplaced = sandbox("us")
    create.return_value = misplaced
    monkeypatch.setattr(AsyncDaytona, "get", AsyncMock(return_value=misplaced))
    delete = AsyncMock()
    monkeypatch.setattr(AsyncDaytona, "delete", delete)

    with pytest.raises(SandboxError, match="placement"):
        await provider.create_sandbox(request())

    delete.assert_not_awaited()


async def test_byoc_refuses_to_access_or_delete_an_existing_sandbox_in_another_region(
    provider: SandboxProvider, region_lookup: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    del region_lookup
    monkeypatch.setattr(AsyncDaytona, "get", AsyncMock(return_value=sandbox("us")))
    delete = AsyncMock()
    monkeypatch.setattr(AsyncDaytona, "delete", delete)

    with pytest.raises(SandboxError, match="placement"):
        await provider.get_sandbox("existing-managed-sandbox")

    with pytest.raises(SandboxError, match="placement"):
        await provider.delete_sandbox("existing-managed-sandbox")

    delete.assert_not_awaited()


async def test_byoc_provider_selection_checks_region_before_access(
    region_lookup: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AsyncDaytona, "get", AsyncMock(return_value=sandbox()))
    config = sandbox_provider_config_from_mapping({**SECRET, "type": "daytona-byoc"})
    async with config.create_provider() as provider:
        result = await provider.get_sandbox("sandbox-id")

    assert result.provider_metadata["target"] == REGION_ID
    region_lookup.assert_awaited_once_with(REGION_ID)


def usage(target: str) -> SimpleNamespace:
    return SimpleNamespace(
        region_id=target,
        sandbox_class=SandboxClass.CONTAINER,
        total_cpu_quota=10,
        current_cpu_usage=0,
        total_memory_quota=10,
        current_memory_usage=0,
        total_disk_quota=10,
        current_disk_usage=0,
        total_gpu_quota=0,
        current_gpu_usage=0,
        allowed_gpu_types=[],
        max_cpu_per_sandbox=None,
        max_memory_per_sandbox=None,
        max_disk_per_sandbox=None,
    )


async def test_byoc_capacity_and_compose_admission_use_only_selected_region(
    provider: SandboxProvider, region_lookup: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    overview = AsyncMock(return_value=SimpleNamespace(region_usage=[usage("us"), usage(REGION_ID)]))
    monkeypatch.setattr(OrganizationsApi, "get_organization_usage_overview", overview)
    domains = await provider.get_capacity_domains()
    assert domains is not None
    assert [domain.target_id for domain in domains] == [REGION_ID]
    assert await provider.get_capacity() == domains[0].capacity
    assert await provider.check_admission(
        ComposeSource(outer=ImageSource(image="python:3.12-slim")), request().resources
    )
    region_lookup.assert_awaited_once_with(REGION_ID)


async def test_byoc_cleanup_list_rejects_an_unexpected_region(
    provider: SandboxProvider, region_lookup: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(DaytonaSandboxProvider, "_list_sandboxes", AsyncMock(return_value=[sandbox(), sandbox("us")]))
    accepted: list[str] = []
    with pytest.raises(SandboxError, match="placement"):
        async for result in provider.list_sandboxes(SandboxQuery(labels={})):
            accepted.append(result.provider_metadata["target"])

    assert accepted == [REGION_ID]
    region_lookup.assert_awaited_once_with(REGION_ID)


async def test_byoc_cleanup_deletes_owned_sandbox_and_tolerates_already_deleted(
    provider: SandboxProvider, region_lookup: AsyncMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    existing = sandbox()
    get = AsyncMock(side_effect=[existing, DaytonaNotFoundError("removed")])
    delete = AsyncMock()
    monkeypatch.setattr(AsyncDaytona, "get", get)
    monkeypatch.setattr(AsyncDaytona, "delete", delete)

    await provider.delete_sandbox("sandbox-id")
    await provider.delete_sandbox("sandbox-id")

    delete.assert_awaited_once_with(existing)
    region_lookup.assert_awaited_once_with(REGION_ID)


@pytest.mark.parametrize("state", [SandboxState.STARTED, SandboxState.ERROR])
async def test_byoc_name_conflict_never_reuses_or_deletes_a_foreign_sandbox(
    provider: SandboxProvider,
    region_lookup: AsyncMock,
    create: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    state: SandboxState,
) -> None:
    create.side_effect = DaytonaError("Sandbox with name byoc-task already exists")
    foreign = sandbox("us")
    foreign.state = state
    monkeypatch.setattr(AsyncDaytona, "get", AsyncMock(return_value=foreign))
    delete = AsyncMock()
    wait = AsyncMock()
    monkeypatch.setattr(AsyncDaytona, "delete", delete)
    monkeypatch.setattr(AsyncSandbox, "wait_for_sandbox_start", wait)

    with pytest.raises(SandboxError, match="placement"):
        await provider.create_sandbox(request())

    delete.assert_not_awaited()
    wait.assert_not_awaited()
    region_lookup.assert_awaited_once_with(REGION_ID)


@pytest.mark.parametrize("request_type", [SetupTaskRequest, EvaluateInstanceRequest, EvaluateResponseRequest])
async def test_byoc_rejects_region_override_after_service_request_transfer(
    request_type: type[SetupTaskRequest] | type[EvaluateInstanceRequest] | type[EvaluateResponseRequest],
    create: AsyncMock,
) -> None:
    outgoing = request_type.model_validate(
        {
            "task_id": "task-1",
            "instance_id": "sandbox-id",
            "response": "answer",
            "sandbox_provider": {**SECRET, "type": "daytona-byoc"},
        }
    )
    incoming = request_type.model_validate_json(outgoing.model_dump_json())
    assert incoming.sandbox_provider is not None
    async with incoming.sandbox_provider.create_provider() as provider:
        with pytest.raises(SandboxError, match="snapshot.*region"):
            await provider.create_sandbox(request(TargetedSnapshotSource(snapshot="managed", target="us")))

    create.assert_not_awaited()


@pytest.mark.parametrize("actual_target", [REGION_ID, "us"])
def test_service_grading_checks_placement_of_its_extra_sandbox(
    region_lookup: AsyncMock,
    create: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    actual_target: str,
) -> None:
    class GradingBenchmark(StubBenchmark):
        async def evaluate_instance(
            self,
            task_id: str,
            sandbox: Sandbox,
            dataset: str | None = None,
        ) -> AsyncGenerator[StreamChunk, None]:
            provider = current_sandbox_provider()
            assert provider is not None
            verifier = await provider.create_sandbox(request())
            result = await verifier.exec("grade")
            yield StreamResultChunk(type="result", data={"resolved": result.exit_code == 0})

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    monkeypatch.setattr(AsyncDaytona, "get", AsyncMock(return_value=sandbox()))
    execute = AsyncMock(return_value=ExecResult(exit_code=0, output="passed"))
    monkeypatch.setattr(DaytonaSandbox, "exec", execute)
    create.return_value = sandbox(actual_target)
    with TestClient(BenchmarkServiceApp(GradingBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-instance") as websocket:
            websocket.send_json(
                {
                    "task_id": "task-1",
                    "instance_id": "sandbox-id",
                    "sandbox_provider": {**SECRET, "type": "daytona-byoc"},
                }
            )
            result = websocket.receive_json()

    region_lookup.assert_awaited_once_with(REGION_ID)
    if actual_target == REGION_ID:
        assert result == {"type": "result", "data": {"resolved": True}}
        execute.assert_awaited_once_with("grade")
    else:
        assert result["type"] == "error"
        assert "placement" in result["data"]
        execute.assert_not_awaited()
