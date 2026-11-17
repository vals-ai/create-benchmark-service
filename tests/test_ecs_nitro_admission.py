from __future__ import annotations

from typing import Any

import pytest
from ecs_nitro_fakes import FakeEc2, FakeEcs, make_config, run_task_params

from benchmark_service.sandbox.ecs_nitro.admission import Admission, validate_source
from benchmark_service.sandbox.ecs_nitro.config import STARTED_BY
from benchmark_service.sandbox.types import (
    DockerfileSource,
    ImageSource,
    Resources,
    SandboxError,
    SnapshotSource,
    UnsupportedSandboxConfigError,
)


def _admission(ecs: FakeEcs, **config: Any) -> Admission:
    return Admission(make_config(**config), ecs, FakeEc2())


@pytest.mark.asyncio
async def test_capacity_provider_without_nitro_is_refused() -> None:
    with pytest.raises(SandboxError, match="Nitro Enclaves"):
        await _admission(FakeEcs(nitro=False)).verify_infrastructure()


@pytest.mark.asyncio
async def test_admission_counts_running_sandboxes() -> None:
    ecs = FakeEcs()
    admission = _admission(ecs, max_sandboxes=1)
    source, resources = ImageSource(image="img"), Resources(vcpu=1, memory=2, disk=10)
    assert await admission.admits(source, resources)
    ecs.tasks["t"] = {"taskArn": "arn/t", "lastStatus": "RUNNING", "startedBy": STARTED_BY}
    assert not await admission.admits(source, resources)


@pytest.mark.asyncio
async def test_reservation_demand_matches_capacity_domain() -> None:
    ecs = FakeEcs()
    admission = _admission(ecs, max_sandboxes=2, max_vcpu=4, max_memory_gib=8)
    demand = admission.demand(ImageSource(image="img"), Resources(vcpu=1.5, memory=4, disk=10))
    assert (demand.vcpu, demand.memory, demand.disk, demand.gpu) == (1.5, 4, 20, 0)
    params = {**run_task_params(), "overrides": {"cpu": "1536", "memory": "4096"}}
    await ecs.call("RunTask", params)
    domains = await admission.capacity_domains()
    [domain] = domains
    assert (domain.target_id, domain.sandbox_class) == (demand.target_id, demand.sandbox_class)
    assert (domain.capacity.cpu.total, domain.capacity.cpu.used) == (8, 1.5)
    assert (domain.capacity.memory.total, domain.capacity.memory.used) == (16, 4)
    assert (domain.capacity.disk.total, domain.capacity.disk.available) == (40, 20)


@pytest.mark.parametrize(
    ("resources", "match"),
    [
        (Resources(vcpu=1, memory=2, disk=10, gpu=1), "GPU"),
        (Resources(vcpu=32, memory=2, disk=10), "vCPU"),
        (Resources(vcpu=1, memory=128, disk=10), "memory"),
        (Resources(vcpu=1, memory=2, disk=21), "fixed 20 GiB"),
    ],
)
def test_unsupported_resources_are_refused(resources: Resources, match: str) -> None:
    with pytest.raises(UnsupportedSandboxConfigError, match=match):
        _admission(FakeEcs()).validate_resources(requires_nested_docker=False, resources=resources)


def test_only_prebuilt_images_and_no_nested_docker_are_accepted() -> None:
    for source in (SnapshotSource(snapshot="snap"), DockerfileSource(dockerfile="/work/Dockerfile")):
        with pytest.raises(UnsupportedSandboxConfigError, match="prebuilt images"):
            validate_source(source)
    with pytest.raises(UnsupportedSandboxConfigError, match="nested Docker"):
        _admission(FakeEcs()).validate_resources(
            requires_nested_docker=True, resources=Resources(vcpu=1, memory=2, disk=10)
        )
