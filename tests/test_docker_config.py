"""Validation at the local Docker provider boundary."""

import pytest

from benchmark_service import (
    DockerProviderConfig,
    ImageSource,
    Resources,
    SandboxCreateRequest,
    SandboxError,
    SnapshotSource,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]
from benchmark_service.sandbox.local.docker import DockerSandboxProvider


def test_docker_requires_process_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject socket access unless the serving process explicitly enables Docker."""
    monkeypatch.delenv("CBS_DOCKER_ENABLED", raising=False)
    config = sandbox_provider_config_from_mapping({"type": "docker"})
    with pytest.raises(SandboxError, match="CBS_DOCKER_ENABLED"):
        config.create_provider()


def test_docker_grading_selects_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve Docker when selected for sandbox grading."""
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "docker")
    assert isinstance(_grading_provider_config(), DockerProviderConfig)


@pytest.mark.parametrize("feature", ["snapshot", "gpu", "secrets"])
async def test_docker_rejects_unsupported_features_before_connecting(
    feature: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Report unsupported requests without attempting to create a container."""
    monkeypatch.setenv("DOCKER_HOST", "unix:///does-not-exist.sock")
    request = SandboxCreateRequest(
        source=SnapshotSource(snapshot="snapshot") if feature == "snapshot" else ImageSource(image="image"),
        resources=Resources(vcpu=1, memory=1, disk=1, gpu=1 if feature == "gpu" else 0),
        name="test",
        labels={},
        env_vars={},
        auto_stop_interval=0,
        create_timeout=1,
        sandbox_secrets={"TOKEN": "reference"} if feature == "secrets" else {},
    )
    async with DockerSandboxProvider() as provider:
        with pytest.raises(SandboxError, match="supports image|does not support"):
            await provider.create_sandbox(request)
