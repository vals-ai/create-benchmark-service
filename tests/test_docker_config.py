"""Validation at the local Docker provider boundary."""

import pytest

from benchmark_service import (
    DockerProviderConfig,
    SandboxError,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]


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
