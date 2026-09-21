"""Validation at the local Docker provider boundary."""

import pytest

from benchmark_service import DockerProviderConfig
from benchmark_service.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]


def test_docker_grading_selects_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve Docker when selected for sandbox grading."""
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "docker")
    assert isinstance(_grading_provider_config(), DockerProviderConfig)
