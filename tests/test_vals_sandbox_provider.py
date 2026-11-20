import sys

import pytest

from benchmark_service.sandbox import (
    MissingSandboxConfigError,
    ValsSandboxProviderConfig,
    sandbox_provider_config_from_mapping,
)


def test_vals_sandbox_config_keeps_its_settings() -> None:
    config = sandbox_provider_config_from_mapping({"type": "vals_sandbox", "VALS_SANDBOX_REGION": "us-west-2"})

    assert isinstance(config, ValsSandboxProviderConfig)
    assert config.model_dump() == {"type": "vals_sandbox", "VALS_SANDBOX_REGION": "us-west-2"}


def test_vals_sandbox_provider_needs_the_package(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "vals_sandbox", None)

    with pytest.raises(MissingSandboxConfigError, match="vals-sandbox package"):
        ValsSandboxProviderConfig().create_provider()
