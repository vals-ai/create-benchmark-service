import builtins
from typing import Any

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
    real_import = builtins.__import__

    def without_vals_sandbox(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("vals_sandbox"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_vals_sandbox)

    with pytest.raises(MissingSandboxConfigError, match="vals-sandbox package"):
        ValsSandboxProviderConfig().create_provider()
