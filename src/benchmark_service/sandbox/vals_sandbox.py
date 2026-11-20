from __future__ import annotations

import importlib
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

from benchmark_service.sandbox.types import MissingSandboxConfigError, SandboxProvider


class ValsSandboxProviderConfig(BaseModel):
    """Vals Sandbox (github.com/vals-ai/vals-sandbox). The ``VALS_SANDBOX_*`` keys pass through to it;
    keys left out are read from the environment."""

    model_config = ConfigDict(extra="allow")

    type: Literal["vals_sandbox"] = "vals_sandbox"

    def create_provider(self) -> SandboxProvider:
        try:
            module = importlib.import_module("vals_sandbox.integrations.cbs")
        except ImportError as error:
            raise MissingSandboxConfigError(
                "The vals_sandbox provider needs the vals-sandbox package (github.com/vals-ai/vals-sandbox)"
            ) from error
        config: _ProviderConfig = module.ValsSandboxProviderConfig.model_validate(self.model_dump())
        return config.create_provider()


class _ProviderConfig(Protocol):
    def create_provider(self) -> SandboxProvider: ...
