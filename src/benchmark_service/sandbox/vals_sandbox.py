from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from benchmark_service.sandbox.types import MissingSandboxConfigError, SandboxProvider


class ValsSandboxProviderConfig(BaseModel):
    """Vals Sandbox (github.com/vals-ai/vals-sandbox). The ``VALS_SANDBOX_*`` keys pass through to it;
    keys left out are read from the environment."""

    model_config = ConfigDict(extra="allow")

    type: Literal["vals_sandbox"] = "vals_sandbox"

    def create_provider(self) -> SandboxProvider:
        try:
            from vals_sandbox.integrations.cbs import ValsSandboxProviderConfig as Config
        except ImportError as error:
            raise MissingSandboxConfigError(
                "The vals_sandbox provider needs the vals-sandbox package: install create-benchmark-service[vals-sandbox]"
            ) from error
        return Config.model_validate(self.model_dump()).create_provider()
