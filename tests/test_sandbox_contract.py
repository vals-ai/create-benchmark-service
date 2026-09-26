"""Tests for concrete sandbox contract implementations.

Run: uv run pytest tests/test_sandbox_contract.py
"""

import subprocess
import sys
import textwrap
from importlib import import_module

from benchmark_service import SandboxCapacityDomain as RootSandboxCapacityDomain
from benchmark_service.sandbox import (
    ModalProviderConfig,
    ResourceCapacity,
    Sandbox,
    SandboxCapacity,
    SandboxCapacityDomain,
    SandboxProvider,
)


def test_sandbox_implementations_are_concrete() -> None:
    """Verify every loaded production implementation satisfies its abstract contract.

    Test cases:
    - Sandbox implementations have no missing abstract methods.
    - SandboxProvider implementations have no missing abstract methods.
    """
    import_module("benchmark_service.sandbox.daytona")
    import_module("benchmark_service.sandbox.modal")
    incomplete = {
        implementation.__name__: sorted(implementation.__abstractmethods__)
        for contract in (Sandbox, SandboxProvider)
        for implementation in contract.__subclasses__()
        if implementation.__module__.startswith("benchmark_service.sandbox.") and implementation.__abstractmethods__
    }

    assert not incomplete, f"Sandbox implementations are missing abstract methods: {incomplete}"


def test_capacity_contract_is_exported_and_backward_compatible() -> None:
    assert RootSandboxCapacityDomain is SandboxCapacityDomain
    resource = ResourceCapacity(total=1, used=0)
    capacity = SandboxCapacity(cpu=resource, memory=resource, disk=resource)

    assert capacity.gpu is None
    assert capacity.allowed_gpu_types is None


async def test_provider_capacity_defaults_to_unsupported() -> None:
    provider = ModalProviderConfig(MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret").create_provider()

    assert await provider.get_capacity() is None
    assert await provider.get_capacity_domains() is None


def test_provider_configs_remain_usable_without_optional_sdks() -> None:
    """Keep public request schemas usable when neither provider extra is installed.

    Test cases:
    - Core package imports and provider-config round trips do not load provider SDKs.
    - Selecting an unavailable provider explains which extra to install.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import sys

                for name in ("daytona", "daytona_api_client_async", "modal"):
                    sys.modules[name] = None

                from benchmark_service import DaytonaProviderConfig, ModalProviderConfig
                from benchmark_service.schemas import SetupTaskRequest

                configs = (
                    DaytonaProviderConfig(
                        DAYTONA_API_KEY="key",
                        DAYTONA_API_URL="https://daytona.example.test",
                        DAYTONA_TARGET="us",
                    ),
                    ModalProviderConfig(MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret"),
                )
                SetupTaskRequest.model_json_schema()
                for config in configs:
                    request = SetupTaskRequest(task_id="task", instance_id="sandbox", sandbox_provider=config)
                    restored = SetupTaskRequest.model_validate_json(request.model_dump_json())
                    assert restored.sandbox_provider == config
                    try:
                        config.create_provider()
                    except ImportError as exc:
                        assert f"create-benchmark-service[{config.type}]" in str(exc), str(exc)
                    else:
                        raise AssertionError(f"{config.type} provider did not require its extra")
                """
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
