"""Tests for concrete sandbox contract implementations.

Run: uv run pytest tests/test_sandbox_contract.py
"""

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
