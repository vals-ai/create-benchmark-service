"""Tests for concrete sandbox contract implementations.

Run: uv run pytest tests/test_sandbox_contract.py
"""

from benchmark_service.sandbox import Sandbox, SandboxProvider


_SANDBOX_IMPLEMENTATIONS = tuple(
    implementation
    for contract in (Sandbox, SandboxProvider)
    for implementation in contract.__subclasses__()
    if implementation.__module__.startswith("benchmark_service.sandbox.")
)


def test_sandbox_implementations_are_concrete() -> None:
    """Verify every loaded production implementation satisfies its abstract contract.

    Test cases:
    - Sandbox implementations have no missing abstract methods.
    - SandboxProvider implementations have no missing abstract methods.
    """
    incomplete = {
        implementation.__name__: sorted(implementation.__abstractmethods__)
        for implementation in _SANDBOX_IMPLEMENTATIONS
        if implementation.__abstractmethods__
    }

    assert not incomplete, f"Sandbox implementations are missing abstract methods: {incomplete}"
