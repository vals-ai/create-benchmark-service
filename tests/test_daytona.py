# Test specific Daytona sandbox methods that are not just straightforward SDK wrappers.

from unittest.mock import AsyncMock, patch

import pytest
from daytona import SandboxState
from daytona.common.errors import DaytonaError
from pydantic import ValidationError

from benchmark_service.sandbox import (
    InvalidSandboxConfigurationError,
    SandboxCreateRequest,
    SandboxResources,
    SandboxSourceType,
)
from benchmark_service.sandbox.daytona import DaytonaSandbox, DaytonaSandboxProvider


class _FakeInnerSandbox:
    def __init__(
        self,
        sandbox_id: str,
        name: str,
        state: SandboxState,
        wait_effects: list[Exception | None] | None = None,
    ) -> None:
        self.id = sandbox_id
        self.name = name
        self.state = state
        self.wait_calls = 0
        self.wait_effects = wait_effects or []

    async def wait_for_sandbox_start(self, timeout: float | None = 60) -> None:
        self.wait_calls += 1
        if self.wait_effects:
            effect = self.wait_effects.pop(0)
            if effect is not None:
                raise effect
        self.state = SandboxState.STARTED


class _FakeDaytonaClient:
    def __init__(self, inner: _FakeInnerSandbox, delete_effects: list[Exception | None] | None = None) -> None:
        self.inner = inner
        self.delete_effects = delete_effects or []
        self.delete_calls = 0
        self.create_calls = 0

    async def get(self, sandbox_id: str) -> _FakeInnerSandbox:
        assert sandbox_id == self.inner.id
        return self.inner

    async def create(self, *args, **kwargs) -> _FakeInnerSandbox:
        self.create_calls += 1
        return self.inner

    async def delete(self, inner: _FakeInnerSandbox) -> None:
        assert inner is self.inner
        self.delete_calls += 1
        if self.delete_effects:
            effect = self.delete_effects.pop(0)
            if effect is not None:
                raise effect

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_delete_sandbox_waits_for_start_before_delete() -> None:
    inner = _FakeInnerSandbox("sandbox-1", "sandbox-1", SandboxState.STARTING)
    daytona = _FakeDaytonaClient(inner)
    provider = DaytonaSandboxProvider(daytona)
    sandbox = DaytonaSandbox(provider=provider, inner=inner)

    await provider.delete_sandbox(sandbox)

    assert inner.wait_calls == 1
    assert daytona.delete_calls == 1


@pytest.mark.asyncio
async def test_delete_sandbox_retries_transient_state_transition_error() -> None:
    inner = _FakeInnerSandbox("sandbox-1", "sandbox-1", SandboxState.STARTED)
    daytona = _FakeDaytonaClient(
        inner,
        delete_effects=[DaytonaError("Sandbox state change in progress"), None],
    )
    provider = DaytonaSandboxProvider(daytona)
    sandbox = DaytonaSandbox(provider=provider, inner=inner)

    with patch("benchmark_service.sandbox.daytona.asyncio.sleep", new=AsyncMock()) as mock_sleep:
        await provider.delete_sandbox(sandbox)

    assert daytona.delete_calls == 2
    mock_sleep.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_sandbox_attempts_delete_when_wait_for_start_fails() -> None:
    inner = _FakeInnerSandbox(
        "sandbox-1",
        "sandbox-1",
        SandboxState.STARTING,
        wait_effects=[DaytonaError("Failure during waiting for sandbox to start: timed out")],
    )
    daytona = _FakeDaytonaClient(inner)
    provider = DaytonaSandboxProvider(daytona)
    sandbox = DaytonaSandbox(provider=provider, inner=inner)

    with patch("benchmark_service.sandbox.daytona.asyncio.sleep", new=AsyncMock()) as mock_sleep:
        await provider.delete_sandbox(sandbox)

    assert inner.wait_calls == 1
    assert daytona.delete_calls == 1
    mock_sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_sandbox_does_not_retry_invalid_configuration() -> None:
    inner = _FakeInnerSandbox("sandbox-1", "sandbox-1", SandboxState.STARTED)
    daytona = _FakeDaytonaClient(inner)
    provider = DaytonaSandboxProvider(daytona)
    request = SandboxCreateRequest(
        source_id="",
        source_type=SandboxSourceType.SNAPSHOT,
        name="sandbox-1",
    )

    with patch("benchmark_service.sandbox.daytona.asyncio.sleep", new=AsyncMock()) as mock_sleep:
        with pytest.raises(InvalidSandboxConfigurationError, match="without a snapshot name"):
            await provider.create_sandbox(request)

    assert daytona.create_calls == 0
    mock_sleep.assert_not_awaited()


def test_sandbox_resources_rejects_fractional_cpu() -> None:
    with pytest.raises(ValidationError, match="valid integer"):
        SandboxResources(cpu=0.5, memory=4, disk=10)
