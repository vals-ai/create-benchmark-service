from typing import cast
from unittest.mock import AsyncMock, Mock

from benchmark_service import SandboxProviders, SandboxRecoveryAttempt, run_with_sandbox_recovery
from benchmark_service.sandbox import SandboxNotFoundError, SandboxProviderConfig
from benchmark_service.schemas import RetrieveTaskResponse


def _task_response(max_sandbox_attempts: int) -> RetrieveTaskResponse:
    return RetrieveTaskResponse.model_validate(
        {
            "source": {"type": "image", "image": "python:3.12"},
            "problem_path": "/tmp/problem_statement.txt",
            "cwd": "/work",
            "resources": {"vcpu": 2, "memory": 4, "disk": 10},
            "sandbox_recovery": {"max_sandbox_attempts": max_sandbox_attempts},
        }
    )


async def test_recovery_without_a_client_loads_the_task_once_and_retries_a_lost_sandbox() -> None:
    load_task = AsyncMock(return_value=_task_response(2))

    async def operation(attempt: SandboxRecoveryAttempt) -> int:
        if attempt.number == 1:
            raise SandboxNotFoundError("sandbox disappeared")
        return attempt.number

    result = await run_with_sandbox_recovery(load_task, "task-1", "run-1", operation, retry_delay_s=0)

    assert result == 2
    load_task.assert_awaited_once()


async def test_recovery_without_a_client_never_loads_the_task_when_the_first_attempt_succeeds() -> None:
    load_task = AsyncMock(return_value=_task_response(2))

    async def operation(attempt: SandboxRecoveryAttempt) -> int:
        return attempt.number

    assert await run_with_sandbox_recovery(load_task, "task-1", "run-1", operation) == 1
    load_task.assert_not_awaited()


async def test_sandbox_providers_create_one_provider_per_config_and_close_them_all() -> None:
    first, second = AsyncMock(), AsyncMock()
    config_a = Mock()
    config_a.model_dump_json.return_value = "a"
    config_a.create_provider.return_value = first
    config_b = Mock()
    config_b.model_dump_json.return_value = "b"
    config_b.create_provider.return_value = second
    providers = SandboxProviders()
    a, b = cast(SandboxProviderConfig, config_a), cast(SandboxProviderConfig, config_b)

    assert providers.get(a) is providers.get(a) is first
    assert providers.get(b) is second
    await providers.close()

    config_a.create_provider.assert_called_once()
    first.close.assert_awaited_once()
    second.close.assert_awaited_once()
