"""Docker configuration and command cleanup checks; run pytest tests/test_docker_config.py."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiodocker.containers import DockerContainer
from aiodocker.execs import Exec
from aiodocker.stream import Stream

from benchmark_service import DockerProviderConfig
from benchmark_service.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]
from benchmark_service.sandbox.local.docker import DockerSandbox, _ContainerInfo  # pyright: ignore[reportPrivateUsage]


def test_docker_grading_selects_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve Docker when selected for sandbox grading."""
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "docker")
    assert isinstance(_grading_provider_config(), DockerProviderConfig)


@pytest.mark.parametrize("outcome", ["exit", "timeout", "cancel_before_cleanup", "cancel_during_cleanup"])
async def test_cleanup_timeout_preserves_command_outcome(
    outcome: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Retain exit codes and cancellation when Docker command cleanup times out."""
    command_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    timeout = asyncio.timeout

    def cleanup_timeout(_delay: float | None) -> asyncio.Timeout:
        return timeout(0.01)

    monkeypatch.setattr(asyncio, "timeout", cleanup_timeout)

    async def read_output() -> None:
        command_started.set()
        if outcome in {"timeout", "cancel_before_cleanup"}:
            await asyncio.Event().wait()

    stream = MagicMock(spec=Stream)
    stream.read_out = AsyncMock(side_effect=read_output)
    execution = MagicMock(spec=Exec)
    execution.start.return_value = stream
    execution.inspect = AsyncMock(return_value={"Running": False, "ExitCode": 7})

    async def execute(command: list[str], **_kwargs: object) -> Exec:
        if command[0] == "setsid":
            return execution
        cleanup_started.set()
        await asyncio.Event().wait()
        raise AssertionError("Cleanup must time out")

    container = MagicMock(spec=DockerContainer)
    container.exec = AsyncMock(side_effect=execute)
    sandbox = DockerSandbox(
        container,
        _ContainerInfo.model_validate(
            {"Id": "test", "Created": "2026-01-01T00:00:00Z", "State": {"Status": "running"}, "Config": {"Labels": {}}}
        ),
    )
    task = asyncio.create_task(sandbox.exec("command", timeout=0.005 if outcome == "timeout" else None))
    if outcome.startswith("cancel"):
        await (cleanup_started if outcome == "cancel_during_cleanup" else command_started).wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
    else:
        assert (await task).exit_code == (124 if outcome == "timeout" else 7)
    assert any(record.exc_info and isinstance(record.exc_info[1], TimeoutError) for record in caplog.records)
