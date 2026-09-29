"""Docker configuration and command cleanup checks; run pytest tests/test_docker_config.py."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiodocker.containers import DockerContainer
from aiodocker.execs import Exec
from aiodocker.stream import Stream

from benchmark_service import DockerProviderConfig
from aiodocker.exceptions import DockerError

from benchmark_service import ImageSource, Resources, SandboxError
from benchmark_service.sandbox.local.docker import (  # pyright: ignore[reportPrivateUsage]
    _MANAGED_LABEL,  # pyright: ignore[reportPrivateUsage]
    DockerSandbox,
    DockerSandboxProvider,
    _ContainerInfo,  # pyright: ignore[reportPrivateUsage]
)
from benchmark_service.sandbox.types import ExecResult, SandboxCreateRequest
from templates.vals_ai.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]


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


def _container_info(labels: dict[str, str] | None = None) -> _ContainerInfo:
    return _ContainerInfo.model_validate(
        {
            "Id": "container-1",
            "Created": "2026-01-01T00:00:00Z",
            "State": {"Status": "running"},
            "Config": {"Labels": labels or {}},
        }
    )


async def test_relative_uploads_resolve_from_the_working_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    """Write a relative upload under the working directory, where commands and downloads resolve it."""
    container = MagicMock(spec=DockerContainer)
    container.put_archive = AsyncMock()
    sandbox = DockerSandbox(container, _container_info())
    commands: list[str] = []

    async def execute(command: str, **_kwargs: object) -> ExecResult:
        commands.append(command)
        return ExecResult(exit_code=0, output="/work\n" if command == "pwd" else "")

    monkeypatch.setattr(sandbox, "exec", execute)

    await sandbox.upload_file("dir/payload.bin", b"data")

    assert commands == ["pwd", "mkdir -p /work/dir"]
    assert container.put_archive.await_args is not None
    assert container.put_archive.await_args.args[0] == "/work/dir"


async def test_timed_out_creation_deletes_a_container_docker_finishes_later() -> None:
    """Delete a container that Docker finishes creating after the caller's deadline instead of leaking it."""
    release = asyncio.Event()
    created = asyncio.Event()
    background: list[asyncio.Task[None]] = []
    deleted: list[bool] = []

    async def daemon_creates() -> None:
        await release.wait()
        created.set()

    async def run(_config: object, *, name: str) -> MagicMock:
        # The daemon keeps creating even if the client request is cancelled.
        background.append(asyncio.ensure_future(daemon_creates()))
        await asyncio.shield(background[-1])
        return MagicMock(id="container-1")

    async def show() -> dict[str, object]:
        if not created.is_set():
            raise DockerError(404, "No such container")
        return {
            "Id": "container-1",
            "Created": "2026-01-01T00:00:00Z",
            "State": {"Status": "created"},
            "Config": {"Labels": {_MANAGED_LABEL: "true"}},
        }

    async def delete(**_kwargs: object) -> None:
        deleted.append(True)

    container = MagicMock(spec=DockerContainer)
    container.show = AsyncMock(side_effect=show)
    container.delete = AsyncMock(side_effect=delete)
    provider = DockerSandboxProvider.__new__(DockerSandboxProvider)
    docker = MagicMock()
    docker.images.inspect = AsyncMock()
    docker.containers.run = run
    docker.containers.container = MagicMock(return_value=container)
    provider._docker = docker  # pyright: ignore[reportPrivateUsage]
    request = SandboxCreateRequest(
        name="slow-create",
        source=ImageSource(image="python:3.12-slim"),
        resources=Resources(vcpu=1, memory=1, disk=5),
        labels={},
        env_vars={},
        auto_stop_interval=5,
        create_timeout=1,
    )

    creation = asyncio.create_task(provider.create_sandbox(request))
    await asyncio.sleep(1.2)
    release.set()
    with pytest.raises(SandboxError, match="timed out"):
        await creation

    assert deleted == [True]


async def test_timed_out_creation_logs_a_late_creation_failure(caplog: pytest.LogCaptureFixture) -> None:
    """Log a create that fails after the caller's deadline instead of leaving an unretrieved task error."""

    async def run(_config: object, *, name: str) -> MagicMock:
        await asyncio.sleep(1.2)
        raise DockerError(500, "image pull failed")

    container = MagicMock(spec=DockerContainer)
    container.show = AsyncMock(side_effect=DockerError(404, "No such container"))
    provider = DockerSandboxProvider.__new__(DockerSandboxProvider)
    docker = MagicMock()
    docker.images.inspect = AsyncMock()
    docker.containers.run = run
    docker.containers.container = MagicMock(return_value=container)
    provider._docker = docker  # pyright: ignore[reportPrivateUsage]
    request = SandboxCreateRequest(
        name="failing-create",
        source=ImageSource(image="python:3.12-slim"),
        resources=Resources(vcpu=1, memory=1, disk=5),
        labels={},
        env_vars={},
        auto_stop_interval=5,
        create_timeout=1,
    )

    with pytest.raises(SandboxError, match="timed out"):
        await provider.create_sandbox(request)

    assert any(
        record.message == "Docker sandbox creation failed after its deadline"
        and record.exc_info
        and isinstance(record.exc_info[1], DockerError)
        for record in caplog.records
    )
    assert not any("never retrieved" in record.getMessage() for record in caplog.records)


async def test_missing_image_is_pulled_by_the_docker_cli_outside_the_creation_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pull a missing image with the docker CLI before creation starts, so a slow pull does not use up create_timeout."""
    image = "registry.example.com/team/agent:1"
    docker_cli = tmp_path / "docker"
    docker_cli.write_text(f'#!/bin/sh\nsleep 1.2\necho "$@" > {tmp_path / "pull-args"}\n')
    docker_cli.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")

    async def run(_config: object, *, name: str) -> MagicMock:
        return MagicMock(id="container-1")

    container = MagicMock(spec=DockerContainer)
    container.show = AsyncMock(
        return_value={
            "Id": "container-1",
            "Created": "2026-01-01T00:00:00Z",
            "State": {"Status": "running"},
            "Config": {"Labels": {_MANAGED_LABEL: "true"}},
        }
    )
    provider = DockerSandboxProvider.__new__(DockerSandboxProvider)
    docker = MagicMock()
    docker.images.inspect = AsyncMock(side_effect=DockerError(404, "No such image"))
    docker.containers.run = run
    docker.containers.container = MagicMock(return_value=container)
    provider._docker = docker  # pyright: ignore[reportPrivateUsage]
    request = SandboxCreateRequest(
        name="missing-image",
        source=ImageSource(image=image),
        resources=Resources(vcpu=1, memory=1, disk=5),
        labels={},
        env_vars={},
        auto_stop_interval=5,
        create_timeout=1,
    )

    sandbox = await provider.create_sandbox(request)

    assert sandbox.id == "container-1"
    assert (tmp_path / "pull-args").read_text().split() == ["pull", "--quiet", image]
