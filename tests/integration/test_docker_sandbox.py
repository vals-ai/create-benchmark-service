"""Local Docker contract checks; run with CBS_DOCKER_ENABLED=true and DOCKER_HOST."""

import asyncio
import os
from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from aiodocker import Docker

from benchmark_service import (
    DockerProviderConfig,
    ImageSource,
    Resources,
    Sandbox,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)


@pytest.fixture
def docker_labels() -> dict[str, str]:
    """Identify only containers belonging to this test."""
    return {"test": f"docker-contract-{uuid4().hex}"}


@pytest.fixture
async def docker_provider(docker_labels: dict[str, str]) -> AsyncGenerator[SandboxProvider]:
    """Clean up only this test's containers on the configured daemon."""
    provider = DockerProviderConfig().create_provider()
    try:
        yield provider
    finally:
        async for sandbox in provider.list_sandboxes(SandboxQuery(labels=docker_labels)):
            await provider.delete_sandbox(sandbox.id)
        await provider.close()


@pytest.fixture
async def docker_sandbox(docker_provider: SandboxProvider, docker_labels: dict[str, str]) -> Sandbox:
    """Create a small image-based sandbox for each check."""
    return await docker_provider.create_sandbox(
        SandboxCreateRequest(
            source=ImageSource(image="python:3.12-slim"),
            resources=Resources(vcpu=1, memory=1, disk=1),
            name="docker-contract",
            labels=docker_labels,
            env_vars={},
            auto_stop_interval=10,
            create_timeout=120,
            network_block_all=True,
        )
    )


async def test_docker_commands_and_binary_files(docker_sandbox: Sandbox) -> None:
    """Preserve streamed text, exit status, command environment, and binary files."""
    output = "".join(
        [
            part
            async for part in docker_sandbox.command(
                'printf "%s" "$TEST_VALUE"; printf error >&2',
                env_vars={"TEST_VALUE": "hello"},
            )
        ]
    )
    assert output == "helloerror"
    result = await docker_sandbox.exec("printf failed; exit 7")
    assert result.exit_code == 7
    assert result.output == "failed"
    result = await docker_sandbox.exec("printf '\\342'; exit 7")
    assert result.exit_code == 7
    assert result.output == "\ufffd"
    for command in ("", "# comment only", "printf comment # trailing comment"):
        result = await docker_sandbox.exec(command)
        assert result.exit_code == 0
        assert result.output == ("comment" if command.startswith("printf") else "")
    data = bytes(range(256)) * 4096
    await docker_sandbox.upload_file("/tmp/a directory/payload.bin", data)
    assert await docker_sandbox.download_file("/tmp/a directory/payload.bin") == data
    assert (await docker_sandbox.exec("pwd", cwd="/tmp/a directory")).output.strip() == "/tmp/a directory"
    assert (await docker_sandbox.exec("test ! -S /var/run/docker.sock")).exit_code == 0
    await docker_sandbox.upload_file("/tmp/nonroot-readable", b"payload")
    result = await docker_sandbox.exec("su nobody -s /bin/sh -c 'cat /tmp/nonroot-readable'")
    assert result.exit_code == 0
    assert result.output == "payload"


async def test_docker_timeout_and_cancel_kill_commands(docker_sandbox: Sandbox) -> None:
    """Stop command process groups on timeout and caller cancellation."""
    result = await docker_sandbox.exec("sleep 2; touch /tmp/timed-out", timeout=0.2)
    assert result.exit_code == 124
    result = await docker_sandbox.exec("(sleep 2; touch /tmp/background-timeout) & echo ready", timeout=0.2)
    assert result.exit_code == 124
    ready = asyncio.Event()

    async def consume() -> None:
        async for chunk in docker_sandbox.command("echo ready; sleep 2; touch /tmp/cancelled"):
            if "ready" in chunk:
                ready.set()

    task = asyncio.create_task(consume())
    await asyncio.wait_for(ready.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(2.2)
    result = await docker_sandbox.exec(
        "test ! -e /tmp/timed-out && test ! -e /tmp/cancelled && test ! -e /tmp/background-timeout"
    )
    assert result.exit_code == 0


async def test_docker_timeout_bounds_exec_creation(docker_sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch) -> None:
    """Honor the command timeout while Docker is creating the exec instance."""
    from aiodocker.containers import DockerContainer

    async def stalled_exec(*_args: object, **_kwargs: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(DockerContainer, "exec", stalled_exec)
    result = await asyncio.wait_for(docker_sandbox.exec("true", timeout=0.01), timeout=1)
    assert result.exit_code == 124


async def test_docker_successful_command_preserves_background_process(docker_sandbox: Sandbox) -> None:
    """Leave intentionally detached processes running after a successful command."""
    result = await docker_sandbox.exec("(sleep 1; touch /tmp/background-success) >/dev/null 2>&1 &")
    assert result.exit_code == 0
    await asyncio.sleep(1.2)
    assert (await docker_sandbox.exec("test -e /tmp/background-success")).exit_code == 0


async def test_docker_inventory_excludes_unmanaged_containers(
    docker_provider: SandboxProvider,
    docker_sandbox: Sandbox,
    docker_labels: dict[str, str],
) -> None:
    """Filter inventory and reject deletion of unrelated Docker containers."""
    async with Docker(url=os.environ.get("DOCKER_HOST")) as docker:
        unrelated = await docker.containers.create({"Image": "python:3.12-slim"})
        try:
            listed = [s async for s in docker_provider.list_sandboxes(SandboxQuery(labels=docker_labels))]
            assert [s.id for s in listed] == [docker_sandbox.id]
            assert listed[0].created_at is not None
            assert unrelated.id not in [s.id async for s in docker_provider.list_sandboxes(SandboxQuery(labels={}))]
            with pytest.raises(SandboxNotFoundError):
                await docker_provider.get_sandbox(unrelated.id)
            with pytest.raises(SandboxNotFoundError):
                await docker_provider.delete_sandbox(unrelated.id)
            assert (await unrelated.show())["Id"] == unrelated.id  # pyright: ignore[reportUnknownMemberType]
        finally:
            await unrelated.delete(force=True, v=True)
    await docker_provider.delete_sandbox(docker_sandbox.id)
    with pytest.raises(SandboxNotFoundError):
        await docker_provider.get_sandbox(docker_sandbox.id)


async def test_docker_closing_stream_terminates_command(docker_sandbox: Sandbox) -> None:
    """Stop the command when a consumer closes its generator after partial output."""
    stream = docker_sandbox.command("echo ready; sleep 1; touch /tmp/closed-stream")
    assert "ready" in await anext(stream)
    await stream.aclose()
    await asyncio.sleep(1.2)
    assert (await docker_sandbox.exec("test ! -e /tmp/closed-stream")).exit_code == 0


async def test_docker_failed_start_removes_container(
    docker_provider: SandboxProvider,
    monkeypatch: pytest.MonkeyPatch,
    docker_labels: dict[str, str],
) -> None:
    """Remove the created container even when starting it fails."""
    from aiodocker.containers import DockerContainer
    from aiodocker.exceptions import DockerError
    from benchmark_service import SandboxError

    async def fail_start(_container: DockerContainer) -> None:
        raise DockerError(500, "injected start failure")

    monkeypatch.setattr(DockerContainer, "start", fail_start)
    with pytest.raises(SandboxError, match="injected start failure"):
        await docker_provider.create_sandbox(
            SandboxCreateRequest(
                source=ImageSource(image="python:3.12-slim"),
                resources=Resources(vcpu=1, memory=1, disk=1),
                name="failed-start",
                labels=docker_labels,
                env_vars={},
                auto_stop_interval=0,
                create_timeout=30,
            )
        )
    assert [s async for s in docker_provider.list_sandboxes(SandboxQuery(labels=docker_labels))] == []
