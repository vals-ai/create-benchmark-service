"""Local Docker contract checks; run with CBS_DOCKER_ENABLED=true and DOCKER_HOST."""

import asyncio
from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest

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
async def docker_provider() -> AsyncGenerator[SandboxProvider]:
    """Use an isolated installation on the configured local daemon."""
    config = DockerProviderConfig.from_env().model_copy(update={"installation_id": f"test-{uuid4().hex}"})
    provider = config.create_provider()
    try:
        yield provider
    finally:
        async for sandbox in provider.list_sandboxes(SandboxQuery(labels={})):
            await provider.delete_sandbox(sandbox.id)
        await provider.close()


@pytest.fixture
async def docker_sandbox(docker_provider: SandboxProvider) -> Sandbox:
    """Create a small image-based sandbox for each check."""
    return await docker_provider.create_sandbox(
        SandboxCreateRequest(
            source=ImageSource(image="python:3.12-slim"),
            resources=Resources(vcpu=1, memory=1, disk=1),
            name="docker-contract",
            labels={"test": "contract"},
            env_vars={},
            auto_stop_interval=0,
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
    data = bytes(range(256)) * 4096
    await docker_sandbox.upload_file("/tmp/a directory/payload.bin", data)
    assert await docker_sandbox.download_file("/tmp/a directory/payload.bin") == data
    assert (await docker_sandbox.exec("pwd", cwd="/tmp/a directory")).output.strip() == "/tmp/a directory"
    assert (await docker_sandbox.exec("test ! -S /var/run/docker.sock")).exit_code == 0


async def test_docker_timeout_and_cancel_kill_commands(docker_sandbox: Sandbox) -> None:
    """Stop command process groups on timeout and caller cancellation."""
    result = await docker_sandbox.exec("sleep 2; touch /tmp/timed-out", timeout=0.2)
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
    assert (await docker_sandbox.exec("test ! -e /tmp/timed-out && test ! -e /tmp/cancelled")).exit_code == 0


async def test_docker_inventory_and_installation_isolation(
    docker_provider: SandboxProvider,
    docker_sandbox: Sandbox,
) -> None:
    """Filter inventory and prevent another installation from deleting containers."""
    listed = [s async for s in docker_provider.list_sandboxes(SandboxQuery(labels={"test": "contract"}))]
    assert [s.id for s in listed] == [docker_sandbox.id]
    assert listed[0].created_at is not None
    config = DockerProviderConfig.from_env().model_copy(update={"installation_id": f"other-{uuid4().hex}"})
    other = config.create_provider()
    try:
        assert [s async for s in other.list_sandboxes(SandboxQuery(labels={}))] == []
        with pytest.raises(SandboxNotFoundError):
            await other.delete_sandbox(docker_sandbox.id)
        assert (await docker_sandbox.exec("true")).exit_code == 0
    finally:
        await other.close()
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
                labels={},
                env_vars={},
                auto_stop_interval=0,
                create_timeout=30,
            )
        )
    assert [s async for s in docker_provider.list_sandboxes(SandboxQuery(labels={}))] == []
