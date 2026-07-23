"""Opt-in live contract test for an already deployed private control service.

Run: TEST_KUBERNETES_CONTROL_URL=... TEST_KUBERNETES_CONTROL_TOKEN=... TEST_KUBERNETES_IMAGE=... \
  uv run pytest tests/integration/test_kubernetes_control_service.py -q
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import suppress

import pytest

from benchmark_service.sandbox.compose import ComposeSandbox
from benchmark_service.sandbox.kubernetes.client import KubernetesControlClientDriver
from benchmark_service.sandbox.types import (
    ComposeSource,
    ImageSource,
    Resources,
    Sandbox,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxQuery,
)

_REQUIRED_ENV = (
    "TEST_KUBERNETES_CONTROL_URL",
    "TEST_KUBERNETES_CONTROL_TOKEN",
    "TEST_KUBERNETES_IMAGE",
)
_COMPOSE_ENV = "TEST_KUBERNETES_COMPOSE_IMAGE"


def _driver() -> KubernetesControlClientDriver:
    return KubernetesControlClientDriver(
        api_url=os.environ["TEST_KUBERNETES_CONTROL_URL"],
        api_token=os.environ["TEST_KUBERNETES_CONTROL_TOKEN"],
        request_timeout=360,
    )


def _request(name: str, image: str) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=ImageSource(image=image),
        resources=Resources(vcpu=1, memory=1, disk=5),
        name=name,
        labels={"contract_test": name},
        env_vars={},
        auto_stop_interval=10,
        create_timeout=300,
    )


def _connection_command(host: str) -> str:
    return f"python -c \"import socket; connection = socket.create_connection(('{host}', 443), 2); connection.close()\""


async def _wait_for_command_state(
    sandbox: Sandbox,
    command: str,
    *,
    should_succeed: bool,
    timeout: float = 15,
) -> None:
    deadline = time.monotonic() + timeout
    last_exit_code: int | None = None
    while time.monotonic() < deadline:
        result = await sandbox.exec(command, timeout=5)
        last_exit_code = result.exit_code
        if (result.exit_code == 0) is should_succeed:
            return
        await asyncio.sleep(0.25)
    pytest.fail(f"Command did not reach the expected state; last exit code was {last_exit_code}")


async def _delete_created_sandboxes(
    driver: KubernetesControlClientDriver,
    sandbox_ids: set[str],
) -> None:
    cleanup_errors: list[Exception] = []
    for sandbox_id in sandbox_ids:
        try:
            await driver.delete_sandbox(sandbox_id)
        except Exception as error:
            cleanup_errors.append(error)
    try:
        await driver.close()
    except Exception as error:
        cleanup_errors.append(error)
    if cleanup_errors:
        raise ExceptionGroup("Could not clean up live Kubernetes sandboxes", cleanup_errors)


@pytest.mark.skipif(
    any(not os.environ.get(name) for name in _REQUIRED_ENV),
    reason="private Kubernetes control-service variables are not configured",
)
async def test_live_kubernetes_control_service_contract() -> None:
    """Prove the required provider contract against the disposable cluster.

    Test cases:
    - Repeated creation is idempotent and lifecycle queries find the unique sandbox.
    - HTTP stream output arrives before completion, cancellation stops its process, and timeout uses exit code 124.
    - Large binary downloads stream in chunks and temporary egress rules can be cleared.
    """
    sandbox_name = f"contract-{uuid.uuid4().hex[:12]}"
    driver = _driver()
    sandbox_ids: set[str] = set()
    sandbox: Sandbox | None = None
    command_stream: AsyncGenerator[str, None] | None = None
    try:
        request = _request(sandbox_name, os.environ["TEST_KUBERNETES_IMAGE"])
        sandbox = await driver.create_sandbox(request)
        sandbox_ids.add(sandbox.id)
        repeated = await driver.create_sandbox(request)
        sandbox_ids.add(repeated.id)

        fetched = await driver.get_sandbox(sandbox.id)
        listed = [
            listed_sandbox
            async for listed_sandbox in driver.list_sandboxes(
                SandboxQuery(labels={"contract_test": sandbox_name}, page_size=2)
            )
        ]

        assert repeated.id == sandbox.id
        assert fetched.id == sandbox.id
        assert [listed_sandbox.id for listed_sandbox in listed] == [sandbox.id]

        stream_started_path = f"/workspace/{sandbox_name}-stream-started"
        stream_finished_path = f"/workspace/{sandbox_name}-stream-finished"
        command_stream = sandbox.command(
            f"rm -f {stream_finished_path}; "
            f"touch {stream_started_path}; "
            "printf 'stream-ready:%s\\n' \"$$\"; "
            f"sleep 60; touch {stream_finished_path}; printf 'stream-finished\\n'"
        )
        first_chunk = await asyncio.wait_for(anext(command_stream), timeout=10)
        streamed_prefix = first_chunk
        while "\n" not in streamed_prefix:
            streamed_prefix += await asyncio.wait_for(anext(command_stream), timeout=10)
        process_line, unconsumed_output = streamed_prefix.split("\n", 1)
        process_id = int(process_line.removeprefix("stream-ready:"))

        assert unconsumed_output == ""

        await _wait_for_command_state(
            sandbox,
            f"test -e {stream_started_path} && test ! -e {stream_finished_path} && kill -0 {process_id}",
            should_succeed=True,
        )
        await asyncio.wait_for(command_stream.aclose(), timeout=10)
        command_stream = None
        await _wait_for_command_state(
            sandbox,
            f"! kill -0 {process_id} 2>/dev/null",
            should_succeed=True,
        )

        with pytest.raises(SandboxCommandError) as timeout_error:
            _ = [chunk async for chunk in sandbox.command("sleep 30", timeout=1)]

        assert timeout_error.value.exit_code == 124

        content = bytes(range(256)) * 8193
        remote_path = f"/workspace/{sandbox_name}-contract.bin"
        await sandbox.upload_file(remote_path, content)
        download_stream = sandbox.stream_download(remote_path)
        try:
            first_download_chunk = await asyncio.wait_for(anext(download_stream), timeout=30)

            assert 0 < len(first_download_chunk) < len(content)

            remaining_download = b"".join([chunk async for chunk in download_stream])
        finally:
            await download_stream.aclose()

        assert first_download_chunk + remaining_download == content

        await sandbox.modify_egress_rules(["example.com"])
        try:
            await _wait_for_command_state(
                sandbox,
                _connection_command("example.org"),
                should_succeed=False,
            )
            await _wait_for_command_state(
                sandbox,
                _connection_command("example.com"),
                should_succeed=True,
            )
        finally:
            await sandbox.clear_egress_rules()

        for host in ("example.com", "example.org"):
            await _wait_for_command_state(
                sandbox,
                _connection_command(host),
                should_succeed=True,
            )
    finally:
        try:
            if command_stream is not None:
                await command_stream.aclose()
        finally:
            await _delete_created_sandboxes(driver, sandbox_ids)


@pytest.mark.skipif(
    any(not os.environ.get(name) for name in (*_REQUIRED_ENV, _COMPOSE_ENV)),
    reason="private Kubernetes Compose contract variables are not configured",
)
async def test_live_kubernetes_compose_contract() -> None:
    """Prove the optional ComposeSandbox flow through the Docker sidecar.

    Test cases:
    - An uploaded Compose file starts a main service from the requested image.
    - ComposeSandbox streams a command and teardown runs before outer sandbox deletion.
    """
    sandbox_name = f"compose-contract-{uuid.uuid4().hex[:12]}"
    compose_path = f"/workspace/{sandbox_name}.yaml"
    compose_command = f"docker compose -p {sandbox_name} -f {compose_path}"
    driver = _driver()
    sandbox_ids: set[str] = set()
    outer: Sandbox | None = None
    try:
        compose_outer_image = os.environ[_COMPOSE_ENV]
        outer = await driver.create_sandbox(_request(sandbox_name, compose_outer_image))
        sandbox_ids.add(outer.id)
        compose_content = (
            "services:\n"
            "  main:\n"
            f"    image: {os.environ['TEST_KUBERNETES_IMAGE']}\n"
            '    command: ["sh", "-lc", "trap : TERM INT; while :; do sleep 3600; done"]\n'
        ).encode()
        await outer.upload_file(compose_path, compose_content)

        start_result = await outer.exec(f"{compose_command} up -d --wait", timeout=180)

        assert start_result.exit_code == 0, start_result.output

        compose = ComposeSandbox(
            outer,
            ComposeSource(
                outer=ImageSource(image=compose_outer_image),
                service="main",
                compose_command=compose_command,
            ),
        )
        command_chunks = [
            chunk
            async for chunk in compose.command(
                "printf 'compose-first\\n'; sleep 1; printf 'compose-second\\n'",
                timeout=15,
            )
        ]

        assert "".join(command_chunks) == "compose-first\ncompose-second\n"
        assert len(command_chunks) >= 2
    except BaseException:
        if outer is not None:
            with suppress(Exception):
                await outer.exec(
                    f"{compose_command} down --volumes --remove-orphans",
                    timeout=180,
                )
        raise
    else:
        assert outer is not None
        teardown_result = await outer.exec(
            f"{compose_command} down --volumes --remove-orphans",
            timeout=180,
        )

        assert teardown_result.exit_code == 0, teardown_result.output
    finally:
        await _delete_created_sandboxes(driver, sandbox_ids)
