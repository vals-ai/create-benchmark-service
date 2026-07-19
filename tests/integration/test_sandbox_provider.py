"""Live integration tests for the shared sandbox provider contract.

Run: AWS_PROFILE=vals uv run pytest tests/integration/test_sandbox_provider.py
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from uuid import uuid4

import pytest

from benchmark_service import (
    ImageSource,
    Resources,
    Sandbox,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)

_POLL_INTERVAL_SECONDS = 2
_POLL_TIMEOUT_SECONDS = 120
_TEST_IMAGE = "python:3.12-slim"
_TEST_RESOURCES = Resources(vcpu=1, memory=2, disk=5)


async def _wait_until_listed(provider: SandboxProvider, query: SandboxQuery, sandbox_id: str) -> None:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        listed_ids = [sandbox.id async for sandbox in provider.list_sandboxes(query)]
        if sandbox_id in listed_ids:
            return
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(f"Sandbox {sandbox_id} was not listed within {_POLL_TIMEOUT_SECONDS} seconds.")


async def _wait_until_not_found(provider: SandboxProvider, sandbox_id: str) -> None:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            await provider.get_sandbox(sandbox_id)
        except SandboxNotFoundError:
            return
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(f"Sandbox {sandbox_id} was still found after {_POLL_TIMEOUT_SECONDS} seconds.")


async def _consume_command(sandbox: Sandbox) -> str:
    return "".join([chunk async for chunk in sandbox.command("echo after-delete")])


async def _consume_download(sandbox: Sandbox, remote_path: str) -> bytes:
    return b"".join([chunk async for chunk in sandbox.stream_download(remote_path)])


class TestSandboxProviderIntegration:
    """Run the same live contract checks against every configured provider."""

    async def test_provider_contract(self, sandbox_provider: SandboxProvider) -> None:
        """Verify each provider implements the shared sandbox lifecycle and operation contract.

        Test cases:
        - Create, get, list, exec, command, file, and streaming download operations work.
        - Deleted sandboxes disappear and stale handles consistently raise SandboxNotFoundError.
        """
        sandbox_name = f"cbs-provider-test-{uuid4().hex[:10]}"
        labels = {"ProviderIntegrationTest": sandbox_name}
        query = SandboxQuery(labels=labels)
        remote_path = "/tmp/provider-integration.bin"
        payload = b"provider-integration-content"
        request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=sandbox_name,
            labels=labels,
            env_vars={},
            auto_stop_interval=10,
            create_timeout=360,
        )

        sandbox = await sandbox_provider.create_sandbox(request)
        try:
            fetched = await sandbox_provider.get_sandbox(sandbox.id)
            assert fetched.id == sandbox.id

            await _wait_until_listed(sandbox_provider, query, sandbox.id)

            exec_result = await sandbox.exec("printf exec-ready", cwd="/tmp")
            assert exec_result.exit_code == 0
            assert exec_result.stdout == "exec-ready"

            command_output = "".join(
                [
                    chunk
                    async for chunk in sandbox.command(
                        'printf "$PROVIDER_TEST_VALUE"',
                        env_vars={"PROVIDER_TEST_VALUE": "command-ready"},
                    )
                ]
            )
            assert "command-ready" in command_output

            await sandbox.upload_file(remote_path, payload)
            assert await sandbox.download_file(remote_path) == payload
            assert await _consume_download(sandbox, remote_path) == payload

        finally:
            await sandbox_provider.delete_sandbox(sandbox.id)

        await _wait_until_not_found(sandbox_provider, sandbox.id)

        listed_after_delete = [listed.id async for listed in sandbox_provider.list_sandboxes(query)]
        assert sandbox.id not in listed_after_delete

        operations: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("provider.get_sandbox", lambda: sandbox_provider.get_sandbox(sandbox.id)),
            ("sandbox.exec", lambda: sandbox.exec("echo after-delete")),
            ("sandbox.command", lambda: _consume_command(sandbox)),
            ("sandbox.upload_file", lambda: sandbox.upload_file(remote_path, payload)),
            ("sandbox.download_file", lambda: sandbox.download_file(remote_path)),
            ("sandbox.stream_download", lambda: _consume_download(sandbox, remote_path)),
        ]

        for operation_name, operation in operations:
            try:
                await operation()
            except SandboxNotFoundError:
                continue
            except Exception as exc:
                pytest.fail(f"{operation_name} raised {type(exc).__name__} instead of SandboxNotFoundError: {exc}")
            else:
                pytest.fail(f"{operation_name} did not raise SandboxNotFoundError")
