"""Live integration tests for the shared sandbox provider contract.

Run: uv run pytest tests/integration/test_sandbox_provider.py

Requires AWS_PROFILE, TEST_AWS_REGION, TEST_DAYTONA_SECRET_NAME, and TEST_MODAL_SECRET_NAME.
"""

import asyncio
import shlex
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from uuid import uuid4

import pytest

from benchmark_service import (
    ImageSource,
    Resources,
    Sandbox,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)

_POLL_INTERVAL_SECONDS = 2
_POLL_TIMEOUT_SECONDS = 120
_TEST_IMAGE = "python:3.12-slim"
_TEST_RESOURCES = Resources(vcpu=1, memory=2, disk=5)


def _python_command(script: str) -> str:
    return f"python -c {shlex.quote(script)}"


_MEMORY_FAULT_COMMAND = _python_command(
    "import resource\n"
    "limit = 256 * 1024 * 1024\n"
    "resource.setrlimit(resource.RLIMIT_AS, (limit, limit))\n"
    "try:\n"
    "    bytearray(1024 * 1024 * 1024)\n"
    "except MemoryError:\n"
    "    raise SystemExit(42)\n"
    "raise SystemExit(1)\n"
)
_STORAGE_FAULT_COMMAND = _python_command(
    "import errno\n"
    "try:\n"
    '    with open("/dev/full", "wb", buffering=0) as full_device:\n'
    '        full_device.write(b"provider-storage-fault")\n'
    "except OSError as exc:\n"
    "    raise SystemExit(43 if exc.errno == errno.ENOSPC else 1)\n"
    "raise SystemExit(1)\n"
)


def _egress_probe_command(allowed_url: str, blocked_url: str | None = None) -> str:
    script = (
        "import sys\n"
        "import urllib.error\n"
        "import urllib.request\n"
        "\n"
        "def can_reach(url):\n"
        '    request = urllib.request.Request(url, headers={"User-Agent": "cbs-provider-test"}, method="HEAD")\n'
        "    try:\n"
        "        with urllib.request.urlopen(request, timeout=10):\n"
        "            return True\n"
        "    except urllib.error.HTTPError:\n"
        "        return True\n"
        "    except (urllib.error.URLError, TimeoutError, OSError):\n"
        "        return False\n"
        "\n"
        "allowed = can_reach(sys.argv[1])\n"
        "blocked = can_reach(sys.argv[2]) if len(sys.argv) > 2 else False\n"
        'print(f"allowed={allowed} blocked={blocked}", flush=True)\n'
        "raise SystemExit(0 if allowed and not blocked else 1)\n"
    )
    arguments = [allowed_url]
    if blocked_url is not None:
        arguments.append(blocked_url)

    return shlex.join(["python", "-c", script, *arguments])


async def _wait_until_listed(provider: SandboxProvider, query: SandboxQuery, sandbox_id: str) -> Sandbox:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        matches = [sandbox async for sandbox in provider.list_sandboxes(query) if sandbox.id == sandbox_id]
        if len(matches) == 1:
            return matches[0]
        if matches:
            pytest.fail(f"Sandbox {sandbox_id} was listed more than once.")
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


async def _wait_until_not_listed(provider: SandboxProvider, query: SandboxQuery, sandbox_id: str) -> None:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        listed_ids = [sandbox.id async for sandbox in provider.list_sandboxes(query)]
        if sandbox_id not in listed_ids:
            return
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(f"Sandbox {sandbox_id} was still listed after {_POLL_TIMEOUT_SECONDS} seconds.")


async def _consume_command(sandbox: Sandbox) -> str:
    return "".join([chunk async for chunk in sandbox.command("echo after-delete")])


async def _consume_download(sandbox: Sandbox, remote_path: str) -> bytes:
    return b"".join([chunk async for chunk in sandbox.stream_download(remote_path)])


async def _read_stream_until_ready(sandbox: Sandbox, ready: asyncio.Event) -> None:
    async for chunk in sandbox.command("echo stream-ready && sleep 300"):
        if "stream-ready" in chunk:
            ready.set()


class TestSandboxProviderIntegration:
    """Run the same live contract checks against every configured provider."""

    async def test_provider_contract(self, provider_type: str, sandbox_provider: SandboxProvider) -> None:
        """Verify each provider implements the shared sandbox lifecycle and operation contract.

        Test cases:
        - Daytona inventory exposes metadata and applies inclusive creation-time filtering.
        - Create-time and command-time environment variables reach sandbox commands.
        - Memory, storage, timeout, and OS-kill faults preserve their command exit codes.
        - Egress rules allow the configured host, block an off-list host, and restore unrestricted access.
        - Deleting a sandbox during command streaming raises SandboxNotFoundError.
        - Create, get, list, exec, file, and streaming download operations work.
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
            env_vars={"PROVIDER_CREATE_VALUE": "create-ready"},
            auto_stop_interval=10,
            create_timeout=360,
        )

        sandbox = await sandbox_provider.create_sandbox(request)
        stream_task: asyncio.Task[None] | None = None
        try:
            fetched = await sandbox_provider.get_sandbox(sandbox.id)
            assert fetched.id == sandbox.id

            listed = await _wait_until_listed(sandbox_provider, query, sandbox.id)
            if provider_type == "daytona":
                listed_labels = listed.labels
                assert listed_labels is not None
                assert all(listed_labels.get(key) == value for key, value in labels.items())
                created_at = listed.created_at
                assert created_at is not None
                assert created_at.utcoffset() == timedelta(0)

                await _wait_until_listed(
                    sandbox_provider,
                    SandboxQuery(labels=labels, created_at_lte=created_at),
                    sandbox.id,
                )
                await _wait_until_not_listed(
                    sandbox_provider,
                    SandboxQuery(
                        labels=labels,
                        created_at_lte=created_at - timedelta(microseconds=1),
                    ),
                    sandbox.id,
                )

            exec_result = await sandbox.exec('printf "$PROVIDER_CREATE_VALUE"', cwd="/tmp")
            assert exec_result.exit_code == 0
            assert exec_result.stdout == "create-ready"

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

            fault_cases: list[tuple[str, str, float | None, int]] = [
                ("memory", _MEMORY_FAULT_COMMAND, None, 42),
                ("storage", _STORAGE_FAULT_COMMAND, None, 43),
                ("timeout", "sleep 30", 1, 124),
                ("os-kill", "sh -c 'kill -KILL $$'", None, 137),
            ]
            for fault_name, fault_command, fault_timeout, expected_exit_code in fault_cases:
                with pytest.raises(SandboxCommandError) as error:
                    _ = "".join([chunk async for chunk in sandbox.command(fault_command, timeout=fault_timeout)])

                assert error.value.exit_code == expected_exit_code, fault_name

            await sandbox.modify_egress_rules(["https://example.com"])
            try:
                restricted_result = await sandbox.exec(
                    _egress_probe_command("https://example.com", "https://www.python.org")
                )
                assert restricted_result.exit_code == 0
                assert "allowed=True blocked=False" in restricted_result.stdout
            finally:
                await sandbox.clear_egress_rules()

            restored_result = await sandbox.exec(_egress_probe_command("https://www.python.org"))
            assert restored_result.exit_code == 0
            assert "allowed=True" in restored_result.stdout

            stream_ready = asyncio.Event()
            stream_task = asyncio.create_task(_read_stream_until_ready(sandbox, stream_ready))
            await asyncio.wait_for(stream_ready.wait(), timeout=30)
            await sandbox_provider.delete_sandbox(sandbox.id)

            with pytest.raises(SandboxNotFoundError):
                await asyncio.wait_for(stream_task, timeout=60)

        finally:
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()
                await asyncio.gather(stream_task, return_exceptions=True)
            await sandbox_provider.delete_sandbox(sandbox.id)

        await _wait_until_not_found(sandbox_provider, sandbox.id)
        await _wait_until_not_listed(sandbox_provider, query, sandbox.id)

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
