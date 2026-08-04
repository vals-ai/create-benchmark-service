"""Live integration tests for the shared sandbox provider contract.

Run: uv run pytest tests/integration/test_sandbox_provider.py

Requires AWS_PROFILE, TEST_AWS_REGION, TEST_DAYTONA_SECRET_NAME, and TEST_MODAL_SECRET_NAME.
"""

import asyncio
import shlex
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import cast
from uuid import uuid4

import pytest
from daytona import CreateSecretParams

from benchmark_service import (
    ImageSource,
    Resources,
    Sandbox,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    VolumeMount,
)
from benchmark_service.sandbox.daytona import DaytonaSandboxProvider

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

    @pytest.mark.parametrize("provider_type", ["daytona"], indirect=True)
    async def test_daytona_inventory_metadata_and_filtering(
        self,
        sandbox_provider: SandboxProvider,
    ) -> None:
        """Verify Daytona inventory metadata and inclusive cutoff filtering."""
        sandbox_name = f"cbs-daytona-inventory-{uuid4().hex[:10]}"
        labels = {"ProviderInventoryTest": sandbox_name}
        query = SandboxQuery(labels=labels)
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
            listed = await _wait_until_listed(sandbox_provider, query, sandbox.id)

            created_labels = sandbox.labels
            created_at = sandbox.created_at
            assert created_labels is not None
            assert all(created_labels.get(key) == value for key, value in labels.items())
            assert created_at is not None
            assert created_at.utcoffset() == timedelta(0)
            assert fetched.labels == listed.labels == created_labels
            assert fetched.created_at == listed.created_at == created_at

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
        finally:
            await asyncio.wait_for(
                sandbox_provider.delete_sandbox(sandbox.id),
                timeout=_POLL_TIMEOUT_SECONDS,
            )

    @pytest.mark.parametrize("provider_type", ["daytona"], indirect=True)
    async def test_daytona_native_secret_and_volume_persistence(
        self,
        sandbox_provider: SandboxProvider,
    ) -> None:
        provider = cast(DaytonaSandboxProvider, sandbox_provider)
        daytona = provider._daytona  # pyright: ignore[reportPrivateUsage]
        probe_id = uuid4().hex[:10]
        secret_name = f"cbs-secret-{probe_id}"
        # Shared long-lived volume: Daytona rejects deleting volumes that have
        # not settled to ready/error, so runs isolate by subpath instead of
        # provisioning ad-hoc volumes that cleanup would race against.
        volume_name = "cbs-provider-integration"
        run_id = f"cbs-run-{probe_id}"
        labels = {"ProviderIntegrationTest": probe_id, "run-id": run_id}
        writer: Sandbox | None = None
        reader: Sandbox | None = None
        secret_id: str | None = None

        try:
            await daytona.volume.get(volume_name, create=True)
            secret = await daytona.secret.create(
                CreateSecretParams(
                    name=secret_name,
                    value=uuid4().hex,
                    description="Ephemeral CBS provider integration secret",
                    hosts=["example.com"],
                )
            )
            secret_id = secret.id
            mount = VolumeMount(
                name=volume_name,
                mount_path="/workspace/artifacts",
                subpath="runs/{run_id}",
            )
            writer = await provider.create_sandbox(
                SandboxCreateRequest(
                    source=ImageSource(image=_TEST_IMAGE),
                    resources=_TEST_RESOURCES,
                    name=f"cbs-secret-writer-{probe_id}",
                    labels=labels,
                    env_vars={},
                    sandbox_secrets={"CBS_SECRET_TEST": secret_name},
                    volumes=[mount],
                    auto_stop_interval=10,
                    create_timeout=360,
                )
            )

            secret_check = await writer.exec(
                f'test -n "$CBS_SECRET_TEST" && test "$CBS_SECRET_TEST" != {shlex.quote(secret_name)}'
            )
            assert secret_check.exit_code == 0
            write_result = await writer.exec("printf provider-volume-persistence > /workspace/artifacts/proof.txt")
            assert write_result.exit_code == 0
            await provider.delete_sandbox(writer.id)
            writer = None

            reader = await provider.create_sandbox(
                SandboxCreateRequest(
                    source=ImageSource(image=_TEST_IMAGE),
                    resources=_TEST_RESOURCES,
                    name=f"cbs-volume-reader-{probe_id}",
                    labels=labels,
                    env_vars={},
                    volumes=[mount],
                    auto_stop_interval=10,
                    create_timeout=360,
                )
            )

            read_result = await reader.exec("cat /workspace/artifacts/proof.txt")
            assert read_result.stdout == "provider-volume-persistence"
            secret_absence = await reader.exec('test -z "${CBS_SECRET_TEST+x}"')
            assert secret_absence.exit_code == 0
        finally:
            if reader is not None:
                await provider.delete_sandbox(reader.id)
            if writer is not None:
                await provider.delete_sandbox(writer.id)
            if secret_id is not None:
                await daytona.secret.delete(secret_id)

    async def test_provider_contract(self, sandbox_provider: SandboxProvider) -> None:
        """Verify each provider implements the shared sandbox lifecycle and operation contract.

        Test cases:
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

            await _wait_until_listed(sandbox_provider, query, sandbox.id)

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
