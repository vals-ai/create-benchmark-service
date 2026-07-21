"""Tests for the experimental Kubernetes sandbox provider.

Run: uv run pytest tests/test_kubernetes_sandbox.py
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping

import pytest

from benchmark_service.sandbox.kubernetes import (
    KubernetesRuntimeDriver,
    KubernetesSandbox,
    KubernetesSandboxProvider,
)
from benchmark_service.sandbox.types import (
    ExecResult,
    ImageSource,
    Resources,
    Sandbox,
    SandboxCreateRequest,
    SandboxQuery,
)


class MockKubernetesRuntimeDriver(KubernetesRuntimeDriver):
    """Records provider requests at the runtime boundary."""

    def __init__(self) -> None:
        self.sandbox = KubernetesSandbox(
            instance_id="sandbox-1",
            name="task-1",
            state="running",
            driver=self,
        )
        self.created_request: SandboxCreateRequest | None = None
        self.requested_instance_id: str | None = None
        self.deleted_instance_id: str | None = None
        self.listed_query: SandboxQuery | None = None
        self.executed_commands: list[
            tuple[str, str, str | None, float | None]
        ] = []
        self.streamed_commands: list[
            tuple[str, str, str | None, float | None, Mapping[str, str] | None]
        ] = []
        self.uploaded_files: list[tuple[str, str, bytes]] = []
        self.downloaded_files: list[tuple[str, str]] = []
        self.streamed_files: list[tuple[str, str]] = []
        self.egress_changes: list[tuple[str, list[str]]] = []
        self.cleared_egress: list[str] = []
        self.closed = False

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        self.created_request = request

        return self.sandbox

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        self.requested_instance_id = instance_id

        return self.sandbox

    async def delete_sandbox(self, instance_id: str) -> None:
        self.deleted_instance_id = instance_id

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        self.listed_query = query
        yield self.sandbox

    async def exec(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        self.executed_commands.append((instance_id, command, cwd, timeout))

        return ExecResult(exit_code=0, output="command output")

    async def command(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        self.streamed_commands.append((instance_id, command, cwd, timeout, env_vars))
        yield "first chunk"
        yield "second chunk"

    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        content: bytes,
    ) -> None:
        self.uploaded_files.append((instance_id, remote_path, content))

    async def download_file(self, instance_id: str, remote_path: str) -> bytes:
        self.downloaded_files.append((instance_id, remote_path))

        return b"artifact"

    async def stream_download(
        self,
        instance_id: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]:
        self.streamed_files.append((instance_id, remote_path))
        yield b"first"
        yield b"second"

    async def modify_egress_rules(
        self,
        instance_id: str,
        allowed_addresses: list[str],
    ) -> None:
        self.egress_changes.append((instance_id, allowed_addresses))

    async def clear_egress_rules(self, instance_id: str) -> None:
        self.cleared_egress.append(instance_id)

    async def close(self) -> None:
        self.closed = True


def _create_request() -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=ImageSource(image="python:3.12"),
        resources=Resources(vcpu=2, memory=4, disk=20),
        name="task-1",
        labels={"run_id": "run-1"},
        env_vars={"WORKSPACE": "/workspace"},
        auto_stop_interval=30,
        create_timeout=120,
    )


class TestKubernetesSandboxProvider:
    """Kubernetes provider behavior at the runtime-driver boundary."""

    async def test_delegates_sandbox_lifecycle_to_runtime_driver(self) -> None:
        """The provider must preserve the shared sandbox contract across runtimes.

        Test cases:
        - Create, get, and list return the runtime driver's sandbox.
        - Request, query, deletion, and close reach the runtime driver unchanged.
        """
        driver = MockKubernetesRuntimeDriver()
        provider = KubernetesSandboxProvider(driver)
        request = _create_request()
        query = SandboxQuery(labels={"run_id": "run-1"})

        async with provider:
            created_sandbox = await provider.create_sandbox(request)
            fetched_sandbox = await provider.get_sandbox("sandbox-1")
            listed_sandboxes = [
                sandbox async for sandbox in provider.list_sandboxes(query)
            ]
            await provider.delete_sandbox("sandbox-1")

        assert created_sandbox is driver.sandbox
        assert fetched_sandbox is driver.sandbox
        assert listed_sandboxes == [driver.sandbox]
        assert driver.created_request is request
        assert driver.requested_instance_id == "sandbox-1"
        assert driver.listed_query is query
        assert driver.deleted_instance_id == "sandbox-1"
        assert driver.closed is True


class TestKubernetesSandbox:
    """Sandbox operations delegated through the Kubernetes runtime driver."""

    def test_requires_runtime_download_stream(self) -> None:
        """Require runtime drivers to provide genuine download streaming.

        Test cases:
        - The runtime contract marks stream_download as abstract.
        """
        assert "stream_download" in KubernetesRuntimeDriver.__abstractmethods__

    async def test_rejects_invalid_command_environment(self) -> None:
        """Reject unsafe command environments before contacting the runtime.

        Test cases:
        - Invalid shell variable names are rejected.
        - Reserved terminal variables are rejected.
        """
        driver = MockKubernetesRuntimeDriver()

        cases = [({"BAD-NAME": "x"}, "Invalid"), ({"TERM": "x"}, "Reserved")]
        for env_vars, message in cases:
            with pytest.raises(ValueError, match=message):
                _ = [chunk async for chunk in driver.sandbox.command("true", env_vars=env_vars)]

        assert driver.streamed_commands == []

    async def test_delegates_commands_files_and_egress_to_runtime(self) -> None:
        """The sandbox must expose the complete shared operation surface.

        Test cases:
        - Properties, exec, and streaming commands preserve sandbox context.
        - File upload, download, streaming download, and egress reach the driver.
        """
        driver = MockKubernetesRuntimeDriver()
        sandbox = driver.sandbox
        command_env = {"TASK_ID": "task-1"}
        allowed_addresses = ["api.example.com", "10.0.0.0/8"]

        exec_result = await sandbox.exec("python main.py", cwd="/workspace", timeout=30)
        command_chunks = [
            chunk
            async for chunk in sandbox.command(
                "pytest -q",
                cwd="/workspace",
                timeout=60,
                env_vars=command_env,
            )
        ]
        await sandbox.upload_file("/workspace/input.txt", b"input")
        downloaded_content = await sandbox.download_file("/workspace/output.txt")
        streamed_content = [
            chunk
            async for chunk in sandbox.stream_download("/workspace/archive.tar")
        ]
        await sandbox.modify_egress_rules(allowed_addresses)
        await sandbox.clear_egress_rules()

        assert sandbox.id == "sandbox-1"
        assert sandbox.name == "task-1"
        assert sandbox.state == "running"
        assert exec_result == ExecResult(exit_code=0, output="command output")
        assert command_chunks == ["first chunk", "second chunk"]
        assert downloaded_content == b"artifact"
        assert streamed_content == [b"first", b"second"]
        assert driver.executed_commands == [
            ("sandbox-1", "python main.py", "/workspace", 30)
        ]
        assert driver.streamed_commands == [
            ("sandbox-1", "pytest -q", "/workspace", 60, command_env)
        ]
        assert driver.uploaded_files == [
            ("sandbox-1", "/workspace/input.txt", b"input")
        ]
        assert driver.downloaded_files == [("sandbox-1", "/workspace/output.txt")]
        assert driver.streamed_files == [("sandbox-1", "/workspace/archive.tar")]
        assert driver.egress_changes == [("sandbox-1", allowed_addresses)]
        assert driver.cleared_egress == ["sandbox-1"]
