"""Tests for the private Kubernetes sandbox control API client."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Mapping
from typing import cast

import httpx
import pytest

from benchmark_service.sandbox import sandbox_provider_config_from_mapping
from benchmark_service.sandbox.kubernetes.client import KubernetesControlClientDriver
from benchmark_service.sandbox.kubernetes.config import KubernetesProviderConfig
from benchmark_service.sandbox.kubernetes.protocol import SandboxRecord
from benchmark_service.sandbox.kubernetes.sandbox import KubernetesSandbox
from benchmark_service.sandbox.types import (
    ExecResult,
    ImageSource,
    Resources,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxQuery,
)

SANDBOX = {
    "id": "sandbox-1",
    "name": "task-1",
    "state": "running",
    "labels": {"run_id": "run-1"},
}


class MockLifecycleDriver(KubernetesControlClientDriver):
    """Supply operations outside the lifecycle test scope."""

    async def exec(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        raise AssertionError("operation is outside this lifecycle test")

    async def command(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        raise AssertionError("operation is outside this lifecycle test")
        yield

    async def upload_file(self, instance_id: str, remote_path: str, content: bytes) -> None:
        raise AssertionError("operation is outside this lifecycle test")

    async def download_file(self, instance_id: str, remote_path: str) -> bytes:
        raise AssertionError("operation is outside this lifecycle test")

    async def stream_download(
        self,
        instance_id: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]:
        raise AssertionError("operation is outside this lifecycle test")
        yield

    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None:
        raise AssertionError("operation is outside this lifecycle test")

    async def clear_egress_rules(self, instance_id: str) -> None:
        raise AssertionError("operation is outside this lifecycle test")


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


class TestKubernetesControlClientLifecycle:
    """Lifecycle behavior for the private control API client."""

    async def test_lifecycle_retries_pages_and_translates_errors(self) -> None:
        """Preserve lifecycle semantics across control API behavior.

        Test cases:
        - Create, get, list, and idempotent delete use bearer authentication.
        - Retryable failures are retried and list continuation tokens are followed.
        - Missing sandboxes use the shared not-found error.
        """
        calls: list[tuple[str, str]] = []
        list_calls = 0
        get_calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal get_calls, list_calls
            assert request.headers["Authorization"] == "Bearer test-token"
            calls.append((request.method, request.url.path))
            if request.method == "POST":
                assert json.loads(request.content) == _create_request().model_dump(mode="json")
                return httpx.Response(201, json=SANDBOX)
            if request.method == "GET" and request.url.path == "/v1/sandboxes/missing":
                return httpx.Response(
                    404,
                    json={"error": {"code": "not_found", "message": "missing", "request_id": "req-1"}},
                )
            if request.method == "GET" and request.url.path.endswith("sandbox-1"):
                get_calls += 1
                if get_calls == 1:
                    return httpx.Response(503, json={"error": {"code": "busy", "message": "retry"}})
                return httpx.Response(200, json=SANDBOX)
            if request.method == "GET":
                list_calls += 1
                assert request.url.params.get_list("label") == ["run_id=run-1"]
                assert request.url.params["limit"] == "100"
                if list_calls == 1:
                    return httpx.Response(200, json={"items": [SANDBOX], "continue_token": "next"})
                assert request.url.params["continue_token"] == "next"
                return httpx.Response(
                    200,
                    json={"items": [{**SANDBOX, "id": "sandbox-2", "name": "task-2"}]},
                )
            if request.method == "DELETE":
                return httpx.Response(404, json={"error": {"code": "not_found", "message": "gone"}})
            raise AssertionError(f"Unexpected request: {request.method} {request.url}")

        driver = MockLifecycleDriver(
            api_url="https://sandbox.internal",
            api_token="test-token",
            transport=httpx.MockTransport(handler),
        )

        try:
            created = await driver.create_sandbox(_create_request())
            fetched = await driver.get_sandbox("sandbox-1")
            listed = [
                sandbox
                async for sandbox in driver.list_sandboxes(SandboxQuery(labels={"run_id": "run-1"}, page_size=250))
            ]
            await driver.delete_sandbox("sandbox-1")
            with pytest.raises(SandboxNotFoundError, match="request_id=req-1"):
                await driver.get_sandbox("missing")
        finally:
            await driver.close()

        provider_config = sandbox_provider_config_from_mapping(
            {
                "type": "kubernetes",
                "KUBERNETES_API_URL": "https://sandbox.internal",
                "KUBERNETES_API_TOKEN": "secret-reference-value",
            }
        )

        assert created.id == fetched.id == "sandbox-1"
        assert isinstance(created, KubernetesSandbox)
        assert isinstance(provider_config, KubernetesProviderConfig)
        assert created.labels == {"run_id": "run-1"}
        assert [sandbox.id for sandbox in listed] == ["sandbox-1", "sandbox-2"]
        assert get_calls == 2
        assert list_calls == 2
        assert calls.count(("DELETE", "/v1/sandboxes/sandbox-1")) == 1

    async def test_maps_exhausted_retryable_status_to_connection_error(self) -> None:
        """Keep exhausted control-service failures retryable by framework callers.

        Test cases:
        - HTTP 429 and 503 responses retry three times and become connection errors.
        """
        for status_code in (429, 503):
            attempts = 0

            def handler(
                _request: httpx.Request,
                status_code: int = status_code,
            ) -> httpx.Response:
                nonlocal attempts
                attempts += 1
                return httpx.Response(
                    status_code,
                    json={"error": {"code": "busy", "message": "retry later"}},
                )

            driver = MockLifecycleDriver(
                api_url="https://sandbox.internal",
                api_token="test-token",
                transport=httpx.MockTransport(handler),
            )
            try:
                with pytest.raises(SandboxConnectionError, match="retry later"):
                    await driver.get_sandbox("sandbox-1")
            finally:
                await driver.close()

            assert attempts == 3


class MockCommandDriver(KubernetesControlClientDriver):
    """Supply file and egress operations outside command test scope."""

    def sandbox_for_test(self, instance_id: str) -> KubernetesSandbox:
        return self._sandbox(driver_record(instance_id))

    async def upload_file(self, instance_id: str, remote_path: str, content: bytes) -> None:
        raise AssertionError("operation is outside this command test")

    async def download_file(self, instance_id: str, remote_path: str) -> bytes:
        raise AssertionError("operation is outside this command test")

    async def stream_download(
        self,
        instance_id: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]:
        raise AssertionError("operation is outside this command test")
        yield

    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None:
        raise AssertionError("operation is outside this command test")

    async def clear_egress_rules(self, instance_id: str) -> None:
        raise AssertionError("operation is outside this command test")


class TestKubernetesControlClientCommands:
    """Command behavior for the private control API client."""

    async def test_streams_command_events_and_preserves_exec_payload(self) -> None:
        """Deliver command output before exit and keep buffered exec distinct.

        Test cases:
        - HTTP stdout and stderr events are yielded in order before exit.
        - Closing a command stream early closes its response without leaking GeneratorExit.
        - A nonzero exit becomes the shared command error.
        - Buffered exec sends all supported request fields without retries.
        """
        release_exit = asyncio.Event()
        observed_payloads: list[dict[str, object]] = []
        opened_streams: list[MockDelayedCommandStream] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["authorization"] == "Bearer test-token"
            if request.url.path.endswith("/command"):
                assert request.extensions["timeout"]["read"] == 23
            payload = cast(dict[str, object], json.loads(await request.aread()))
            observed_payloads.append(payload)
            if request.url.path.endswith("/exec"):
                return httpx.Response(200, json={"exit_code": 0, "output": "done"})

            instance_id = request.url.path.split("/")[-2]
            if instance_id == "truncated":
                return httpx.Response(
                    200,
                    headers={"Content-Type": "application/x-ndjson"},
                    stream=MockStaticCommandStream([b'{"type":"stdout","data":"partial"}\n']),
                )
            if instance_id == "invalid":
                return httpx.Response(
                    200,
                    headers={"Content-Type": "application/x-ndjson"},
                    stream=MockStaticCommandStream([b"not-json\n"]),
                )
            stream = MockDelayedCommandStream(
                release_exit=None if instance_id == "failed" else release_exit,
                exit_code=7 if instance_id == "failed" else 0,
            )
            opened_streams.append(stream)

            return httpx.Response(200, headers={"Content-Type": "application/x-ndjson"}, stream=stream)

        driver = MockCommandDriver(
            api_url="https://sandbox.internal",
            api_token="test-token",
            stream_read_timeout=23,
            transport=httpx.MockTransport(handler),
        )
        try:
            sandbox = driver.sandbox_for_test("sandbox-1")
            chunks = sandbox.command(
                "printf hello",
                cwd="/workspace",
                timeout=3,
                env_vars={"FOO": "bar"},
            )
            assert await anext(chunks) == "hel"
            assert await anext(chunks) == "lo"

            cancelled = sandbox.command("sleep 60")
            assert await anext(cancelled) == "hel"
            await cancelled.aclose()
            assert opened_streams[1].closed.is_set()

            release_exit.set()
            with pytest.raises(StopAsyncIteration):
                await anext(chunks)

            failed = driver.sandbox_for_test("failed")
            with pytest.raises(SandboxCommandError) as error_info:
                _ = [chunk async for chunk in failed.command("false")]

            for instance_id, message in [("truncated", "exit event"), ("invalid", "invalid command event")]:
                incomplete = driver.sandbox_for_test(instance_id)
                with pytest.raises(SandboxConnectionError, match=message):
                    _ = [chunk async for chunk in incomplete.command("true")]

            result = await sandbox.exec("echo done", cwd="/workspace", timeout=5)
        finally:
            await driver.close()

        assert error_info.value.exit_code == 7
        assert result == ExecResult(exit_code=0, output="done")
        assert observed_payloads == [
            {"command": "printf hello", "cwd": "/workspace", "timeout": 3.0, "env_vars": {"FOO": "bar"}},
            {"command": "sleep 60", "cwd": None, "timeout": None, "env_vars": None},
            {"command": "false", "cwd": None, "timeout": None, "env_vars": None},
            {"command": "true", "cwd": None, "timeout": None, "env_vars": None},
            {"command": "true", "cwd": None, "timeout": None, "env_vars": None},
            {"command": "echo done", "cwd": "/workspace", "timeout": 5.0, "env_vars": None},
        ]


class MockDelayedCommandStream(httpx.AsyncByteStream):
    """Emit command events around a caller-controlled exit boundary."""

    def __init__(self, release_exit: asyncio.Event | None, exit_code: int) -> None:
        self.release_exit = release_exit
        self.exit_code = exit_code
        self.closed = asyncio.Event()

    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        if self.release_exit is not None:
            yield b'\n{"type":"stdout","data":"hel"}\n'
            yield b'{"type":"stderr","data":"lo"}\n'
            await self.release_exit.wait()
        yield f'{{"type":"exit","exit_code":{self.exit_code}}}\n'.encode()

    async def aclose(self) -> None:
        self.closed.set()


class MockStaticCommandStream(httpx.AsyncByteStream):
    """Emit a fixed command response for protocol-failure cases."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        pass


class MockDelayedByteStream(httpx.AsyncByteStream):
    """Yield two chunks with a caller-controlled boundary."""

    def __init__(self, release_second_chunk: asyncio.Event) -> None:
        self.release_second_chunk = release_second_chunk

    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        yield b"first"
        await self.release_second_chunk.wait()
        yield b"second"


class TestKubernetesControlClientFilesAndEgress:
    """File and egress behavior for the private control API client."""

    async def test_streams_files_and_replaces_temporary_egress_rules(self) -> None:
        """Preserve binary files and temporary egress semantics.

        Test cases:
        - Streaming download yields before the response is complete.
        - Buffered download joins the same chunks and upload preserves arbitrary bytes.
        - Egress replacement sends the allowlist and clear restores unrestricted access.
        """
        release_second_chunk = asyncio.Event()
        observed_uploads: list[tuple[str, bytes]] = []
        observed_egress: list[list[str]] = []
        clear_calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal clear_calls
            assert request.headers["Authorization"] == "Bearer test-token"
            if request.url.path.endswith("files"):
                assert request.url.params["path"] == "/workspace/data.bin"
            if request.method == "GET":
                return httpx.Response(200, stream=MockDelayedByteStream(release_second_chunk))
            if request.method == "PUT" and request.url.path.endswith("files"):
                observed_uploads.append((request.url.params["path"], await request.aread()))
                return httpx.Response(204)
            if request.method == "PUT":
                observed_egress.append(json.loads(await request.aread())["allowed_addresses"])
                return httpx.Response(204)
            if request.method == "DELETE":
                clear_calls += 1
                return httpx.Response(204)
            raise AssertionError(f"Unexpected request: {request.method} {request.url}")

        driver = KubernetesControlClientDriver(
            api_url="https://sandbox.internal",
            api_token="test-token",
            transport=httpx.MockTransport(handler),
        )
        allowed_addresses = ["api.example.com", "10.0.0.0/8"]
        try:
            stream = driver.stream_download("sandbox-1", "/workspace/data.bin")
            assert await anext(stream) == b"first"
            pending_chunk = asyncio.create_task(anext(stream))
            await asyncio.sleep(0)
            assert pending_chunk.done() is False
            release_second_chunk.set()
            assert await pending_chunk == b"second"
            with pytest.raises(StopAsyncIteration):
                await anext(stream)

            downloaded = await driver.download_file("sandbox-1", "/workspace/data.bin")
            await driver.upload_file("sandbox-1", "/workspace/data.bin", b"\x00\xffpayload")
            await driver.modify_egress_rules("sandbox-1", allowed_addresses)
            await driver.clear_egress_rules("sandbox-1")
        finally:
            await driver.close()

        assert downloaded == b"firstsecond"
        assert observed_uploads == [("/workspace/data.bin", b"\x00\xffpayload")]
        assert observed_egress == [allowed_addresses]
        assert clear_calls == 1


def driver_record(instance_id: str) -> SandboxRecord:
    return SandboxRecord(id=instance_id, name="task-1", state="running")
