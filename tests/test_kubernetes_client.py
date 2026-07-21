"""Tests for the private Kubernetes sandbox control API client."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Mapping

import httpx
import pytest
from fastapi import FastAPI, WebSocket
from httpx_ws.transport import ASGIWebSocketTransport

from benchmark_service.sandbox.kubernetes.client import KubernetesControlClientDriver
from benchmark_service.sandbox.kubernetes.protocol import SandboxRecord
from benchmark_service.sandbox.kubernetes.sandbox import KubernetesSandbox
from benchmark_service.sandbox.types import (
    ExecResult,
    ImageSource,
    Resources,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxQuery,
)

SANDBOX = {"id": "sandbox-1", "name": "task-1", "state": "running"}


class LifecycleTestDriver(KubernetesControlClientDriver):
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

        driver = LifecycleTestDriver(
            api_url="https://sandbox.internal",
            api_token="test-token",
            transport=httpx.MockTransport(handler),
        )

        try:
            created = await driver.create_sandbox(_create_request())
            fetched = await driver.get_sandbox("sandbox-1")
            listed = [
                sandbox
                async for sandbox in driver.list_sandboxes(SandboxQuery(labels={"run_id": "run-1"}, page_size=2))
            ]
            await driver.delete_sandbox("sandbox-1")
            with pytest.raises(SandboxNotFoundError, match="request_id=req-1"):
                await driver.get_sandbox("missing")
        finally:
            await driver.close()

        assert created.id == fetched.id == "sandbox-1"
        assert [sandbox.id for sandbox in listed] == ["sandbox-1", "sandbox-2"]
        assert get_calls == 2
        assert list_calls == 2
        assert calls.count(("DELETE", "/v1/sandboxes/sandbox-1")) == 1

    async def test_rejects_invalid_timeouts(self) -> None:
        """Reject unusable client timeouts before opening a connection.

        Test cases:
        - Connect and request timeouts must both be positive.
        """
        for connect_timeout, request_timeout in [(0, 1), (1, 0)]:
            with pytest.raises(ValueError, match="positive"):
                LifecycleTestDriver(
                    api_url="https://sandbox.internal",
                    api_token="test-token",
                    connect_timeout=connect_timeout,
                    request_timeout=request_timeout,
                )


class CommandTestDriver(KubernetesControlClientDriver):
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
        - WebSocket stdout and stderr events are yielded in order before exit.
        - A nonzero exit becomes the shared command error.
        - Buffered exec sends all supported request fields without retries.
        """
        app = FastAPI()
        release_exit = asyncio.Event()
        observed_payloads: list[dict[str, object]] = []

        async def command(websocket: WebSocket, instance_id: str) -> None:
            assert websocket.headers["authorization"] == "Bearer test-token"
            await websocket.accept()
            observed_payloads.append(await websocket.receive_json())
            if instance_id == "failed":
                await websocket.send_json({"type": "exit", "exit_code": 7})
                return
            await websocket.send_json({"type": "stdout", "data": "hel"})
            await websocket.send_json({"type": "stderr", "data": "lo"})
            await release_exit.wait()
            await websocket.send_json({"type": "exit", "exit_code": 0})

        async def exec_command(instance_id: str, payload: dict[str, object]) -> dict[str, object]:
            assert instance_id == "sandbox-1"
            observed_payloads.append(payload)
            return {"exit_code": 0, "output": "done"}

        app.websocket("/v1/sandboxes/{instance_id}/command")(command)
        app.post("/v1/sandboxes/{instance_id}/exec")(exec_command)

        transport = ASGIWebSocketTransport(app=app)
        async with transport:
            driver = CommandTestDriver(
                api_url="https://sandbox.internal",
                api_token="test-token",
                transport=transport,
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
                release_exit.set()
                with pytest.raises(StopAsyncIteration):
                    await anext(chunks)

                failed = driver.sandbox_for_test("failed")
                with pytest.raises(SandboxCommandError) as error_info:
                    _ = [chunk async for chunk in failed.command("false")]

                result = await sandbox.exec("echo done", cwd="/workspace", timeout=5)
            finally:
                await driver.close()

        assert error_info.value.exit_code == 7
        assert result == ExecResult(exit_code=0, output="done")
        assert observed_payloads == [
            {"command": "printf hello", "cwd": "/workspace", "timeout": 3.0, "env_vars": {"FOO": "bar"}},
            {"command": "false", "cwd": None, "timeout": None, "env_vars": None},
            {"command": "echo done", "cwd": "/workspace", "timeout": 5.0, "env_vars": None},
        ]


def driver_record(instance_id: str) -> SandboxRecord:
    return SandboxRecord(id=instance_id, name="task-1", state="running")
