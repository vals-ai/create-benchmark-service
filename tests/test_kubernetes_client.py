"""Tests for the private Kubernetes sandbox control API client."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Mapping

import httpx
import pytest

from benchmark_service.sandbox.kubernetes.client import KubernetesControlClientDriver
from benchmark_service.sandbox.types import (
    ExecResult,
    ImageSource,
    Resources,
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
                async for sandbox in driver.list_sandboxes(
                    SandboxQuery(labels={"run_id": "run-1"}, page_size=2)
                )
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
