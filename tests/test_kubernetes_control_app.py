"""Tests for the private Kubernetes sandbox control API."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from datetime import datetime

import httpx
import pytest
from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

from benchmark_service.sandbox.kubernetes.control import (
    KubernetesControlSettings,
    create_kubernetes_control_app,
)
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandEvent,
    CommandExitEvent,
    CommandOutputEvent,
    CommandRequest,
    ExecResponse,
    SandboxListPage,
    SandboxRecord,
)
from benchmark_service.sandbox.types import SandboxCreateRequest

SANDBOX = SandboxRecord(id="sandbox-1", name="task-1", state="running")


class RecordingControlBackend:
    """Record each private API operation for contract assertions."""

    def __init__(self) -> None:
        self.operations: list[tuple[str, object]] = []
        self.closed = False
        self.janitor_calls = 0

    async def create_sandbox(self, request: SandboxCreateRequest) -> SandboxRecord:
        self.operations.append(("create", request))
        return SANDBOX

    async def get_sandbox(self, instance_id: str) -> SandboxRecord:
        self.operations.append(("get", instance_id))
        return SANDBOX

    async def list_sandboxes(
        self,
        labels: dict[str, str],
        limit: int,
        continue_token: str | None,
    ) -> SandboxListPage:
        self.operations.append(("list", (labels, limit, continue_token)))
        return SandboxListPage(items=[SANDBOX], continue_token="next")

    async def delete_sandbox(self, instance_id: str) -> None:
        self.operations.append(("delete", instance_id))

    async def exec(self, instance_id: str, request: CommandRequest) -> ExecResponse:
        self.operations.append(("exec", (instance_id, request)))
        return ExecResponse(exit_code=0, output="done")

    async def command(
        self,
        instance_id: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandEvent, None]:
        self.operations.append(("command", (instance_id, request)))
        yield CommandOutputEvent(type="stdout", data="first")
        yield CommandOutputEvent(type="stderr", data="second")
        yield CommandExitEvent(type="exit", exit_code=0)

    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None:
        content = b"".join([chunk async for chunk in chunks])
        self.operations.append(("upload", (instance_id, remote_path, content)))

    async def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
        self.operations.append(("download", (instance_id, remote_path)))
        yield b"first"
        yield b"second"

    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None:
        self.operations.append(("modify_egress", (instance_id, allowed_addresses)))

    async def clear_egress_rules(self, instance_id: str) -> None:
        self.operations.append(("clear_egress", instance_id))

    async def delete_idle_sandboxes(self, now: datetime) -> int:
        del now
        self.janitor_calls += 1
        return 0

    async def close(self) -> None:
        self.closed = True


def _settings() -> KubernetesControlSettings:
    return KubernetesControlSettings(
        api_token="test-token",
        docker_image="registry.internal/docker@sha256:abc",
    )


def _create_payload() -> dict[str, object]:
    return {
        "source": {"type": "image", "image": "registry.internal/python@sha256:def"},
        "resources": {"vcpu": 2, "memory": 4, "disk": 20},
        "name": "task-1",
        "labels": {"run_id": "run-1"},
        "env_vars": {"WORKSPACE": "/workspace"},
        "auto_stop_interval": 30,
        "create_timeout": 120,
    }


class TestKubernetesControlApp:
    """Private HTTP and WebSocket protocol behavior."""

    async def test_http_contract_auth_request_ids_and_operations(self) -> None:
        """Preserve lifecycle, files, egress, authentication, and request IDs.

        Test cases:
        - Health is configuration-free while protected routes reject missing tokens.
        - Lifecycle, exec, chunked files, pagination, and egress reach distinct backend methods.
        - Every HTTP response includes a request ID.
        """
        backend = RecordingControlBackend()
        app = create_kubernetes_control_app(_settings(), backend)
        transport = httpx.ASGITransport(app=app)

        async def upload_chunks() -> AsyncGenerator[bytes, None]:
            yield b"first"
            yield b"second"

        async with httpx.AsyncClient(transport=transport, base_url="http://control") as client:
            health = await client.get("/health")
            unauthorized = await client.get("/v1/sandboxes/sandbox-1")
            headers = {"Authorization": "Bearer test-token", "X-Request-ID": "req-test"}
            created = await client.post("/v1/sandboxes", headers=headers, json=_create_payload())
            fetched = await client.get("/v1/sandboxes/sandbox-1", headers=headers)
            listed = await client.get(
                "/v1/sandboxes",
                headers=headers,
                params=[("label", "run_id=run-1"), ("limit", "2"), ("continue_token", "prior")],
            )
            executed = await client.post(
                "/v1/sandboxes/sandbox-1/exec",
                headers=headers,
                json={"command": "true"},
            )
            uploaded = await client.put(
                "/v1/sandboxes/sandbox-1/files",
                headers=headers,
                params={"path": "/workspace/data.bin"},
                content=upload_chunks(),
            )
            downloaded = await client.get(
                "/v1/sandboxes/sandbox-1/files",
                headers=headers,
                params={"path": "/workspace/data.bin"},
            )
            modified = await client.put(
                "/v1/sandboxes/sandbox-1/egress",
                headers=headers,
                json={"allowed_addresses": ["api.example.com"]},
            )
            cleared = await client.delete("/v1/sandboxes/sandbox-1/egress", headers=headers)
            deleted = await client.delete("/v1/sandboxes/sandbox-1", headers=headers)

        responses = [
            health,
            unauthorized,
            created,
            fetched,
            listed,
            executed,
            uploaded,
            downloaded,
            modified,
            cleared,
            deleted,
        ]
        assert health.json() == {"status": "ok"}
        assert unauthorized.status_code == 401
        assert all(response.headers["X-Request-ID"] for response in responses)
        assert created.status_code == 201 and fetched.json() == SANDBOX.model_dump(mode="json")
        assert listed.json()["continue_token"] == "next"
        assert executed.json() == {"exit_code": 0, "output": "done"}
        assert uploaded.status_code == modified.status_code == cleared.status_code == deleted.status_code == 204
        assert downloaded.content == b"firstsecond"
        assert ("list", ({"run_id": "run-1"}, 2, "prior")) in backend.operations
        assert ("upload", ("sandbox-1", "/workspace/data.bin", b"firstsecond")) in backend.operations
        assert ("modify_egress", ("sandbox-1", ["api.example.com"])) in backend.operations
        assert ("clear_egress", "sandbox-1") in backend.operations

    async def test_websocket_auth_and_command_event_order(self) -> None:
        """Authenticate command streams and preserve event order.

        Test cases:
        - Incorrect tokens close before command execution.
        - Valid commands receive stdout, stderr, and exit events in order.
        """
        backend = RecordingControlBackend()
        app = create_kubernetes_control_app(_settings(), backend)
        transport = ASGIWebSocketTransport(app=app)
        async with transport, httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(WebSocketDisconnect) as error_info:
                async with aconnect_ws(
                    "ws://control/v1/sandboxes/sandbox-1/command",
                    client=client,
                    headers={"Authorization": "Bearer wrong"},
                ):
                    pass

            async with aconnect_ws(
                "ws://control/v1/sandboxes/sandbox-1/command",
                client=client,
                headers={"Authorization": "Bearer test-token", "X-Request-ID": "req-ws"},
            ) as websocket:
                await websocket.send_json({"command": "printf ok"})
                events = [await websocket.receive_json() for _ in range(3)]

            async with aconnect_ws(
                "ws://control/v1/sandboxes/sandbox-1/command",
                client=client,
                headers={"Authorization": "Bearer test-token", "X-Request-ID": "req-error"},
            ) as websocket:
                await websocket.send_json({"cwd": "/workspace"})
                error_event = await websocket.receive_json()

        assert error_info.value.code == 1008
        assert [event["type"] for event in events] == ["stdout", "stderr", "exit"]
        assert error_event["type"] == "error"
        assert error_event["request_id"] == "req-error"
        assert backend.operations[-1][0] == "command"

    async def test_lifespan_cancels_janitor_and_closes_backend(self) -> None:
        """Stop background cleanup before closing Kubernetes clients.

        Test cases:
        - App shutdown cancels the janitor and closes the backend once.
        """
        backend = RecordingControlBackend()
        app = create_kubernetes_control_app(_settings(), backend)

        async with app.router.lifespan_context(app):
            pass

        assert backend.closed is True
