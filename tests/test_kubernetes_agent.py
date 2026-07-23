"""Tests for the direct in-Pod sandbox data plane."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import httpx
import pytest

from benchmark_service.sandbox.kubernetes.control.agent import PodAgentClient, agent_token
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import CommandExitEvent, CommandOutputEvent, CommandRequest
from benchmark_service.sandbox.types import SandboxConnectionError, SandboxError


class MockClosingStream(httpx.AsyncByteStream):
    """Expose NDJSON chunks and record early response closure."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def _settings() -> KubernetesControlSettings:
    return KubernetesControlSettings(
        api_token="control-secret",
        docker_image="registry.internal/docker@sha256:" + "a" * 64,
        agent_image="registry.internal/control@sha256:" + "b" * 64,
        agent_port=8787,
        exec_connection_pool_size=32,
    )


class TestPodAgentClient:
    """Authenticated command and file traffic sent directly to a sandbox Pod."""

    async def test_streams_commands_and_files_without_kubernetes_exec(self) -> None:
        """Keep command and file bytes on the Pod HTTP data plane.

        Test cases:
        - A sandbox-specific bearer token authenticates every request.
        - Command output and exit events stream in order from the Pod IP.
        - Upload and download bodies remain binary and response closure propagates.
        """
        stream = MockClosingStream(
            [
                b'{"type":"stdout","data":"first"}\n',
                b'{"type":"stderr","data":"warning"}\n',
                b'{"type":"exit","exit_code":0}\n',
            ]
        )
        uploaded = b""

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal uploaded
            assert request.url.host == "10.0.0.8"
            assert request.url.port == 8787
            assert request.headers["Authorization"] == f"Bearer {agent_token('control-secret', 'task-1')}"
            if request.url.path == "/v1/command":
                assert json.loads(request.content) == {
                    "command": "printf ok",
                    "cwd": "/workspace",
                    "timeout": 4.0,
                    "env_vars": {"TASK": "one"},
                }
                return httpx.Response(200, headers={"content-type": "application/x-ndjson"}, stream=stream)
            if request.method == "PUT":
                uploaded = await request.aread()
                assert request.url.params["path"] == "/workspace/result.bin"
                return httpx.Response(204)
            assert request.method == "GET"
            return httpx.Response(200, content=b"downloaded")

        client = PodAgentClient(_settings(), transport=httpx.MockTransport(handler))
        events = [
            event
            async for event in client.command(
                "10.0.0.8",
                "task-1",
                CommandRequest(
                    command="printf ok",
                    cwd="/workspace",
                    timeout=4,
                    env_vars={"TASK": "one"},
                ),
            )
        ]
        await client.upload_file("10.0.0.8", "task-1", "/workspace/result.bin", _chunks([b"up", b"loaded"]))
        downloaded = [
            chunk
            async for chunk in client.stream_download(
                "10.0.0.8",
                "task-1",
                "/workspace/result.bin",
            )
        ]
        await client.close()

        assert [(event.type, event.data) for event in events[:-1] if isinstance(event, CommandOutputEvent)] == [
            ("stdout", "first"),
            ("stderr", "warning"),
        ]
        assert isinstance(events[-1], CommandExitEvent) and events[-1].exit_code == 0
        assert uploaded == b"uploaded"
        assert b"".join(downloaded) == b"downloaded"
        assert stream.closed is True

    async def test_rejects_agent_errors_and_invalid_pod_addresses(self) -> None:
        """Fail closed before traffic can escape the Pod network boundary.

        Test cases:
        - Only literal Pod IP addresses are accepted as destinations.
        - Agent authorization failures become stable sandbox errors.
        - A command response without an exit event is treated as disconnected.
        """

        async def denied(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(401, text="unauthorized")

        client = PodAgentClient(_settings(), transport=httpx.MockTransport(denied))
        with pytest.raises(SandboxError, match="HTTP 401"):
            _ = [
                event
                async for event in client.command(
                    "10.0.0.8",
                    "task-1",
                    CommandRequest(command="true"),
                )
            ]
        with pytest.raises(SandboxError, match="Pod IP"):
            _ = [
                event
                async for event in client.command(
                    "example.com",
                    "task-1",
                    CommandRequest(command="true"),
                )
            ]
        await client.close()

        async def truncated(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(
                200,
                stream=MockClosingStream([b'{"type":"stdout","data":"partial"}\n']),
            )

        truncated_client = PodAgentClient(_settings(), transport=httpx.MockTransport(truncated))
        with pytest.raises(SandboxConnectionError, match="exit event"):
            _ = [
                event
                async for event in truncated_client.command(
                    "10.0.0.8",
                    "task-1",
                    CommandRequest(command="true"),
                )
            ]
        await truncated_client.close()


async def _chunks(values: list[bytes]) -> AsyncGenerator[bytes, None]:
    for value in values:
        yield value
