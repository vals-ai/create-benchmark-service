import asyncio
import json
import socket
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest
import uvicorn
import websockets

from benchmark_service import BenchmarkServiceApp, BenchmarkServiceClient, BenchmarkServiceError, Sandbox
from benchmark_service.context import current_tool_context, tool_context_scope
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.schemas import (
    BENCHMARK_TOOLS_HEADER,
    MCPToolContext,
    NativeToolContext,
    RetrieveTaskResponse,
    StreamChunk,
    StreamResultChunk,
)
from conftest import StubBenchmark
from tests.test_app import ProviderSelectionProvider


def test_tool_context_is_native_outside_an_mcp_request() -> None:
    assert current_tool_context() == NativeToolContext(mode="native")
    selected = MCPToolContext(mode="mcp", model="openai/test-model")
    with tool_context_scope(selected):
        assert current_tool_context() == selected
    assert current_tool_context() == NativeToolContext(mode="native")


class ToolContextBenchmark(StubBenchmark):
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        task = await super().retrieve_task(task_id, skip_validation, dataset)
        await asyncio.sleep(0.01)
        context = current_tool_context()
        return task.model_copy(
            update={
                "cwd": context.model_dump_json(),
                "tool_context": context if task_id == "task-1" else NativeToolContext(mode="native"),
            }
        )

    async def setup_task(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        yield StreamResultChunk(type="result", data=current_tool_context().model_dump())


@pytest.fixture
async def running_service(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[str, None]:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)

    def provider(_config: DaytonaProviderConfig) -> ProviderSelectionProvider:
        return ProviderSelectionProvider()

    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", provider)
    app = BenchmarkServiceApp(ToolContextBenchmark)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, log_level="critical"))
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    await asyncio.sleep(0.01)
            yield url
        finally:
            server.should_exit = True
            await asyncio.wait_for(serving, timeout=5)


async def test_http_and_setup_websocket_share_the_selected_tool_context(running_service: str) -> None:
    selected = {"mode": "mcp", "model": "openai/test-model"}
    headers = {
        BENCHMARK_TOOLS_HEADER: json.dumps(selected),
        "x-api-key": "test-key",
        "x-api-url": "https://sandbox.example.test",
        "x-target": "us",
    }
    async with BenchmarkServiceClient(running_service, headers) as client:
        assert json.loads((await client.retrieve_task("task-1")).cwd) == selected
    async with websockets.connect(
        running_service.replace("http://", "ws://") + "/ws/setup-task", additional_headers=headers
    ) as websocket:
        await websocket.send(json.dumps({"task_id": "task-1", "instance_id": "i-1"}))
        assert json.loads(await websocket.recv()) == {"type": "result", "data": selected}
    async with BenchmarkServiceClient(running_service, {}) as client:
        assert json.loads((await client.retrieve_task("task-1")).cwd) == {"mode": "native"}

    async def retrieve(model: str) -> str:
        context = MCPToolContext(mode="mcp", model=model)
        async with BenchmarkServiceClient(
            running_service, {BENCHMARK_TOOLS_HEADER: context.model_dump_json()}
        ) as client:
            return json.loads((await client.retrieve_task("task-1")).cwd)["model"]

    assert await asyncio.gather(retrieve("openai/first"), retrieve("openai/second")) == [
        "openai/first",
        "openai/second",
    ]


@pytest.mark.parametrize("selected", [{"mode": "other"}, {"mode": "mcp"}, {"mode": "mcp", "model": ""}])
async def test_invalid_tool_context_is_rejected(selected: dict[str, Any], running_service: str) -> None:
    async with httpx.AsyncClient(base_url=running_service) as client:
        response = await client.get(
            "/retrieve-task/", params={"task_id": "task-1"}, headers={BENCHMARK_TOOLS_HEADER: json.dumps(selected)}
        )
        assert response.status_code == 400


async def test_mcp_client_rejects_a_native_only_task(running_service: str) -> None:
    selected = MCPToolContext(mode="mcp", model="openai/test-model")
    async with BenchmarkServiceClient(running_service, {BENCHMARK_TOOLS_HEADER: selected.model_dump_json()}) as client:
        with pytest.raises(BenchmarkServiceError, match="tool context"):
            await client.retrieve_task("task-2")
