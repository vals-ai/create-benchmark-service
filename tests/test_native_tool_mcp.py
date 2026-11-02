import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from pydantic import BaseModel

from benchmark_service.tool_runtime.native_tool_mcp import create_tool_mcp_app


class Output(BaseModel):
    output: str | list[dict[str, str]]
    error: str | None = None
    done: bool = False
    metadata: BaseModel | None = None
    native_query_result: BaseModel | None = None


class Metadata(BaseModel):
    input_tokens: int = 12
    source: str = "secret-value"


class NativeTool:
    description = "Native test tool"
    parameters: dict[str, Any] = {"value": {"type": "string"}}
    required = ["value"]

    def __init__(self, name: str):
        self.name = name

    async def execute(self, args: dict[str, Any], state: dict[str, Any], logger: Any) -> Output:
        if self.name == "bash":
            return Output(output="sandbox executor result")
        if self.name == "image":
            return Output(
                output=[
                    {"type": "text", "text": "Native image"},
                    {"type": "image", "data": "aW1hZ2U=", "mimeType": "image/png"},
                ]
            )
        if self.name == "store":
            state["document"] = args["value"]
            return Output(output="stored")
        if self.name == "retrieve":
            return Output(output=state["document"])
        if self.name == "submit":
            return Output(output=args["value"], done=True)
        if self.name == "fail":
            return Output(
                output="failed secret-value", error="secret-value", metadata=Metadata(), native_query_result=Metadata()
            )
        if self.name == "raise":
            raise ValueError("failure secret-value")
        value = state.get("counter", 0)
        await asyncio.sleep(0.01)
        state["counter"] = value + 1
        return Output(output=str(value + 1))


@pytest.fixture
async def client(tmp_path: Path) -> AsyncIterator[TestClient[web.Request, web.Application]]:
    app = create_tool_mcp_app(
        tools=[NativeTool(name) for name in ("store", "retrieve", "submit", "fail", "raise", "count")],
        task_id="task-1",
        model="provider/test-model",
        output_dir=tmp_path / "private",
        secret_values=("secret-value",),
    )
    async with TestClient(TestServer(app)) as connected:
        yield connected


async def rpc(client: TestClient[web.Request, web.Application], method: str, params: dict[str, Any]) -> dict[str, Any]:
    response = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    assert response.status == 200
    return await response.json()


async def test_native_state_submission_and_postsubmit_rejection(
    client: TestClient[web.Request, web.Application], tmp_path: Path
) -> None:
    initialized = await rpc(client, "initialize", {"protocolVersion": "2025-06-18"})
    assert initialized["result"]["capabilities"] == {"tools": {"listChanged": False}}
    listed = await rpc(client, "tools/list", {})
    assert listed["result"]["tools"][0]["inputSchema"] == {
        "type": "object",
        "properties": NativeTool.parameters,
        "required": ["value"],
    }
    await rpc(client, "tools/call", {"name": "store", "arguments": {"value": "kept document"}})
    retrieved = await rpc(client, "tools/call", {"name": "retrieve", "arguments": {}})
    assert retrieved["result"]["content"] == [{"type": "text", "text": "kept document"}]
    await rpc(client, "tools/call", {"name": "submit", "arguments": {"value": "final answer"}})
    assert json.loads((tmp_path / "private/generation.json").read_text()) == {
        "task_id": "task-1",
        "model": "provider/test-model",
        "status": "success",
        "answer": "final answer",
    }
    rejected = await rpc(client, "tools/call", {"name": "store", "arguments": {"value": "changed"}})
    assert rejected["result"]["isError"] is True
    assert "submitted" in rejected["result"]["content"][0]["text"]


@pytest.mark.parametrize("name", ["fail", "raise"])
async def test_native_errors_are_redacted_and_recorded(
    client: TestClient[web.Request, web.Application], tmp_path: Path, name: str
) -> None:
    result = await rpc(client, "tools/call", {"name": name, "arguments": {}})
    assert result["result"]["isError"] is True
    records = (tmp_path / "private/tool_calls.jsonl").read_text()
    assert "secret-value" not in json.dumps(result) + records
    event = json.loads(records)
    assert event["error"] is not None
    assert event["duration_seconds"] >= 0
    if name == "fail":
        assert event["metadata"]["input_tokens"] == 12
        assert event["native_query_result"]["input_tokens"] == 12
    assert (tmp_path / "private").stat().st_mode & 0o777 == 0o700
    assert (tmp_path / "private/tool_calls.jsonl").stat().st_mode & 0o777 == 0o600


async def test_calls_share_one_serialized_state(client: TestClient[web.Request, web.Application]) -> None:
    results = await asyncio.gather(*(rpc(client, "tools/call", {"name": "count", "arguments": {}}) for _ in range(3)))
    assert sorted(result["result"]["content"][0]["text"] for result in results) == ["1", "2", "3"]


async def test_transport_rejects_unknown_methods_and_remote_origin(
    client: TestClient[web.Request, web.Application],
) -> None:
    unknown = await rpc(client, "not-a-method", {})
    assert unknown["error"]["code"] == -32601
    notification = await client.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert notification.status == 202
    assert not await notification.read()
    assert (await client.get("/mcp")).status == 405
    remote = await client.post(
        "/mcp", headers={"Origin": "https://untrusted.test"}, json={"jsonrpc": "2.0", "id": 1, "method": "ping"}
    )
    assert remote.status == 403


async def test_only_registered_sandbox_executor_can_run(tmp_path: Path) -> None:
    app = create_tool_mcp_app(
        tools=[NativeTool("bash"), NativeTool("image")],
        task_id="task-1",
        model="provider/test-model",
        output_dir=tmp_path,
        secret_values=(),
    )
    async with TestClient(TestServer(app)) as client:
        result = await rpc(client, "tools/call", {"name": "bash", "arguments": {"value": "pwd"}})
        assert result["result"]["content"] == [{"type": "text", "text": "sandbox executor result"}]
        rejected = await rpc(client, "tools/call", {"name": "host_shell", "arguments": {}})
        assert rejected["error"]["code"] == -32602
        image = await rpc(client, "tools/call", {"name": "image", "arguments": {}})
        assert image["result"]["content"] == [
            {"type": "text", "text": "Native image"},
            {"type": "image", "data": "aW1hZ2U=", "mimeType": "image/png"},
        ]
