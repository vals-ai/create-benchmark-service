"""Expose native benchmark tools to a task-local MCP client.

The caller runs this app on loopback under a tools-only UID. Keep its output
outside the agent workspace. The caller owns the trusted tool roster, credentials,
networking, and shutdown. The caller must confine tools that execute code.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlsplit

from aiohttp import web
from pydantic import BaseModel, StrictInt, StrictStr, ValidationError


class NativeTool(Protocol):
    name: str
    description: str
    parameters: dict[str, Any]
    required: list[str]

    async def execute(self, args: dict[str, Any], state: dict[str, Any], logger: logging.Logger) -> Any: ...


class _Message(BaseModel):
    jsonrpc: Literal["2.0"]
    id: StrictInt | StrictStr | None = None
    method: str
    params: dict[str, Any] = {}


class _Call(BaseModel):
    name: str
    arguments: dict[str, Any] = {}


def create_tool_mcp_app(
    tools: Sequence[NativeTool],
    task_id: str,
    model: str,
    output_dir: Path,
    secret_values: Sequence[str],
) -> web.Application:
    """Keep one native tool state and one frozen submission for this task."""
    registry = {tool.name: tool for tool in tools}
    assert len(registry) == len(tools), "Tool names must be unique"
    assert all(secret_values), "Secret values must not be empty"
    output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    output_dir.chmod(0o700)
    state: dict[str, Any] = {}
    lock = asyncio.Lock()
    submitted = False
    logger = logging.Logger("benchmark-native-tools")
    logger.addHandler(logging.NullHandler())

    def redact(value: Any) -> Any:
        if isinstance(value, str):
            for secret in secret_values:
                value = value.replace(secret, "[REDACTED]")
            return value
        if isinstance(value, dict):
            return {key: redact(item) for key, item in cast(dict[str, Any], value).items()}
        if isinstance(value, list):
            return [redact(item) for item in cast(list[Any], value)]
        return value

    def error(request_id: str | int | None, code: int, message: str) -> web.Response:
        return web.json_response(
            {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": redact(message)}}
        )

    async def handle(request: web.Request) -> web.Response:
        nonlocal submitted
        origin = request.headers.get("Origin")
        if origin is not None:
            try:
                host = urlsplit(origin).hostname
            except ValueError:
                host = None
            if host not in {"127.0.0.1", "localhost", "::1"}:
                raise web.HTTPForbidden(text="Only loopback origins are allowed")
        try:
            message = _Message.model_validate(await request.json())
        except (ValueError, ValidationError):
            return error(None, -32600, "Invalid JSON-RPC request")
        if message.id is None:
            return web.Response(status=202)
        if message.method == "initialize":
            version = message.params.get("protocolVersion")
            result: dict[str, Any] = {
                "protocolVersion": version if version in {"2025-03-26", "2025-06-18", "2025-11-25"} else "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "benchmark-native-tools", "version": "1"},
            }
        elif message.method == "ping":
            result = {}
        elif message.method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": {"type": "object", "properties": tool.parameters, "required": tool.required},
                    }
                    for tool in tools
                ]
            }
        elif message.method == "tools/call":
            try:
                call = _Call.model_validate(message.params)
            except ValidationError:
                return error(message.id, -32602, "Invalid tool arguments")
            if call.name not in registry:
                return error(message.id, -32602, f"Unknown tool: {call.name}")
            async with lock:
                if submitted:
                    return web.json_response(
                        {
                            "jsonrpc": "2.0",
                            "id": message.id,
                            "result": {
                                "content": [{"type": "text", "text": "Task already submitted"}],
                                "isError": True,
                            },
                        }
                    )
                started = time.monotonic()
                record: dict[str, Any] = {
                    "tool": call.name,
                    "arguments": call.arguments,
                    "started_at": datetime.now(UTC).isoformat(),
                }
                try:
                    native = await registry[call.name].execute(call.arguments, state, logger)
                    record |= {"output": native.output, "error": native.error, "done": native.done}
                    for name in ("metadata", "native_query_result"):
                        value = getattr(native, name)
                        record[name] = value.model_dump(mode="json") if value is not None else None
                except Exception as exc:
                    record |= {"output": str(exc), "error": str(exc), "done": False}
                record["duration_seconds"] = time.monotonic() - started
                record = redact(record)
                descriptor = os.open(output_dir / "tool_calls.jsonl", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
                with os.fdopen(descriptor, "a", encoding="utf-8") as log:
                    log.write(json.dumps(record, ensure_ascii=False) + "\n")
                if record["done"] and record["error"] is None:
                    submission = {"task_id": task_id, "model": model, "status": "success", "answer": record["output"]}
                    temporary = output_dir / "generation.tmp"
                    temporary.write_text(json.dumps(submission, ensure_ascii=False) + "\n", encoding="utf-8")
                    temporary.chmod(0o600)
                    temporary.replace(output_dir / "generation.json")
                    submitted = True
                result = {
                    "content": record["output"]
                    if isinstance(record["output"], list)
                    else [{"type": "text", "text": record["output"]}],
                    "isError": record["error"] is not None,
                }
        else:
            return error(message.id, -32601, f"Method not found: {message.method}")
        return web.json_response({"jsonrpc": "2.0", "id": message.id, "result": result})

    app = web.Application()
    app.router.add_post("/mcp", handle)
    return app
