"""Tests for the tools and skills a task gives to the agent."""

import tomllib
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import Field, ValidationError

from benchmark_service import MCPServer
from benchmark_service.sandbox.types import ImageSource, Resources
from benchmark_service.schemas import RetrieveTaskResponse


def task_response(**fields: Any) -> RetrieveTaskResponse:
    return RetrieveTaskResponse(
        source=ImageSource(image="python:3.12-slim"),
        problem_path="/tmp/problem_statement.txt",
        cwd="/workspace",
        resources=Resources(vcpu=1, memory=1, disk=1),
        **fields,
    )


def test_a_task_without_tools_keeps_the_wire_shape() -> None:
    data = task_response().model_dump(mode="json")
    assert "mcp_servers" not in data
    assert "skills_dir" not in data
    assert "task_dir" not in data


def test_tools_and_skills_survive_the_wire() -> None:
    response = task_response(
        mcp_servers=[
            MCPServer(name="supabase", transport="streamable-http", url="http://10.0.0.17:8082/mcp"),
            MCPServer(
                name="search", transport="stdio", command="web-search-mcp", args=["--max-end-date", "2026-03-31"]
            ),
            MCPServer(name="docs", url="http://docs:3080/sse", headers={"Authorization": "Bearer ${DOCS_TOKEN}"}),
        ],
        skills_dir="/opt/skills",
        task_dir="/opt/task",
    )
    data = response.model_dump(mode="json")
    assert data["mcp_servers"] == [
        {
            "name": "supabase",
            "transport": "streamable-http",
            "url": "http://10.0.0.17:8082/mcp",
            "command": None,
            "args": [],
        },
        {
            "name": "search",
            "transport": "stdio",
            "url": None,
            "command": "web-search-mcp",
            "args": ["--max-end-date", "2026-03-31"],
        },
        {
            "name": "docs",
            "transport": "sse",
            "url": "http://docs:3080/sse",
            "command": None,
            "args": [],
            "headers": {"Authorization": "Bearer ${DOCS_TOKEN}"},
        },
    ]
    assert data["skills_dir"] == "/opt/skills"
    assert data["task_dir"] == "/opt/task"
    assert RetrieveTaskResponse.model_validate(data) == response


def test_http_means_streamable_http() -> None:
    server = MCPServer.model_validate({"name": "s", "transport": "http", "url": "http://s/mcp"})
    assert server.transport == "streamable-http"


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"name": "s", "transport": "stdio"}, "needs a command"),
        ({"name": "s", "transport": "sse"}, "needs a url"),
        ({"name": "s", "transport": "streamable-http", "command": "x"}, "needs a url"),
        ({"name": "s", "transport": "stdio", "command": "x", "headers": {"A": "b"}}, "headers are only for"),
        ({"name": "s", "transport": "websocket", "url": "ws://s"}, "Input should be"),
    ],
)
def test_bad_mcp_server_is_refused(fields: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        MCPServer.model_validate(fields)


def test_a_harbor_task_toml_entry_is_a_valid_server() -> None:
    # The form of `[[environment.mcp_servers]]` in a Harbor task.toml, read with tomllib.
    entries = tomllib.loads(
        """
        [[environment.mcp_servers]]
        name = "playwright"
        transport = "sse"
        url = "http://playwright-mcp:3080/sse"

        [[environment.mcp_servers]]
        name = "courtlistener"
        transport = "http"
        url = "http://courtlistener-mcp:3080/mcp"

        [[environment.mcp_servers]]
        name = "web-search"
        transport = "stdio"
        command = "web-search-mcp"
        args = ["--max-end-date", "2026-03-31"]
        """
    )["environment"]["mcp_servers"]
    servers = task_response(mcp_servers=entries).mcp_servers
    assert [(server.name, server.transport) for server in servers] == [
        ("playwright", "sse"),
        ("courtlistener", "streamable-http"),
        ("web-search", "stdio"),
    ]


def test_a_subclass_can_declare_the_fields_with_its_own_types() -> None:
    # The Tracker's response for a Harbor dataset task does this.
    class OwnShape(RetrieveTaskResponse):
        mcp_servers: dict[str, dict[str, Any]] = Field(default_factory=dict)  # pyright: ignore[reportIncompatibleVariableOverride]
        skills_dir: str | None = None

    response = OwnShape(
        source=ImageSource(image="python:3.12-slim"),
        problem_path="/app/instruction.md",
        cwd="/app",
        resources=Resources(vcpu=1, memory=1, disk=1),
        mcp_servers={"playwright": {"type": "sse", "url": "http://playwright-mcp:3080/sse"}},
        skills_dir="skills",
    )
    assert response.mcp_servers == {"playwright": {"type": "sse", "url": "http://playwright-mcp:3080/sse"}}
    assert response.skills_dir == "skills"
    assert OwnShape.model_validate_json(response.model_dump_json()) == response


def test_a_legacy_response_reads_as_a_task_without_tools() -> None:
    response = RetrieveTaskResponse.model_validate(
        {
            "docker_image": "python:3.12-slim",
            "problem_path": "/tmp/problem_statement.txt",
            "cwd": "/workspace",
            "resources": {"vcpu": 1, "memory": 1, "disk": 1},
        }
    )
    assert response.mcp_servers == []
    assert response.skills_dir is None
    assert response.task_dir is None


def test_the_service_response_has_no_tool_fields_when_unset(client: TestClient) -> None:
    data = client.get("/retrieve-task/", params={"task_id": "task-1"}).json()
    assert "mcp_servers" not in data
    assert "skills_dir" not in data
    assert "task_dir" not in data
