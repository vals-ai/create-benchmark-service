"""Tests for the tools and skills a task gives to the agent."""

from typing import Any

import pytest
from pydantic import ValidationError

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
    assert RetrieveTaskResponse.model_validate(data) == response


def test_http_means_streamable_http() -> None:
    server = MCPServer.model_validate({"name": "s", "transport": "http", "url": "http://s/mcp"})
    assert server.transport == "streamable-http"


def test_a_later_server_replaces_an_earlier_one_of_the_same_name() -> None:
    response = task_response(
        mcp_servers=[
            MCPServer(name="a", url="http://old/sse"),
            MCPServer(name="b", url="http://b/sse"),
            MCPServer(name="a", url="http://new/sse"),
        ]
    )
    assert [(server.name, server.url) for server in response.mcp_servers] == [
        ("a", "http://new/sse"),
        ("b", "http://b/sse"),
    ]


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"name": "s", "transport": "stdio"}, "needs a command"),
        ({"name": "s", "transport": "sse"}, "needs a url"),
        ({"name": "s", "transport": "streamable-http", "command": "x"}, "needs a url"),
        ({"name": "s", "transport": "stdio", "command": "x", "headers": {"A": "b"}}, "headers are only for"),
        ({"name": "", "url": "http://s/sse"}, "at least 1 character"),
        ({"name": "s", "transport": "websocket", "url": "ws://s"}, "Input should be"),
    ],
)
def test_bad_mcp_server_is_refused(fields: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        MCPServer.model_validate(fields)


def test_skills_dir_must_be_absolute() -> None:
    with pytest.raises(ValidationError, match="absolute sandbox path"):
        task_response(skills_dir="skills")
