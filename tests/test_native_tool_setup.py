import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from benchmark_service.sandbox.types import ExecResult
from benchmark_service.sandbox.types import Sandbox as BaseSandbox
from benchmark_service.schemas import ChildEgress
from benchmark_service.tool_runtime.setup import (
    NATIVE_TOOLS,
    ToolSetup,
    mcp_tool_egress,
    setup_native_tools,
    stop_native_tools,
)


class Sandbox:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.commands: list[str] = []
        self.egress: list[list[str]] = []

    async def modify_egress_rules(self, addresses: list[str]) -> None:
        self.egress.append(addresses)

    async def upload_file(self, path: str, content: bytes) -> None:
        self.files[path] = content

    async def exec(self, command: str) -> ExecResult:
        self.commands.append(command)
        return ExecResult(exit_code=0)


@pytest.fixture
def settings() -> ToolSetup:
    return ToolSetup(
        entrypoint=NATIVE_TOOLS,
        workspace="/workspace/agent",
        native_config={
            "benchmark": "fabv2-internal",
            "model": "openai/example-max",
            "task_id": "task-1",
            "as_of_date": "2026-03-01",
            "workbook_path": "/workspace/submission.xlsx",
            "output_dir": "/app/results/valkyrie/task-1",
            "question": "Find the historical price.",
            "credentials": {},
            "port": 8765,
        },
        python_path="/app/.venv/bin/python",
        protected_dirs=["/app"],
        credential_names=["TAVILY_API_KEY", "SEC_EDGAR_API_KEY", "PRICING_DATA_API_KEY"],
        codex_config={},
        tools_uid=10002,
        tools_gid=10002,
        egress=None,
    )


def test_mcp_policy_separates_agent_and_native_tools() -> None:
    policy = mcp_tool_egress("openai/example-max")
    assert policy.setup_task == []
    assert policy.run == []
    assert policy.evaluation == []
    assert policy.run_through_proxy
    assert policy.children["tools"].egress == "*"
    assert policy.children["tools"].models == ["openai/example-max"]


@pytest.mark.asyncio
async def test_setup_stages_trusted_code_and_private_role_configuration(settings: ToolSetup) -> None:
    sandbox = Sandbox()
    grant = ChildEgress(
        proxy_url="https://profile:proxy-secret@proxy.example:443",
        proxy_cidrs=["203.0.113.1/32"],
        ca_certificate="test-ca",
        gateway_url="https://gateway.example",
        gateway_token="mgwt_tools-secret",
    )
    await setup_native_tools(cast(BaseSandbox, sandbox), replace(settings, egress=grant))
    prefix = "/run/benchmark-tools"
    assert {"native_tools.py", "native_tool_mcp.py", "runtime.py", "egress_tunnel.py"} <= {
        Path(path).name for path in sandbox.files
    }
    config = json.loads(sandbox.files[f"{prefix}/runtime.json"])
    assert config["native"] == settings.native_config
    assert config["codex_config"] == {}
    assert config["credential_names"] == settings.credential_names
    assert config["tool_environment"]["MODEL_GATEWAY_API_KEY"] == "mgwt_tools-secret"
    assert config["tool_environment"]["HTTPS_PROXY"] == "http://profile:proxy-secret@127.0.0.1:8766"
    assert config["tool_environment"]["NO_PROXY"] == ""
    assert config["tool_environment"]["no_proxy"] == ""
    assert config["protected_dirs"] == ["/app"]
    assert config["workspace"] == "/workspace/agent"
    assert config["agent_uid"] != config["tools_uid"]
    assert sandbox.egress == [["203.0.113.1/32"]]
    assert sandbox.commands[-1] == f"python3 -I {prefix}/runtime.py start {prefix}/runtime.json"
    assert "proxy-secret" not in "\n".join(sandbox.commands)
    assert "mgwt_tools-secret" not in "\n".join(sandbox.commands)


@pytest.mark.asyncio
async def test_local_tools_start_without_a_proxy_grant_or_credentials(settings: ToolSetup) -> None:
    sandbox = Sandbox()
    await setup_native_tools(
        cast(BaseSandbox, sandbox), replace(settings, credential_names=[], tools_uid=0, tools_gid=0)
    )
    config = json.loads(sandbox.files["/run/benchmark-tools/runtime.json"])
    assert config["tools_uid"] == 0
    assert config["tools_gid"] == 0
    assert config["tool_environment"] == {}
    assert config["credential_names"] == []
    assert sandbox.egress == []
    assert not any("egress_tunnel.py" in command for command in sandbox.commands)


@pytest.mark.asyncio
async def test_custom_service_entrypoint_is_staged_from_trusted_source(settings: ToolSetup, tmp_path: Path) -> None:
    entrypoint = tmp_path / "cleanroom.py"
    entrypoint.write_text("print('service-owned tools')\n")
    sandbox = Sandbox()
    await setup_native_tools(cast(BaseSandbox, sandbox), replace(settings, entrypoint=entrypoint))
    assert sandbox.files["/run/benchmark-tools/native_tools.py"] == entrypoint.read_bytes()


@pytest.mark.asyncio
async def test_stop_finishes_before_grading() -> None:
    sandbox = Sandbox()
    await stop_native_tools(cast(BaseSandbox, sandbox))
    assert sandbox.commands == ["python3 -I /run/benchmark-tools/runtime.py stop /run/benchmark-tools/runtime.json"]
