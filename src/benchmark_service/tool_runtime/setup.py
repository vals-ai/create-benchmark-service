"""Stage trusted benchmark tools in the task sandbox."""

import json
import shlex
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from benchmark_service.sandbox import ImageSource, Sandbox, SnapshotSource
from benchmark_service.schemas import BenchmarkEgressPlan, ChildEgress, ChildPolicy

ROOT = "/run/benchmark-tools"
NATIVE_TOOLS = Path(__file__).with_name("native_tools.py")


@dataclass(frozen=True)
class ToolSetup:
    entrypoint: Path
    workspace: str
    native_config: dict[str, Any]
    python_path: str
    protected_dirs: list[str]
    credential_names: list[str]
    codex_config: dict[str, object]
    tools_uid: int
    tools_gid: int
    egress: ChildEgress | None


def mcp_tool_egress(model: str) -> BenchmarkEgressPlan:
    """Give the agent no public web access and grant it to trusted tools."""
    return BenchmarkEgressPlan(
        setup_task=[],
        run=[],
        evaluation=[],
        run_through_proxy=True,
        children={"tools": ChildPolicy(egress="*", models=[model])},
    )


def mcp_tool_source(source: str) -> ImageSource | SnapshotSource:
    if source.startswith("snapshot:"):
        return SnapshotSource(snapshot=source.removeprefix("snapshot:"))
    return ImageSource(image=source)


async def setup_native_tools(sandbox: Sandbox, settings: ToolSetup) -> None:
    source = files("benchmark_service")
    grant = settings.egress
    environment = {}
    if grant is not None:
        await sandbox.modify_egress_rules(grant.proxy_cidrs)
        environment = grant.environment(tunnel_port=8766) | {"NO_PROXY": "", "no_proxy": ""}
    result = await sandbox.exec(f"mkdir -p {ROOT}")
    assert result.exit_code == 0, "Cannot create the benchmark tool directory"
    await sandbox.upload_file(f"{ROOT}/native_tools.py", settings.entrypoint.read_bytes())
    for name in ("native_tool_mcp.py", "runtime.py"):
        await sandbox.upload_file(f"{ROOT}/{name}", source.joinpath("tool_runtime", name).read_bytes())
    config = {
        "root": ROOT,
        "workspace": settings.workspace,
        "protected_dirs": settings.protected_dirs,
        "agent_uid": 10001,
        "agent_gid": 10001,
        "tools_uid": settings.tools_uid,
        "tools_gid": settings.tools_gid,
        "python_path": settings.python_path,
        "credential_names": settings.credential_names,
        "tool_environment": environment,
        "codex_config": settings.codex_config,
        "native": settings.native_config,
    }
    await sandbox.upload_file(f"{ROOT}/runtime.json", json.dumps(config).encode())
    result = await sandbox.exec(f"chmod 600 {ROOT}/runtime.json")
    assert result.exit_code == 0, "Cannot protect benchmark tool configuration"
    if grant is not None:
        await sandbox.upload_file(f"{ROOT}/egress_tunnel.py", source.joinpath("egress_tunnel.py").read_bytes())
        await sandbox.upload_file(f"{ROOT}/proxy-ca.pem", grant.ca_certificate.encode())
        command = grant.tunnel_command(f"{ROOT}/egress_tunnel.py", f"{ROOT}/proxy-ca.pem", 8766)
        result = await sandbox.exec(shlex.join(command))
        assert result.exit_code == 0, "Cannot start the benchmark tool proxy"
    result = await sandbox.exec(f"python3 -I {ROOT}/runtime.py start {ROOT}/runtime.json")
    assert result.exit_code == 0, "Cannot start the benchmark tools"


async def stop_native_tools(sandbox: Sandbox) -> None:
    result = await sandbox.exec(f"python3 -I {ROOT}/runtime.py stop {ROOT}/runtime.json")
    assert result.exit_code == 0, "Cannot stop benchmark processes before grading"
