"""Keep the model's shell separate from trusted benchmark tools."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import ProxyHandler, Request, build_opener


@dataclass(frozen=True)
class RuntimeConfig:
    root: str
    workspace: str
    protected_dirs: list[str]
    agent_uid: int
    agent_gid: int
    tools_uid: int
    tools_gid: int
    python_path: str
    native: dict[str, Any]
    credential_names: list[str]
    tool_environment: dict[str, str]
    codex_config: dict[str, Any]


_AGENT_ENVIRONMENT = {
    "PATH",
    "LANG",
    "LC_ALL",
    "TERM",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "MODEL_GATEWAY_URL",
    "MODEL_GATEWAY_API_KEY",
}


def _write(path: Path, content: str, uid: int, gid: int, mode: int) -> None:
    path.unlink(missing_ok=True)
    path.write_text(content)
    os.chown(path, uid, gid)
    path.chmod(mode)


def _drop_privileges(uid: int, gid: int) -> list[str]:
    return [
        "setpriv",
        f"--reuid={uid}",
        f"--regid={gid}",
        "--clear-groups",
        "--bounding-set=-all",
        "--inh-caps=-all",
        "--ambient-caps=-all",
        "--no-new-privs",
        "--",
    ]


def start(config: RuntimeConfig, config_path: Path) -> dict[str, int]:
    assert os.geteuid() == 0, "The benchmark runtime needs root during trusted setup"
    assert 0 < config.agent_uid != config.tools_uid > 0, "Agent and tools UIDs must be positive and distinct"
    assert config.agent_gid > 0 and config.agent_gid != config.tools_gid
    assert shutil.which("setpriv"), "Install util-linux before trusted setup"
    root = Path(config.root)
    root.mkdir(parents=True, exist_ok=True)
    os.chown(root, 0, config.tools_gid)
    root.chmod(0o750)
    os.chown(config_path, 0, 0)
    config_path.chmod(0o600)
    for source in root.glob("*.py"):
        os.chown(source, 0, 0)
        source.chmod(0o555)
    agent_root = root.with_name("benchmark-agent")
    agent_root.mkdir(exist_ok=True)
    os.chown(agent_root, 0, 0)
    agent_root.chmod(0o755)
    agent_home = agent_root / "home"
    tools_home = root / "home"
    for home, uid, gid in (
        (agent_home, config.agent_uid, config.agent_gid),
        (tools_home, config.tools_uid, config.tools_gid),
    ):
        home.mkdir(mode=0o700)
        os.chown(home, uid, gid)
        for name in ("tmp", ".codex", ".codex/sessions"):
            directory = home / name
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.chown(directory, uid, gid)
    workspace = Path(config.workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    os.chown(workspace, config.agent_uid, config.tools_gid)
    workspace.chmod(0o2770)
    for protected in config.protected_dirs:
        os.chown(protected, 0, config.tools_gid)
        Path(protected).chmod(0o750)
    output = Path(config.native["output_dir"])
    output.mkdir(parents=True, exist_ok=True)
    os.chown(output, config.tools_uid, config.tools_gid)
    output.chmod(0o700)
    credentials = {name: os.environ[name] for name in config.credential_names}
    credentials.update(config.tool_environment)
    _write(root / "native.json", json.dumps({**config.native, "credentials": credentials}), 0, config.tools_gid, 0o640)
    tools_environment = {
        "PATH": os.environ["PATH"],
        "HOME": str(tools_home),
        "TMPDIR": str(tools_home / "tmp"),
        **config.tool_environment,
    }
    with (root / "broker.log").open("w") as log:
        broker = subprocess.Popen(
            [
                *_drop_privileges(config.tools_uid, config.tools_gid),
                config.python_path,
                "-I",
                str(root / "native_tools.py"),
                str(root / "native.json"),
            ],
            cwd=workspace,
            env=tools_environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    request = Request(
        f"http://127.0.0.1:{config.native['port']}/mcp",
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    connection = build_opener(ProxyHandler({}))
    while True:
        if broker.poll() is not None:
            raise RuntimeError("Native tools exited during setup; inspect the private broker log")
        try:
            with connection.open(request, timeout=1) as response:
                json.load(response)["result"]["tools"]
            break
        except (URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    _write(agent_root / "prompt.txt", (output / "prompt.txt").read_text(), 0, 0, 0o444)
    root_codex = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    root_codex.mkdir(parents=True, exist_ok=True)
    original_config = root_codex / "config.toml"
    original_text = original_config.read_text() if original_config.is_file() else ""
    _write(agent_home / ".codex/config.toml", original_text, config.agent_uid, config.agent_gid, 0o600)
    original_settings = tomllib.loads(original_text)
    overrides = {
        f"projects.{json.dumps(str(workspace))}.trust_level": "trusted",
        "mcp_servers.vals_benchmark.url": f"http://127.0.0.1:{config.native['port']}/mcp",
        "mcp_servers.vals_benchmark.required": True,
        **config.codex_config,
    }
    instructions = (output / "instructions.txt").read_text()
    if instructions:
        overrides["developer_instructions"] = "\n\n".join(
            value for value in (original_settings.get("developer_instructions", ""), instructions) if value
        )
    sessions = root_codex / "sessions"
    if sessions.exists():
        sessions.rmdir()
    sessions.symlink_to(agent_home / ".codex/sessions", target_is_directory=True)
    codex = Path(shutil.which("codex") or "")
    assert codex.is_file(), "Install Codex before trusted task setup"
    executable = codex.resolve()
    if executable == codex:
        executable = codex.with_name("codex-native")
        codex.rename(executable)
    state = {"broker_pid": broker.pid, "codex": str(executable), "overrides": overrides}
    _write(root / "state.json", json.dumps(state), 0, 0, 0o600)
    shim = (
        f"#!{sys.executable} -I\nimport os, sys\n"
        f"os.execv({sys.executable!r}, [{sys.executable!r}, '-I', {str(Path(__file__).resolve())!r}, "
        f"'agent', {str(config_path)!r}, *sys.argv[1:]])\n"
    )
    _write(codex, shim, 0, 0, 0o755)
    return {"broker_pid": broker.pid}


def agent(config: RuntimeConfig, arguments: list[str]) -> None:
    assert os.geteuid() == 0, "The root launcher owns the Codex identity boundary"
    root = Path(config.root)
    home = root.with_name("benchmark-agent") / "home"
    state = json.loads((root / "state.json").read_text())
    if arguments[0] == "exec":
        _write(root / "generation_started_at", str(time.monotonic()), 0, config.tools_gid, 0o440)
        overrides = [item for key, value in state["overrides"].items() for item in ("-c", f"{key}={json.dumps(value)}")]
        index = arguments.index("--") if "--" in arguments else len(arguments)
        arguments[index:index] = overrides
    environment = {name: value for name, value in os.environ.items() if name in _AGENT_ENVIRONMENT}
    environment.update({"HOME": str(home), "CODEX_HOME": str(home / ".codex"), "TMPDIR": str(home / "tmp")})
    os.umask(0o002)
    os.chdir(config.workspace)
    command = [*_drop_privileges(config.agent_uid, config.agent_gid), state["codex"], *arguments]
    os.execvpe(command[0], command, environment)


def stop(config: RuntimeConfig) -> None:
    assert os.geteuid() == 0, "The benchmark service owns process cleanup"
    assert 0 < config.agent_uid != config.tools_uid > 0, "Agent and tools UIDs must be positive and distinct"
    for uid in (config.agent_uid, config.tools_uid):
        deadline = time.monotonic() + 5
        while True:
            active: list[int] = []
            for entry in Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    status = (entry / "status").read_text()
                except (FileNotFoundError, ProcessLookupError):
                    continue
                fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
                if int(fields["Uid"].split()[1]) == uid and fields["State"].strip()[0] != "Z":
                    active.append(int(entry.name))
            if not active:
                break
            for signum in (signal.SIGSTOP, signal.SIGKILL):
                for pid in active:
                    try:
                        os.kill(pid, signum)
                    except ProcessLookupError:
                        pass
            if time.monotonic() >= deadline:
                raise TimeoutError("Benchmark processes did not stop before grading")
            time.sleep(0.01)
    source = Path(config.native["output_dir"]) / "tool_calls.jsonl"
    if source.is_file():
        destination = Path("/logs/benchmark_tools/tool_calls.jsonl")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        destination.chmod(0o600)


if __name__ == "__main__":
    action, filename, *arguments = sys.argv[1:]
    path = Path(filename)
    config = RuntimeConfig(**json.loads(path.read_text()))
    match action:
        case "start":
            print(json.dumps(start(config, path)), flush=True)
        case "agent":
            agent(config, arguments)
        case "stop":
            stop(config)
        case _:
            raise ValueError(f"Unknown benchmark runtime action: {action}")
