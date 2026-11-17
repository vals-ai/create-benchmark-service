"""Caller environment for the commands a benchmark runs during setup."""

import asyncio
import json
import os
from collections.abc import AsyncGenerator, Mapping
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from pydantic import JsonValue, ValidationError

from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import (
    ExecResult,
    Sandbox,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxProvider,
    SandboxQuery,
)
from benchmark_service.schemas import SetupTaskRequest, StreamResultChunk
from tests.conftest import StubBenchmark

PROXY_ENV = {
    "HTTPS_PROXY": "http://key:token@127.0.0.1:3128",
    "NO_PROXY": "localhost,127.0.0.1,::1",
    "NODE_EXTRA_CA_CERTS": "/etc/egress/ca.pem",
    "SSL_CERT_FILE": "/etc/egress/ca.pem",
}
PRINT_ENV = "printenv HTTPS_PROXY NO_PROXY NODE_EXTRA_CA_CERTS SSL_CERT_FILE; true"


class ProcessSandbox(Sandbox):
    """Runs commands as real local processes; `exec` gets no caller environment, as on every provider."""

    commands: list[str] = []

    @property
    def id(self) -> str:
        return "i-1"

    @property
    def name(self) -> str:
        return "process"

    @property
    def state(self) -> str:
        return "running"

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        try:
            async for chunk in self.command(command, cwd=cwd, timeout=timeout):
                output.append(chunk)
        except SandboxCommandError as error:
            return ExecResult(exit_code=error.exit_code, output="".join(output))
        return ExecResult(exit_code=0, output="".join(output))

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        self.commands.append(command)
        env = {name: value for name, value in os.environ.items() if name not in PROXY_ENV} | dict(env_vars or {})
        process = await asyncio.create_subprocess_shell(command, stdout=asyncio.subprocess.PIPE, env=env, cwd=cwd)
        stdout, _ = await process.communicate()
        yield stdout.decode()
        if process.returncode:
            raise SandboxCommandError(process.returncode)

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        raise NotImplementedError

    async def download_file(self, remote_path: str) -> bytes:
        raise NotImplementedError


class ProcessProvider(SandboxProvider):
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        raise NotImplementedError

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        return ProcessSandbox()

    async def delete_sandbox(self, instance_id: str) -> None:
        pass

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        if False:
            yield ProcessSandbox()


class SetupBenchmark(StubBenchmark):
    async def setup_task(self, task_id: str, sandbox: Sandbox, dataset: str | None = None):
        executed = await sandbox.exec(PRINT_ENV)
        streamed = "".join([chunk async for chunk in sandbox.command(PRINT_ENV)])
        overridden = "".join(
            [chunk async for chunk in sandbox.command("printenv NO_PROXY", env_vars={"NO_PROXY": "own"})]
        )
        data: dict[str, JsonValue] = {
            "exec": list[JsonValue](executed.output.split()),
            "command": list[JsonValue](streamed.split()),
            "override": overridden.strip(),
        }
        yield StreamResultChunk(type="result", data=data)


def _setup(monkeypatch: pytest.MonkeyPatch, body: dict[str, object]) -> dict[str, object]:
    def create_provider(_config: ModalProviderConfig) -> SandboxProvider:
        return ProcessProvider()

    monkeypatch.setattr(ModalProviderConfig, "create_provider", create_provider)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    ProcessSandbox.commands = []
    request = {
        "task_id": "task-1",
        "instance_id": "i-1",
        "sandbox_provider": {"type": "modal", "MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"},
    } | body
    with TestClient(BenchmarkServiceApp(SetupBenchmark)) as client:
        with client.websocket_connect("/ws/setup-task") as ws:
            ws.send_json(request)
            return ws.receive_json()


def test_setup_commands_receive_the_callers_proxy_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every setup `exec` and `command` process gets the request's `env_vars`, and a command's own `env_vars` win.

    Test cases:
    - The values reach each process through its environment, never the command string.
    """
    result = _setup(monkeypatch, {"env_vars": PROXY_ENV})

    assert result == {
        "type": "result",
        "data": {"exec": list(PROXY_ENV.values()), "command": list(PROXY_ENV.values()), "override": "own"},
    }
    assert not any(value in command for command in ProcessSandbox.commands for value in PROXY_ENV.values())


def test_setup_without_env_vars_keeps_the_providers_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A request without `env_vars` runs setup commands exactly as before."""
    result = _setup(monkeypatch, {})

    assert result == {"type": "result", "data": {"exec": [], "command": [], "override": "own"}}


def test_setup_env_vars_reject_bad_names_without_echoing_values() -> None:
    """Invalid or reserved names fail validation, and the error never contains a value."""
    for name in ("BAD-NAME", "LANG"):
        with pytest.raises(ValidationError) as error:
            SetupTaskRequest(task_id="t", instance_id="i", env_vars={name: "http://key:token@proxy"})
        assert "token" not in str(error.value)
    assert "token" not in repr(SetupTaskRequest(task_id="t", instance_id="i", env_vars=PROXY_ENV))


async def test_client_sends_setup_env_vars_only_when_set() -> None:
    """`setup_task` sends `env_vars` when given and omits the field otherwise, so older services see the same body."""
    sent: list[dict[str, object]] = []
    for env_vars in (PROXY_ENV, None):
        ws = AsyncMock()
        ws.__aenter__.return_value = ws
        ws.__aiter__.return_value = [json.dumps({"type": "result", "data": {"status": "ok"}})]
        ws.close_code = 1000
        client = BenchmarkServiceClient(url="http://service", headers={}, timeout=10)
        with patch("benchmark_service.client._NoRedirectConnect", new=AsyncMock(return_value=ws)):
            await client.setup_task("task-1", "i-1", env_vars=env_vars)
        sent.append(json.loads(ws.send.call_args.args[0]))

    assert sent[0]["env_vars"] == PROXY_ENV
    assert "env_vars" not in sent[1]
