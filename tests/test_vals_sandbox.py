"""Tests for the Vals Sandbox provider (Lambda MicroVM backend) against the real in-VM agent on localhost.

Run: uv run pytest tests/test_vals_sandbox.py
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import threading
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx
import pytest

from benchmark_service.sandbox import (
    DockerfileSource,
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    UnsupportedSandboxConfigError,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.sandbox.vals_sandbox import (
    ValsSandboxProviderConfig,
    ValsSandbox,
    ValsSandboxProvider,
)
from benchmark_service.sandbox.vals_sandbox import agent
from benchmark_service.sandbox.vals_sandbox.egress_proxy import EgressPolicy, EgressProxy
from benchmark_service.sandbox.vals_sandbox.egress_proxy import serve as egress_serve
from benchmark_service.sandbox.vals_sandbox.image import (
    AGENT_FILE,
    baseline_memory_mib,
    build_artifact,
    ensure_image,
)

IMAGE_ARN = "arn:aws:lambda:us-east-1:123456789012:microvm-image:test"


class NotFound(Exception):
    response = {"Error": {"Code": "ResourceNotFoundException"}}


class FakeMicrovms:
    """Stands in for Lambda: runs the hook the way Lambda does and routes the endpoint to the local agent."""

    def __init__(self, app_port: int, hook_port: int, *, fail_start: bool = False) -> None:
        self.app_port = app_port
        self.hook_port = hook_port
        self.fail_start = fail_start
        self.vms: dict[str, dict[str, Any]] = {}
        self.run_calls: list[dict[str, Any]] = []
        self.terminated: list[str] = []

    def run_microvm(self, **kwargs: Any) -> dict[str, Any]:
        self.run_calls.append(kwargs)
        vm_id = f"mvm-{len(self.vms) + 1}"
        if not self.fail_start:
            httpx.post(
                f"http://127.0.0.1:{self.hook_port}{agent.HOOK_PREFIX}run",
                json={"microvmId": vm_id, "runHookPayload": kwargs["runHookPayload"]},
            ).raise_for_status()
        self.vms[vm_id] = {
            "microvmId": vm_id,
            "state": "PENDING",
            "endpoint": f"http://127.0.0.1:{self.app_port}",
            "imageArn": kwargs["imageIdentifier"],
            "imageVersion": "1",
            "startedAt": datetime.now(UTC),
        }
        return dict(self.vms[vm_id])

    def get_microvm(self, microvmIdentifier: str) -> dict[str, Any]:
        if microvmIdentifier not in self.vms:
            raise NotFound()
        vm = self.vms[microvmIdentifier]
        if vm["state"] == "PENDING":
            vm["state"] = "TERMINATED" if self.fail_start else "RUNNING"
        return dict(vm)

    def create_microvm_auth_token(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["allowedPorts"] == [{"port": agent.APP_PORT}]
        return {"authToken": {"X-aws-proxy-auth": "token"}}

    def terminate_microvm(self, microvmIdentifier: str) -> dict[str, Any]:
        if microvmIdentifier not in self.vms:
            raise NotFound()
        self.terminated.append(microvmIdentifier)
        self.vms[microvmIdentifier]["state"] = "TERMINATED"
        return {}

    def list_microvms(self, **_kwargs: Any) -> dict[str, Any]:
        return {"items": [dict(vm) for vm in self.vms.values()]}


@pytest.fixture
def agent_ports() -> Iterator[tuple[int, int]]:
    app, hooks = agent.serve(0, 0)
    yield app.server_address[1], hooks.server_address[1]
    for server in (app, hooks):
        server.shutdown()
        server.server_close()


def _request(**overrides: Any) -> SandboxCreateRequest:
    values: dict[str, Any] = {
        "source": ImageSource(image=IMAGE_ARN),
        "resources": Resources(vcpu=2, memory=4, disk=8),
        "name": "task-1",
        "labels": {"run-id": "r1"},
        "env_vars": {"GREETING": "hello"},
        "auto_stop_interval": 15,
        "create_timeout": 10,
    }
    values.update(overrides)
    return SandboxCreateRequest(**values)


def _provider(fake: FakeMicrovms) -> ValsSandboxProvider:
    return ValsSandboxProvider(ValsSandboxProviderConfig(VALS_SANDBOX_IMAGE=IMAGE_ARN), microvms=fake)


async def test_sandbox_lifecycle_runs_commands_and_moves_files(agent_ports: tuple[int, int], tmp_path: Path) -> None:
    """Verify create, exec, file transfer, list, and terminate through the real agent.

    Test cases:
    - The run-hook payload carries name, labels, and env; idle and egress settings reach RunMicrovm.
    - Exit codes, combined stdout/stderr, cwd, per-command env, and timeouts (124) come back.
    - Uploaded bytes download unchanged; a missing file is an error.
    - Listing filters by labels; deleting terminates the MicroVM.
    """
    fake = FakeMicrovms(*agent_ports)
    async with _provider(fake) as provider:
        sandbox = await provider.create_sandbox(_request())
        assert isinstance(sandbox, ValsSandbox)
        assert (sandbox.id, sandbox.name, sandbox.state) == ("mvm-1", "task-1", "running")
        assert sandbox.labels == {"run-id": "r1"}
        call = fake.run_calls[0]
        assert call["idlePolicy"]["maxIdleDurationSeconds"] == 900
        assert call["egressNetworkConnectors"][0].endswith(":INTERNET_EGRESS")

        result = await sandbox.exec("echo $GREETING; echo oops >&2; exit 3")
        assert (result.exit_code, result.output) == (3, "hello\noops\n")
        result = await sandbox.exec("pwd", cwd=str(tmp_path))
        assert result.output.strip() == str(tmp_path)
        chunks = [chunk async for chunk in sandbox.command("echo $EXTRA", env_vars={"EXTRA": "x"})]
        assert "".join(chunks) == "x\n"
        assert (await sandbox.exec("sleep 5", timeout=0.5)).exit_code == agent.TIMEOUT_EXIT_CODE
        closed = await sandbox.exec("echo early; exec >&- 2>&-; sleep 5", timeout=0.5)
        assert (closed.exit_code, closed.output) == (agent.TIMEOUT_EXIT_CODE, "early\n")

        target = tmp_path / "nested" / "data.bin"
        await sandbox.upload_file(str(target), b"\x00\x01payload")
        assert await sandbox.download_file(str(target)) == b"\x00\x01payload"
        with pytest.raises(SandboxError) as missing:
            await sandbox.download_file(str(tmp_path / "missing"))
        assert not isinstance(missing.value, SandboxNotFoundError)

        assert [s.id async for s in provider.list_sandboxes(SandboxQuery(labels={"run-id": "r1"}))] == ["mvm-1"]
        assert [s.id async for s in provider.list_sandboxes(SandboxQuery(labels={"run-id": "other"}))] == []
        past = datetime.now(UTC) - timedelta(hours=1)
        assert [s.id async for s in provider.list_sandboxes(SandboxQuery(labels={}, created_at_lte=past))] == []

        await provider.delete_sandbox(sandbox.id)
        assert fake.terminated == ["mvm-1"]
        assert (await provider.get_sandbox(sandbox.id)).has_ended


async def test_network_block_all_starts_without_egress(agent_ports: tuple[int, int]) -> None:
    fake = FakeMicrovms(*agent_ports)
    async with _provider(fake) as provider:
        await provider.create_sandbox(_request(network_block_all=True))
    assert fake.run_calls[0]["egressNetworkConnectors"] == []


async def test_failed_start_terminates_the_microvm(agent_ports: tuple[int, int]) -> None:
    fake = FakeMicrovms(*agent_ports, fail_start=True)
    async with _provider(fake) as provider:
        with pytest.raises(SandboxError, match="ended while starting"):
            await provider.create_sandbox(_request())
    assert fake.terminated == ["mvm-1"]


@pytest.mark.parametrize(
    ("nested", "resources"),
    [
        (True, Resources(vcpu=1, memory=1, disk=1)),
        (False, Resources(vcpu=1, memory=1, disk=1, gpu=1)),
        (False, Resources(vcpu=1, memory=64, disk=1)),
        (False, Resources(vcpu=1, memory=1, disk=64)),
    ],
)
def test_unsupported_configs_are_rejected_before_provisioning(nested: bool, resources: Resources) -> None:
    provider = ValsSandboxProviderConfig(VALS_SANDBOX_IMAGE=IMAGE_ARN).create_provider()
    with pytest.raises(UnsupportedSandboxConfigError):
        provider.validate_sandbox_config(requires_nested_docker=nested, resources=resources)


def test_config_decodes_from_provider_mapping() -> None:
    config = sandbox_provider_config_from_mapping({"type": "vals_sandbox", "VALS_SANDBOX_IMAGE": IMAGE_ARN})
    assert isinstance(config, ValsSandboxProviderConfig)


def test_baseline_memory_is_smallest_size_whose_peak_fits() -> None:
    assert baseline_memory_mib(1, 2) == 512
    assert baseline_memory_mib(2, 4) == 1024
    assert baseline_memory_mib(8, 4) == 4096
    with pytest.raises(SandboxError):
        baseline_memory_mib(32, 4)


def test_build_artifact_wraps_dockerfile_with_agent(tmp_path: Path) -> None:
    """Verify the uploaded build context.

    Test cases:
    - The artifact is deterministic and holds the context, the wrapped Dockerfile, and the agent.
    - The agent replaces any inherited ENTRYPOINT, and Python is installed when the image lacks it.
    - Executable context files keep their mode; symlinks leaving the context are not uploaded.
    """
    context = tmp_path / "context"
    context.mkdir()
    (context / "run.sh").write_text("#!/bin/sh\n")
    (context / "run.sh").chmod(0o755)
    (tmp_path / "secret").write_text("outside")
    (context / "leak").symlink_to(tmp_path / "secret")
    artifact = build_artifact("FROM python:3.12-slim\n", context)
    assert artifact == build_artifact("FROM python:3.12-slim\n", context)
    with zipfile.ZipFile(BytesIO(artifact)) as archive:
        assert sorted(archive.namelist()) == sorted(["Dockerfile", AGENT_FILE, "run.sh"])
        assert (archive.getinfo("run.sh").external_attr >> 16) & 0o777 == 0o755
        dockerfile = archive.read("Dockerfile").decode()
    lines = dockerfile.splitlines()
    assert lines[0] == "FROM python:3.12-slim"
    assert lines[1].startswith("RUN command -v python3")
    assert lines[-2:] == ["ENTRYPOINT []", f'CMD ["python3", "/opt/cbs/{AGENT_FILE}"]']


class FakeImages:
    region_name = "us-east-1"

    def __init__(self, existing: bool) -> None:
        self.meta = self
        self.existing = existing
        self.created: list[dict[str, Any]] = []

    def get_microvm_image(self, imageIdentifier: str) -> dict[str, Any]:
        if not self.existing and not self.created:
            raise NotFound()
        return {"imageArn": f"arn:image/{imageIdentifier}", "state": "CREATED", "latestActiveImageVersion": "1"}

    def create_microvm_image(self, **kwargs: Any) -> dict[str, Any]:
        self.created.append(kwargs)
        return {}


class FakeS3:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def put_object(self, **kwargs: Any) -> None:
        self.keys.append(kwargs["Key"])


@pytest.mark.parametrize("existing", [True, False])
def test_ensure_image_reuses_or_builds(existing: bool) -> None:
    images, s3 = FakeImages(existing), FakeS3()
    arn = ensure_image(
        images,
        s3,
        dockerfile="FROM python:3.12-slim\n",
        context_dir=None,
        memory_mib=1024,
        bucket="bucket",
        build_role_arn="arn:aws:iam::123456789012:role/build",
    )
    assert arn.startswith("arn:image/cbs-")
    assert len(images.created) == (0 if existing else 1)
    if not existing:
        created = images.created[0]
        assert created["resources"] == [{"minimumMemoryInMiB": 1024}]
        assert created["hooks"]["port"] == agent.HOOK_PORT
        assert created["tags"]["Purpose"] == "microvm-sandbox-mvp"
        assert s3.keys == [f"cbs-microvm-images/{created['name']}.zip"]


async def test_dockerfile_source_without_build_config_is_rejected(agent_ports: tuple[int, int], tmp_path: Path) -> None:
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM python:3.12-slim\n")
    async with _provider(FakeMicrovms(*agent_ports)) as provider:
        with pytest.raises(MissingSandboxConfigError, match="VALS_SANDBOX_BUILD_BUCKET"):
            await provider.create_sandbox(_request(source=DockerfileSource(dockerfile=str(dockerfile))))


def test_agent_run_hook_rejects_unknown_hooks(agent_ports: tuple[int, int]) -> None:
    _, hook_port = agent_ports
    response = httpx.post(f"http://127.0.0.1:{hook_port}{agent.HOOK_PREFIX}bogus", content=json.dumps({}))
    assert response.status_code == 404


class _Target(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = b"reached"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


async def test_egress_goes_through_the_proxy_and_follows_each_sandboxes_policy(agent_ports: tuple[int, int]) -> None:
    """Verify proxy-enforced egress end to end through the agent, the provider, and the proxy.

    Test cases:
    - A proxied sandbox runs on the VPC egress connector, and its commands get proxy credentials.
    - Allow-all still refuses private addresses, even for a hostname that resolves to one.
    - A CIDR rule allows the address, a deny-all blocks it again, and deleting drops the policy.
    """
    target = ThreadingHTTPServer(("127.0.0.1", 0), _Target)
    threading.Thread(target=target.serve_forever, daemon=True).start()

    async def resolve(host: str, port: int) -> list[str]:
        return ["127.0.0.1"] if host == "target.test" else []

    proxy = EgressProxy("admin-token", resolve=resolve)
    proxy_server, admin_server = await egress_serve(proxy, host="127.0.0.1", proxy_port=0, admin_port=0)
    proxy_port = proxy_server.sockets[0].getsockname()[1]
    admin_port = admin_server.sockets[0].getsockname()[1]
    connector = "arn:aws:lambda:us-east-1:123456789012:network-connector:nc-1"
    config = ValsSandboxProviderConfig(
        VALS_SANDBOX_IMAGE=IMAGE_ARN,
        VALS_SANDBOX_EGRESS_CONNECTOR=connector,
        VALS_SANDBOX_EGRESS_PROXY_URL=f"http://127.0.0.1:{proxy_port}",
        VALS_SANDBOX_EGRESS_ADMIN_URL=f"http://127.0.0.1:{admin_port}",
        VALS_SANDBOX_EGRESS_ADMIN_TOKEN="admin-token",
    )
    fetch = (
        'python3 -c "import urllib.request; '
        f"print(urllib.request.urlopen('http://target.test:{target.server_address[1]}/').read().decode())\""
    )
    fake = FakeMicrovms(*agent_ports)
    try:
        async with ValsSandboxProvider(config, microvms=fake) as provider:
            sandbox = await provider.create_sandbox(_request())
            assert isinstance(sandbox, ValsSandbox)
            assert fake.run_calls[0]["egressNetworkConnectors"] == [connector]
            assert sandbox.egress_id in proxy.policies

            assert (await sandbox.exec(fetch)).exit_code != 0
            await sandbox.modify_egress_rules(["127.0.0.1/32"])
            assert (await sandbox.exec(fetch)).output.strip() == "reached"
            await sandbox.modify_egress_rules(["target.test"])
            assert (await sandbox.exec(fetch)).exit_code != 0
            await sandbox.block_all_egress()
            assert (await sandbox.exec(fetch)).exit_code != 0

            await provider.delete_sandbox(sandbox.id)
            assert proxy.policies == {}
    finally:
        proxy_server.close()
        admin_server.close()
        target.shutdown()


def test_egress_config_is_all_or_nothing() -> None:
    with pytest.raises(ValueError, match="VALS_SANDBOX_EGRESS"):
        ValsSandboxProviderConfig(VALS_SANDBOX_IMAGE=IMAGE_ARN, VALS_SANDBOX_EGRESS_CONNECTOR="arn")


@pytest.mark.parametrize(
    ("policy", "host", "allowed"),
    [
        (EgressPolicy(token="t", mode="allowlist", domains=["*.example.com"]), "api.example.com", True),
        (EgressPolicy(token="t", mode="allowlist", domains=["*.example.com"]), "example.org", False),
        (EgressPolicy(token="t", mode="allowlist", domains=["example.com"]), "a.example.com", False),
        (EgressPolicy(token="t", mode="allow_all"), "93.184.216.34", True),
        (EgressPolicy(token="t", mode="allow_all"), "169.254.169.254", False),
        (EgressPolicy(token="t", mode="allow_all"), "10.0.0.5", False),
        (EgressPolicy(token="t", mode="deny_all"), "93.184.216.34", False),
    ],
)
async def test_egress_policy_decisions(policy: EgressPolicy, host: str, allowed: bool) -> None:
    async def resolve(_host: str, _port: int) -> list[str]:
        return ["93.184.216.34"]

    address = await EgressProxy("admin", resolve=resolve).choose_address(policy, host, 443)
    assert (address is not None) == allowed


async def test_deny_all_never_resolves_names() -> None:
    resolved: list[str] = []

    async def resolve(host: str, _port: int) -> list[str]:
        resolved.append(host)
        return ["93.184.216.34"]

    proxy = EgressProxy("admin", resolve=resolve)
    assert await proxy.choose_address(EgressPolicy(token="t", mode="deny_all"), "secret.attacker.test", 443) is None
    assert resolved == []


async def test_policy_change_closes_open_tunnels() -> None:
    """An open CONNECT tunnel is closed when its sandbox's policy changes, so revocation covers it."""
    echo = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    echo_port = echo.sockets[0].getsockname()[1]

    async def resolve(_host: str, _port: int) -> list[str]:
        return ["127.0.0.1"]

    proxy = EgressProxy("admin", resolve=resolve)
    proxy.policies["sb"] = EgressPolicy(token="tok", mode="allowlist", cidrs=[ipaddress.IPv4Network("127.0.0.1/32")])
    proxy_server, admin_server = await egress_serve(proxy, host="127.0.0.1", proxy_port=0, admin_port=0)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_server.sockets[0].getsockname()[1])
        auth = base64.b64encode(b"sb:tok").decode()
        writer.write(f"CONNECT echo.test:{echo_port} HTTP/1.1\r\nProxy-Authorization: Basic {auth}\r\n\r\n".encode())
        assert (await reader.readuntil(b"\r\n\r\n")).startswith(b"HTTP/1.1 200")
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"http://127.0.0.1:{admin_server.sockets[0].getsockname()[1]}/sandboxes/sb",
                headers={"Authorization": "Bearer admin"},
                json={"mode": "deny_all"},
            )
        assert response.status_code == 204
        assert await asyncio.wait_for(reader.read(), 5) == b""
        assert proxy.policies["sb"].token == "tok"
        writer.close()
    finally:
        for server in (proxy_server, admin_server, echo):
            server.close()
