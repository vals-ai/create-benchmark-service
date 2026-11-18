"""Tests for the Lambda MicroVM provider against the real in-VM agent on localhost.

Run: uv run pytest tests/test_lambda_microvm.py
"""

from __future__ import annotations

import json
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
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
from benchmark_service.sandbox.lambda_microvm import (
    LambdaMicrovmProviderConfig,
    LambdaMicrovmSandbox,
    LambdaMicrovmSandboxProvider,
)
from benchmark_service.sandbox.lambda_microvm import agent
from benchmark_service.sandbox.lambda_microvm.image import (
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


def _provider(fake: FakeMicrovms) -> LambdaMicrovmSandboxProvider:
    return LambdaMicrovmSandboxProvider(LambdaMicrovmProviderConfig(LAMBDA_MICROVM_IMAGE=IMAGE_ARN), microvms=fake)


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
        assert isinstance(sandbox, LambdaMicrovmSandbox)
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
    provider = LambdaMicrovmProviderConfig(LAMBDA_MICROVM_IMAGE=IMAGE_ARN).create_provider()
    with pytest.raises(UnsupportedSandboxConfigError):
        provider.validate_sandbox_config(requires_nested_docker=nested, resources=resources)


def test_config_decodes_from_provider_mapping() -> None:
    config = sandbox_provider_config_from_mapping({"type": "lambda_microvm", "LAMBDA_MICROVM_IMAGE": IMAGE_ARN})
    assert isinstance(config, LambdaMicrovmProviderConfig)


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
        with pytest.raises(MissingSandboxConfigError, match="LAMBDA_MICROVM_BUILD_BUCKET"):
            await provider.create_sandbox(_request(source=DockerfileSource(dockerfile=str(dockerfile))))


def test_agent_run_hook_rejects_unknown_hooks(agent_ports: tuple[int, int]) -> None:
    _, hook_port = agent_ports
    response = httpx.post(f"http://127.0.0.1:{hook_port}{agent.HOOK_PREFIX}bogus", content=json.dumps({}))
    assert response.status_code == 404
