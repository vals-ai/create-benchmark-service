from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest

from benchmark_service.sandbox import sandbox_provider_config_from_mapping
from benchmark_service.sandbox.ecs_nitro import EcsNitroProviderConfig, EcsNitroSandboxProvider
from benchmark_service.sandbox.ecs_nitro.agent import serve
from benchmark_service.sandbox.ecs_nitro.aws import AwsApiError
from benchmark_service.sandbox.ecs_nitro.provider import (
    ISOLATION_MODE,
    SANDBOX_ID_TAG,
    SECURITY_GROUP_TAG,
    STARTED_BY,
    agent_token,
)
from benchmark_service.sandbox.types import (
    DockerfileSource,
    ImageSource,
    Resources,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    SnapshotSource,
    UnsupportedSandboxConfigError,
    VolumeMount,
)

KEY = "k" * 64


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _config(port: int = 7070, **overrides: Any) -> EcsNitroProviderConfig:
    values: dict[str, Any] = {
        "region": "us-east-1",
        "cluster": "proof",
        "capacity_provider": "nitro-mi",
        "vpc_id": "vpc-1",
        "subnet_ids": ["subnet-a"],
        "control_security_group_id": "sg-control",
        "execution_role_arn": "arn:aws:iam::123:role/exec",
        "agent_image": "123.dkr.ecr.us-east-1.amazonaws.com/agent@sha256:abc",
        "agent_token_key": KEY,
        "agent_port": port,
        "egress_ceiling_cidrs": ["10.0.0.0/16"],
    }
    values.update(overrides)
    return EcsNitroProviderConfig(**values)


class FakeEcs:
    def __init__(self, *, nitro: bool = True, run_status: str = "RUNNING", place: bool = True) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.tasks: dict[str, dict[str, Any]] = {}
        self.definitions: dict[str, dict[str, Any]] = {}
        self.nitro = nitro
        self.run_status = run_status
        self.place = place

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append((action, dict(params)))
        if action == "DescribeClusters":
            return {"clusters": [{"status": "ACTIVE", "capacityProviders": ["nitro-mi"]}]}
        if action == "DescribeCapacityProviders":
            template = {"nitroEnclavesEnabled": self.nitro}
            return {
                "capacityProviders": [
                    {"status": "ACTIVE", "managedInstancesProvider": {"instanceLaunchTemplate": template}}
                ]
            }
        if action == "DescribeTaskDefinition":
            family = params["taskDefinition"]
            if family not in self.definitions:
                raise AwsApiError("ecs", action, "ClientException", "Unable to describe task definition.")
            return {"taskDefinition": self.definitions[family]}
        if action == "RegisterTaskDefinition":
            definition = {**params, "taskDefinitionArn": f"arn:td/{params['family']}:1", "status": "ACTIVE"}
            self.definitions[params["family"]] = definition
            return {"taskDefinition": definition}
        if action == "RunTask":
            if not self.place:
                return {"tasks": [], "failures": [{"reason": "RESOURCE:CPU"}]}
            task_id = f"task{len(self.tasks)}"
            task = {
                "taskArn": f"arn:aws:ecs:us-east-1:123:task/proof/{task_id}",
                "lastStatus": self.run_status,
                "startedBy": params["startedBy"],
                "capacityProviderName": params["capacityProviderStrategy"][0]["capacityProvider"],
                "launchType": "MANAGED_INSTANCES",
                "createdAt": time.time(),
                "tags": params["tags"],
                "attachments": [
                    {
                        "type": "ElasticNetworkInterface",
                        "details": [{"name": "privateIPv4Address", "value": "127.0.0.1"}],
                    }
                ],
            }
            self.tasks[task_id] = task
            return {"tasks": [task]}
        if action == "DescribeTasks":
            found = [
                self.tasks[ref.rsplit("/", 1)[-1]] for ref in params["tasks"] if ref.rsplit("/", 1)[-1] in self.tasks
            ]
            return {"tasks": found}
        if action == "StopTask":
            self.tasks[params["task"].rsplit("/", 1)[-1]]["lastStatus"] = "STOPPED"
            return {}
        if action == "ListTasks":
            running = [t["taskArn"] for t in self.tasks.values() if t["lastStatus"] != "STOPPED"]
            return {"taskArns": running}
        raise AssertionError(f"unexpected ECS call {action}")

    async def close(self) -> None:
        pass


class FakeEc2:
    def __init__(self) -> None:
        self.groups: dict[str, list[dict[str, Any]]] = {}
        self.ingress: dict[str, list[dict[str, Any]]] = {}
        self.deleted: list[str] = []

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        if action == "describe_subnets":
            return {"Subnets": [{"SubnetId": "subnet-a", "VpcId": "vpc-1", "MapPublicIpOnLaunch": False}]}
        if action == "create_security_group":
            group = f"sg-{len(self.groups)}"
            self.groups[group] = [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]
            return {"GroupId": group}
        if action == "describe_security_groups":
            group = params["GroupIds"][0]
            return {"SecurityGroups": [{"IpPermissionsEgress": list(self.groups[group])}]}
        if action == "revoke_security_group_egress":
            self.groups[params["GroupId"]] = []
            return {}
        if action == "authorize_security_group_egress":
            self.groups[params["GroupId"]].extend(params["IpPermissions"])
            return {}
        if action == "authorize_security_group_ingress":
            self.ingress[params["GroupId"]] = list(params["IpPermissions"])
            return {}
        if action == "delete_security_group":
            self.deleted.append(params["GroupId"])
            self.groups.pop(params["GroupId"], None)
            return {}
        raise AssertionError(f"unexpected EC2 call {action}")


def _egress(ec2: FakeEc2, group: str) -> list[str]:
    return [r["CidrIp"] for permission in ec2.groups[group] for r in permission.get("IpRanges", [])]


def _request(**overrides: Any) -> SandboxCreateRequest:
    values: dict[str, Any] = {
        "source": ImageSource(image="public.ecr.aws/docker/library/python:3.12"),
        "resources": Resources(vcpu=1.5, memory=4, disk=10),
        "name": "task-1",
        "labels": {"run": "r1"},
        "env_vars": {"FOO": "bar"},
        "auto_stop_interval": 0,
        "create_timeout": 5,
    }
    values.update(overrides)
    return SandboxCreateRequest(**values)


@pytest.fixture
async def agent_port() -> AsyncIterator[int]:
    port = _free_port()
    yield port


def _start_agent(port: int, sandbox_id: str) -> None:
    thread = threading.Thread(
        target=serve, args=(agent_token(KEY, sandbox_id), port), kwargs={"host": "127.0.0.1"}, daemon=True
    )
    thread.start()


class AgentStartingEcs(FakeEcs):
    def __init__(self, port: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.port = port

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        result = await super().call(action, params)
        if action == "RunTask" and result["tasks"]:
            tags = {t["key"]: t["value"] for t in params["tags"]}
            _start_agent(self.port, tags[SANDBOX_ID_TAG])
        return result


def _provider(ecs: FakeEcs, ec2: FakeEc2, port: int = 7070, **config: Any) -> EcsNitroSandboxProvider:
    return EcsNitroSandboxProvider(_config(port, **config), ecs=ecs, ec2=ec2, poll_seconds=0.01, cleanup_timeout=1)


@pytest.mark.asyncio
async def test_config_resolves_through_provider_union() -> None:
    config = sandbox_provider_config_from_mapping({**_config().model_dump(mode="json"), "agent_token_key": KEY})
    assert isinstance(config, EcsNitroProviderConfig)
    assert isinstance(config.create_provider(), EcsNitroSandboxProvider)


@pytest.mark.asyncio
async def test_create_runs_isolated_task_and_serves_commands_and_files(
    agent_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="benchmark_service.sandbox.ecs_nitro.provider")
    ecs, ec2 = AgentStartingEcs(agent_port), FakeEc2()
    provider = _provider(ecs, ec2, agent_port)
    sandbox = await provider.create_sandbox(_request())

    definition = next(params for action, params in ecs.calls if action == "RegisterTaskDefinition")
    assert definition["trustedExecutionConfiguration"] == {"isolationMode": ISOLATION_MODE}
    assert definition["requiresCompatibilities"] == ["MANAGED_INSTANCES"]
    assert definition["networkMode"] == "awsvpc"
    assert "taskRoleArn" not in definition
    run = next(params for action, params in ecs.calls if action == "RunTask")
    assert run["networkConfiguration"]["awsvpcConfiguration"]["assignPublicIp"] == "DISABLED"
    assert run["capacityProviderStrategy"] == [{"capacityProvider": "nitro-mi", "weight": 1}]
    assert "launchType" not in run
    assert run["overrides"]["cpu"] == "1536" and run["overrides"]["memory"] == "4096"
    assert run["enableExecuteCommand"] is False

    group = sandbox.provider_metadata["security_group"]
    assert _egress(ec2, group) == ["10.0.0.0/16"]
    assert ec2.ingress[group][0]["UserIdGroupPairs"] == [{"GroupId": "sg-control"}]
    assert sandbox.labels == {"run": "r1"}

    result = await sandbox.exec("echo token=${CBS_AGENT_TOKEN:-unset}; pwd", cwd="/tmp")
    assert result.exit_code == 0
    assert result.output.splitlines() == ["token=unset", "/tmp"]
    assert (await sandbox.exec("exit 3")).exit_code == 3
    timings = [json.loads(r.getMessage()) for r in caplog.records if r.getMessage().startswith("{")]
    assert [t["event"] for t in timings] == ["ecs_nitro.sandbox_ready", "ecs_nitro.first_exec"]
    assert timings[0]["run_task_submitted_at"] <= timings[0]["agent_ready_at"]
    assert timings[1]["exit_code"] == 0
    assert (await sandbox.exec("sleep 5", timeout=0.2)).exit_code == 124

    chunks = [chunk async for chunk in sandbox.command("printf a; printf $X", env_vars={"X": "b"})]
    assert "".join(chunks) == "ab"
    with pytest.raises(SandboxCommandError) as failure:
        _ = [chunk async for chunk in sandbox.command("exit 7")]
    assert failure.value.exit_code == 7

    payload = bytes(range(256)) * 5000
    await sandbox.upload_file(f"/tmp/cbs-ecs-test-{agent_port}/nested/file.bin", payload)
    assert await sandbox.download_file(f"/tmp/cbs-ecs-test-{agent_port}/nested/file.bin") == payload
    streamed = [c async for c in sandbox.stream_download(f"/tmp/cbs-ecs-test-{agent_port}/nested/file.bin")]
    assert b"".join(streamed) == payload
    with pytest.raises(SandboxNotFoundError):
        await sandbox.download_file(f"/tmp/cbs-ecs-test-{agent_port}/missing")

    await sandbox.modify_egress_rules(["10.0.4.0/24", "10.0.5.6"])
    assert sorted(_egress(ec2, group)) == ["10.0.4.0/24", "10.0.5.6/32"]
    await sandbox.block_all_egress()
    assert _egress(ec2, group) == []
    await sandbox.clear_egress_rules()
    assert _egress(ec2, group) == ["10.0.0.0/16"]
    with pytest.raises(UnsupportedSandboxConfigError, match="hostnames"):
        await sandbox.modify_egress_rules(["api.example.com"])
    with pytest.raises(UnsupportedSandboxConfigError, match="ceiling"):
        await sandbox.modify_egress_rules(["8.8.8.8"])

    found = await provider.get_sandbox(sandbox.id)
    assert (await found.exec("echo again")).output == "again\n"
    listed = [s.id async for s in provider.list_sandboxes(SandboxQuery(labels={"run": "r1"}))]
    assert listed == [sandbox.id]
    assert [s async for s in provider.list_sandboxes(SandboxQuery(labels={"run": "other"}))] == []

    await provider.delete_sandbox(sandbox.id)
    assert ecs.tasks[sandbox.id]["lastStatus"] == "STOPPED"
    assert ec2.deleted == [group]


@pytest.mark.asyncio
async def test_agent_rejects_wrong_token(agent_port: int) -> None:
    ecs, ec2 = AgentStartingEcs(agent_port), FakeEc2()
    provider = _provider(ecs, ec2, agent_port)
    sandbox = await provider.create_sandbox(_request(network_block_all=True))
    assert _egress(ec2, sandbox.provider_metadata["security_group"]) == []
    forged = provider.agent_client("127.0.0.1", "other-sandbox")
    assert not await forged.healthy()
    with pytest.raises(SandboxError, match="401"):
        await forged.upload("/tmp/forged", b"x")


@pytest.mark.asyncio
async def test_agent_timeout_cleans_up_task_and_security_group() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    provider = _provider(ecs, ec2, _free_port())
    with pytest.raises(TimeoutError, match="agent"):
        await provider.create_sandbox(_request(create_timeout=1))
    assert [t["lastStatus"] for t in ecs.tasks.values()] == ["STOPPED"]
    assert ec2.deleted == ["sg-0"]


@pytest.mark.asyncio
async def test_failed_placement_deletes_security_group() -> None:
    ecs, ec2 = FakeEcs(place=False), FakeEc2()
    with pytest.raises(SandboxError, match="RESOURCE:CPU"):
        await _provider(ecs, ec2).create_sandbox(_request())
    assert ec2.deleted == ["sg-0"]


@pytest.mark.asyncio
async def test_task_stopped_during_startup_is_cleaned_up() -> None:
    ecs, ec2 = FakeEcs(run_status="STOPPED"), FakeEc2()
    with pytest.raises(SandboxError, match="stopped during startup"):
        await _provider(ecs, ec2).create_sandbox(_request())
    assert ec2.deleted == ["sg-0"]


@pytest.mark.asyncio
async def test_capacity_provider_without_nitro_is_refused() -> None:
    ecs, ec2 = FakeEcs(nitro=False), FakeEc2()
    with pytest.raises(SandboxError, match="Nitro Enclaves"):
        await _provider(ecs, ec2).create_sandbox(_request())
    assert not any(action == "RunTask" for action, _ in ecs.calls)
    assert ec2.groups == {}


@pytest.mark.asyncio
async def test_task_definition_without_isolation_is_refused() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    provider = _provider(ecs, ec2)
    family = provider._task_definition("img", {})["family"]  # pyright: ignore[reportPrivateUsage]
    ecs.definitions[family] = {"taskDefinitionArn": "arn:td", "status": "ACTIVE"}
    with pytest.raises(SandboxError, match="ISOLATED_SANDBOX"):
        await provider.create_sandbox(_request(source=ImageSource(image="img")))
    assert not any(action == "RunTask" for action, _ in ecs.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"source": SnapshotSource(snapshot="snap")}, "snapshot sources"),
        ({"source": DockerfileSource(dockerfile="/work/Dockerfile")}, "dockerfile sources"),
        ({"resources": Resources(vcpu=1, memory=2, disk=10, gpu=1)}, "GPU"),
        ({"resources": Resources(vcpu=32, memory=2, disk=10)}, "vCPU"),
        ({"resources": Resources(vcpu=1, memory=128, disk=10)}, "memory"),
        ({"resources": Resources(vcpu=1, memory=2, disk=21)}, "fixed 20 GiB"),
        ({"requires_nested_docker": True}, "nested Docker"),
        ({"volumes": [VolumeMount(name="v", mount_path="/data")]}, "volumes"),
        ({"sandbox_secrets": {"TOKEN": "plain-name"}}, "ARNs"),
        ({"labels": {"cbs:sandbox-id": "x"}}, "reserved"),
    ],
)
async def test_unsupported_requests_fail_before_provisioning(overrides: dict[str, Any], match: str) -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    with pytest.raises(UnsupportedSandboxConfigError, match=match):
        await _provider(ecs, ec2).create_sandbox(_request(**overrides))
    assert ecs.calls == [] and ec2.groups == {}


@pytest.mark.asyncio
async def test_admission_counts_running_sandboxes() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    provider = _provider(ecs, ec2, max_sandboxes=1)
    source, resources = ImageSource(image="img"), Resources(vcpu=1, memory=2, disk=10)
    assert await provider.check_admission(source, resources)
    ecs.tasks["t"] = {"taskArn": "arn/t", "lastStatus": "RUNNING", "startedBy": STARTED_BY}
    assert not await provider.check_admission(source, resources)
    assert provider.admission_pool_id == "ecs_nitro:us-east-1:proof:nitro-mi"


@pytest.mark.asyncio
async def test_missing_sandbox_lookup_and_idempotent_delete() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    provider = _provider(ecs, ec2)
    ecs.tasks["foreign"] = {"taskArn": "arn/foreign", "lastStatus": "RUNNING", "startedBy": "someone-else"}
    with pytest.raises(SandboxNotFoundError):
        await provider.get_sandbox("foreign")
    await provider.delete_sandbox("absent")
    assert not any(action == "StopTask" for action, _ in ecs.calls)


def test_config_rejects_short_key_and_bad_ceiling() -> None:
    with pytest.raises(ValueError, match="agent_token_key"):
        _config(agent_token_key="short")
    with pytest.raises(ValueError):
        _config(egress_ceiling_cidrs=["not-a-cidr"])
    assert SECURITY_GROUP_TAG.startswith("cbs:")
