from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from typing import Any

import httpx
import pytest
from ecs_nitro_agents import KEY, free_port, start_agent
from ecs_nitro_fakes import FakeEc2, FakeEcs, egress_of, make_config, run_task_params

from benchmark_service.app import (
    ECS_NITRO_ENABLED_ENV,
    _request_may_use_provider,  # pyright: ignore[reportPrivateUsage]
)
from benchmark_service.sandbox import sandbox_provider_config_from_mapping
from benchmark_service.sandbox.ecs_nitro import EcsNitroProviderConfig, EcsNitroSandboxProvider
from benchmark_service.sandbox.ecs_nitro.config import ISOLATION_MODE, SANDBOX_ID_TAG
from benchmark_service.sandbox.types import (
    ImageSource,
    Resources,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    UnsupportedSandboxConfigError,
    VolumeMount,
)


def make_request(**overrides: Any) -> SandboxCreateRequest:
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
    port = free_port()
    yield port


class AgentStartingEcs(FakeEcs):
    def __init__(self, port: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.port = port

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        result = await super().call(action, params)
        if action == "RunTask" and result["tasks"]:
            tags = {t["key"]: t["value"] for t in params["tags"]}
            start_agent(self.port, tags[SANDBOX_ID_TAG])
        return result


def make_provider(ecs: FakeEcs, ec2: FakeEc2, port: int = 7070, **config: Any) -> EcsNitroSandboxProvider:
    return EcsNitroSandboxProvider(make_config(port, **config), ecs=ecs, ec2=ec2, poll_seconds=0.01, cleanup_timeout=1)


@pytest.mark.asyncio
async def test_config_resolves_through_provider_union() -> None:
    config = sandbox_provider_config_from_mapping({**make_config().model_dump(mode="json"), "agent_token_key": KEY})
    assert isinstance(config, EcsNitroProviderConfig)
    assert isinstance(config.create_provider(), EcsNitroSandboxProvider)


@pytest.mark.asyncio
async def test_create_runs_isolated_task_and_serves_commands_and_files(
    agent_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="benchmark_service.sandbox.ecs_nitro")
    ecs, ec2 = AgentStartingEcs(agent_port), FakeEc2()
    provider = make_provider(ecs, ec2, agent_port, resource_tags={"valkyrie:deployment": "proof"})
    sandbox = await provider.create_sandbox(make_request())

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
    assert {t["key"]: t["value"] for t in run["tags"]}["valkyrie:deployment"] == "proof"
    assert definition["tags"] == [{"key": "valkyrie:deployment", "value": "proof"}]

    group = sandbox.provider_metadata["security_group"]
    assert egress_of(ec2, group) == ["10.0.0.0/16"]
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
    assert sorted(egress_of(ec2, group)) == ["10.0.4.0/24", "10.0.5.6/32"]
    await sandbox.block_all_egress()
    assert egress_of(ec2, group) == []
    await sandbox.clear_egress_rules()
    assert egress_of(ec2, group) == ["10.0.0.0/16"]
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
async def test_agent_timeout_cleans_up_task_and_security_group() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    provider = make_provider(ecs, ec2, free_port())
    with pytest.raises(TimeoutError, match="agent"):
        await provider.create_sandbox(make_request(create_timeout=1))
    assert [t["lastStatus"] for t in ecs.tasks.values()] == ["STOPPED"]
    assert ec2.deleted == ["sg-0"]


@pytest.mark.asyncio
async def test_failed_placement_deletes_security_group() -> None:
    ecs, ec2 = FakeEcs(place=False), FakeEc2()
    with pytest.raises(SandboxError, match="RESOURCE:CPU"):
        await make_provider(ecs, ec2).create_sandbox(make_request())
    assert ec2.deleted == ["sg-0"]


@pytest.mark.asyncio
async def test_task_stopped_during_startup_is_cleaned_up() -> None:
    ecs, ec2 = FakeEcs(run_status="STOPPED"), FakeEc2()
    with pytest.raises(SandboxError, match="stopped during startup"):
        await make_provider(ecs, ec2).create_sandbox(make_request())
    assert ec2.deleted == ["sg-0"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "match"),
    [
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
        await make_provider(ecs, ec2).create_sandbox(make_request(**overrides))
    assert ecs.calls == [] and ec2.groups == {}


@pytest.mark.asyncio
async def test_missing_sandbox_lookup_and_idempotent_delete() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    provider = make_provider(ecs, ec2)
    ecs.tasks["foreign"] = {"taskArn": "arn/foreign", "lastStatus": "RUNNING", "startedBy": "someone-else"}
    with pytest.raises(SandboxNotFoundError):
        await provider.get_sandbox("foreign")
    await provider.delete_sandbox("absent")
    assert not any(action == "StopTask" for action, _ in ecs.calls)


class LostResponseEcs(FakeEcs):
    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        result = await super().call(action, params)
        if action == "RunTask":
            raise httpx.ReadError("connection lost after RunTask was accepted")
        return result


@pytest.mark.asyncio
async def test_lost_run_task_response_still_stops_the_task() -> None:
    ecs, ec2 = LostResponseEcs(), FakeEc2()
    with pytest.raises(httpx.ReadError):
        await make_provider(ecs, ec2).create_sandbox(make_request())
    assert [t["lastStatus"] for t in ecs.tasks.values()] == ["STOPPED"]
    assert ec2.deleted == ["sg-0"]


@pytest.mark.asyncio
async def test_stopped_sandboxes_stay_listed_for_cleanup() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    provider = make_provider(ecs, ec2)
    params = run_task_params()
    params["tags"] = [*params["tags"], {"key": "run", "value": "r1"}]
    task_arn = (await ecs.call("RunTask", params))["tasks"][0]["taskArn"]
    await ecs.call("StopTask", {"task": task_arn})
    listed = [s async for s in provider.list_sandboxes(SandboxQuery(labels={"run": "r1"}))]
    assert [s.has_ended for s in listed] == [True]


def test_requests_need_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    config = make_config()
    monkeypatch.delenv(ECS_NITRO_ENABLED_ENV, raising=False)
    assert not _request_may_use_provider(config)
    monkeypatch.setenv(ECS_NITRO_ENABLED_ENV, "true")
    assert _request_may_use_provider(config)
    assert sandbox_provider_config_from_mapping(json.loads(config.model_dump_json())) == config


@pytest.mark.asyncio
async def test_group_that_never_becomes_visible_fails_before_run_task() -> None:
    ecs, ec2 = FakeEcs(), FakeEc2()
    ec2.invisible_calls = 10_000
    with pytest.raises(SandboxError, match="not visible"):
        await make_provider(ecs, ec2).create_sandbox(make_request())
    assert not any(action == "RunTask" for action, _ in ecs.calls)
    assert ec2.deleted == ["sg-0"]
