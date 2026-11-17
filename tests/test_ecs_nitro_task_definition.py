from __future__ import annotations

import pytest
from ecs_nitro_fakes import FakeEcs, make_config

from benchmark_service.sandbox.ecs_nitro.config import ISOLATION_MODE
from benchmark_service.sandbox.ecs_nitro.task_definition import TaskDefinitions
from benchmark_service.sandbox.types import SandboxError


@pytest.mark.asyncio
async def test_definition_is_isolated_private_and_reused() -> None:
    ecs = FakeEcs()
    definitions = TaskDefinitions(make_config(task_family_prefix="proof-sandbox"), ecs)
    arn = await definitions.ensure("img", {"TOKEN": "arn:aws:secretsmanager:us-east-1:1:secret:t"})
    assert await definitions.ensure("img", {"TOKEN": "arn:aws:secretsmanager:us-east-1:1:secret:t"}) == arn
    [definition] = [params for action, params in ecs.calls if action == "RegisterTaskDefinition"]
    assert definition["family"].startswith("proof-sandbox-")
    assert definition["trustedExecutionConfiguration"] == {"isolationMode": ISOLATION_MODE}
    assert definition["requiresCompatibilities"] == ["MANAGED_INSTANCES"]
    assert definition["networkMode"] == "awsvpc"
    assert "taskRoleArn" not in definition
    sandbox = definition["containerDefinitions"][1]
    assert sandbox["secrets"] == [{"name": "TOKEN", "valueFrom": "arn:aws:secretsmanager:us-east-1:1:secret:t"}]


@pytest.mark.asyncio
async def test_definition_without_isolation_is_refused() -> None:
    ecs = FakeEcs()
    definitions = TaskDefinitions(make_config(), ecs)
    ecs.definitions[definitions.spec("img", {})["family"]] = {"taskDefinitionArn": "arn:td", "status": "ACTIVE"}
    with pytest.raises(SandboxError, match="ISOLATED_SANDBOX"):
        await definitions.ensure("img", {})
