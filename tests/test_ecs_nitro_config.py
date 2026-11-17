from __future__ import annotations

from typing import Any

import pytest
from ecs_nitro_agents import KEY, free_port

from benchmark_service.sandbox.ecs_nitro.config import EcsNitroProviderConfig


def test_config_rejects_short_key_and_bad_ceiling_and_hides_the_key() -> None:
    values: dict[str, Any] = {
        "region": "us-east-1",
        "cluster": "proof",
        "capacity_provider": "nitro-mi",
        "vpc_id": "vpc-1",
        "subnet_ids": ["subnet-a"],
        "control_security_group_id": "sg-control",
        "execution_role_arn": "arn:aws:iam::123:role/exec",
        "agent_image": "agent@sha256:abc",
        "agent_port": free_port(),
    }
    with pytest.raises(ValueError, match="agent_token_key"):
        EcsNitroProviderConfig(**values, agent_token_key="short")
    with pytest.raises(ValueError):
        EcsNitroProviderConfig(**values, agent_token_key=KEY, egress_ceiling_cidrs=["not-a-cidr"])
    config = EcsNitroProviderConfig(**values, agent_token_key=KEY)
    assert KEY not in repr(config)
    assert EcsNitroProviderConfig.model_validate_json(config.model_dump_json()) == config
