from __future__ import annotations

import pytest
from ecs_nitro_fakes import FakeEc2, egress_of, make_config

from benchmark_service.sandbox.ecs_nitro.network import SecurityGroups
from benchmark_service.sandbox.types import UnsupportedSandboxConfigError


def _groups(ec2: FakeEc2) -> SecurityGroups:
    return SecurityGroups(make_config(), ec2, poll_seconds=0.01, delete_timeout=1)


@pytest.mark.asyncio
async def test_group_admits_only_the_control_group_and_keeps_egress_inside_the_ceiling() -> None:
    ec2 = FakeEc2()
    groups = SecurityGroups(
        make_config(resource_tags={"valkyrie:deployment": "proof"}), ec2, poll_seconds=0.01, delete_timeout=1
    )
    group = await groups.create("sandbox-1", [])
    tags = {t["Key"]: t["Value"] for t in ec2.created[0]["TagSpecifications"][0]["Tags"]}
    assert tags == {"valkyrie:deployment": "proof", "cbs:sandbox-id": "sandbox-1"}
    assert egress_of(ec2, group) == []
    assert ec2.ingress[group][0]["UserIdGroupPairs"] == [{"GroupId": "sg-control"}]
    assert ec2.ingress[group][0]["FromPort"] == 7070
    await groups.set_egress(group, groups.allowed_egress(["10.0.4.0/24", "10.0.5.6"]))
    assert sorted(egress_of(ec2, group)) == ["10.0.4.0/24", "10.0.5.6/32"]
    await groups.delete(group)
    await groups.delete(group)
    assert ec2.deleted == [group, group]


def test_resource_tags_cannot_use_reserved_prefixes() -> None:
    with pytest.raises(ValueError, match="reserved"):
        make_config(resource_tags={"cbs:sandbox-id": "x"})


@pytest.mark.parametrize(("address", "match"), [("api.example.com", "hostnames"), ("8.8.8.8", "ceiling")])
def test_egress_outside_the_policy_is_refused(address: str, match: str) -> None:
    with pytest.raises(UnsupportedSandboxConfigError, match=match):
        _groups(FakeEc2()).allowed_egress([address])
