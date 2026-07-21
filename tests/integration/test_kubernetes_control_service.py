"""Opt-in live contract test for an already deployed private control service.

Run: TEST_KUBERNETES_CONTROL_URL=... TEST_KUBERNETES_CONTROL_TOKEN=... TEST_KUBERNETES_IMAGE=... \
  uv run pytest tests/integration/test_kubernetes_control_service.py -q
"""

from __future__ import annotations

import os
import uuid

import pytest

from benchmark_service.sandbox.kubernetes.client import KubernetesControlClientDriver
from benchmark_service.sandbox.types import ImageSource, Resources, SandboxCreateRequest, SandboxQuery

_REQUIRED_ENV = (
    "TEST_KUBERNETES_CONTROL_URL",
    "TEST_KUBERNETES_CONTROL_TOKEN",
    "TEST_KUBERNETES_IMAGE",
)


@pytest.mark.skipif(
    any(not os.environ.get(name) for name in _REQUIRED_ENV),
    reason="private Kubernetes control-service variables are not configured",
)
async def test_live_kubernetes_control_service_contract() -> None:
    """Prove provider operations against infrastructure deployed outside this branch.

    Test cases:
    - Lifecycle and list-by-label find one unique sandbox.
    - Commands and binary files stream through the private endpoint.
    - Temporary egress can be applied, cleared, and cleaned up in finally.
    """
    sandbox_name = f"contract-{uuid.uuid4().hex[:12]}"
    driver = KubernetesControlClientDriver(
        api_url=os.environ["TEST_KUBERNETES_CONTROL_URL"],
        api_token=os.environ["TEST_KUBERNETES_CONTROL_TOKEN"],
    )
    created = None
    try:
        created = await driver.create_sandbox(
            SandboxCreateRequest(
                source=ImageSource(image=os.environ["TEST_KUBERNETES_IMAGE"]),
                resources=Resources(vcpu=1, memory=1, disk=5),
                name=sandbox_name,
                labels={"contract_test": sandbox_name},
                env_vars={},
                auto_stop_interval=10,
                create_timeout=300,
            )
        )
        fetched = await driver.get_sandbox(created.id)
        listed = [
            sandbox
            async for sandbox in driver.list_sandboxes(
                SandboxQuery(labels={"contract_test": sandbox_name}, page_size=2)
            )
        ]
        command_chunks = [
            chunk async for chunk in created.command("for value in first second third; do echo $value; sleep 0.2; done")
        ]
        content = b"\x00kubernetes-contract\xff"
        await created.upload_file("/workspace/contract.bin", content)
        downloaded = await created.download_file("/workspace/contract.bin")
        streamed = b"".join([chunk async for chunk in created.stream_download("/workspace/contract.bin")])
        await created.modify_egress_rules(["10.0.0.0/8"])
        await created.clear_egress_rules()

        assert fetched.id == created.id
        assert [sandbox.id for sandbox in listed] == [created.id]
        assert "first\nsecond\nthird\n" == "".join(command_chunks)
        assert len(command_chunks) >= 2
        assert downloaded == streamed == content
    finally:
        if created is not None:
            await driver.delete_sandbox(created.id)
        await driver.close()
