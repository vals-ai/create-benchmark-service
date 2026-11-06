import os
from uuid import uuid4

import pytest

from benchmark_service import ImageSource, Resources, SailProviderConfig, SandboxCreateRequest


@pytest.mark.skipif(
    not os.environ.get("TEST_SAIL_API_KEY"), reason="TEST_SAIL_API_KEY is required for a paid live smoke"
)
async def test_sail_live_lifecycle() -> None:
    async with SailProviderConfig(SAIL_API_KEY=os.environ["TEST_SAIL_API_KEY"]).create_provider() as provider:
        sandbox = await provider.create_sandbox(
            SandboxCreateRequest(
                source=ImageSource(image="sail:debian"),
                resources=Resources(vcpu=1, memory=16, disk=8),
                name=f"cbs-smoke-{uuid4().hex}",
                labels={"purpose": "cbs-live-smoke"},
                env_vars={},
                auto_stop_interval=1,
                create_timeout=180,
            )
        )
        try:
            await sandbox.probe_generation_containment()
            await sandbox.upload_file("/tmp/cbs-smoke", b"\x00\xff")
            assert await sandbox.download_file("/tmp/cbs-smoke") == b"\x00\xff"
            result = await sandbox.exec("printf sail-smoke", timeout=10)
            assert result.exit_code == 0
            assert result.output == "sail-smoke"
        finally:
            await provider.delete_sandbox(sandbox.id)
