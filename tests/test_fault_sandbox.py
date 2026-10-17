"""Tests for FaultySandboxProvider.

Run: uv run pytest tests/test_fault_sandbox.py
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from benchmark_service.sandbox import (
    Fault,
    FaultyProviderConfig,
    FaultySandbox,
    FaultySandboxProvider,
    ImageSource,
    Resources,
    SandboxCapabilities,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    SnapshotSource,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.sandbox.types import ExecResult, Sandbox, SandboxProvider


class RecordingSandbox(Sandbox):
    """Inner sandbox that records every call so tests can tell injected faults from pass-through."""

    def __init__(self, sandbox_id: str) -> None:
        self._id = sandbox_id
        self.calls: list[str] = []
        self.labels = {"run-id": "run-1"}
        self.created_at = datetime(2026, 1, 1, tzinfo=UTC)

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return f"name-{self._id}"

    @property
    def state(self) -> str:
        return "started"

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        return {"runner_id": "r1"}

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.calls.append(f"exec:{command}")
        return ExecResult(exit_code=0, output=f"ran {command}")

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        self.calls.append(f"command:{command}")
        yield "line 1\n"
        yield "line 2\n"

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.calls.append(f"upload:{remote_path}")

    async def download_file(self, remote_path: str) -> bytes:
        self.calls.append(f"download:{remote_path}")
        return b"content"

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        self.calls.append("egress")

    async def snapshot(self, name: str) -> SnapshotSource:
        self.calls.append(f"snapshot:{name}")
        return SnapshotSource(snapshot=name)


class RecordingProvider(SandboxProvider):
    def __init__(self) -> None:
        self.sandboxes: dict[str, RecordingSandbox] = {}
        self.deleted: list[str] = []
        self.closed = False

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(snapshot=True)

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        sandbox = RecordingSandbox(f"sb-{len(self.sandboxes) + 1}")
        self.sandboxes[sandbox.id] = sandbox
        return sandbox

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        try:
            return self.sandboxes[instance_id]
        except KeyError:
            raise SandboxNotFoundError(instance_id) from None

    async def delete_sandbox(self, instance_id: str) -> None:
        self.deleted.append(instance_id)
        self.sandboxes.pop(instance_id, None)

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        for sandbox in list(self.sandboxes.values()):
            yield sandbox

    async def close(self) -> None:
        self.closed = True


REQUEST = SandboxCreateRequest(
    source=ImageSource(image="python:3.12-slim"),
    resources=Resources(vcpu=1, memory=1, disk=1),
    name="task-1",
    labels={},
    env_vars={},
    auto_stop_interval=5,
    create_timeout=60,
)


def make_provider(*faults: Fault) -> tuple[FaultySandboxProvider, RecordingProvider]:
    inner = RecordingProvider()
    return FaultySandboxProvider(inner, faults), inner


async def test_create_fault_fires_once_then_passes_through() -> None:
    provider, inner = make_provider(Fault(at="create_sandbox", effect="connection_error", message="dial tcp: refused"))

    with pytest.raises(SandboxConnectionError, match="dial tcp: refused"):
        await provider.create_sandbox(REQUEST)
    assert inner.sandboxes == {}, "a 'before' fault must not create a real sandbox"

    sandbox = await provider.create_sandbox(REQUEST)

    assert isinstance(sandbox, FaultySandbox)
    assert sandbox.id == "sb-1"
    assert sandbox.labels == {"run-id": "run-1"}
    assert sandbox.provider_metadata == {"runner_id": "r1"}
    assert provider.pending == []
    assert [injected.fault.at for injected in provider.injected] == ["create_sandbox"]


async def test_times_repeats_a_fault() -> None:
    provider, inner = make_provider(Fault(at="create_sandbox", times=2))

    for _ in range(2):
        with pytest.raises(SandboxConnectionError):
            await provider.create_sandbox(REQUEST)
    await provider.create_sandbox(REQUEST)

    assert len(inner.sandboxes) == 1
    assert len(provider.injected) == 2


async def test_faults_fire_in_plan_order_across_points() -> None:
    provider, inner = make_provider(
        Fault(
            at="exec", effect="command_failure", exit_code=7, message="docker daemon is not ready inside the sandbox"
        ),
        Fault(at="command", effect="not_found", message="Sandbox sb-1 was removed"),
    )
    sandbox = await provider.create_sandbox(REQUEST)

    result = await sandbox.exec("start-dockerd")
    assert (result.exit_code, result.output) == (7, "docker daemon is not ready inside the sandbox")
    assert inner.sandboxes["sb-1"].calls == [], "command_failure must not run the real command"

    chunks: list[str] = []
    with pytest.raises(SandboxNotFoundError, match="sb-1 was removed"):
        async for chunk in sandbox.command("run-agent"):
            chunks.append(chunk)
    assert chunks == []

    assert (await sandbox.exec("start-dockerd")).output == "ran start-dockerd"
    assert [chunk async for chunk in sandbox.command("run-agent")] == ["line 1\n", "line 2\n"]
    assert [(injected.fault.at, injected.sandbox_id, injected.command) for injected in provider.injected] == [
        ("exec", "sb-1", "start-dockerd"),
        ("command", "sb-1", "run-agent"),
    ]


async def test_command_contains_skips_other_commands() -> None:
    provider, inner = make_provider(Fault(at="exec", command_contains="pip install"))
    sandbox = await provider.create_sandbox(REQUEST)

    assert (await sandbox.exec("echo hi")).exit_code == 0
    with pytest.raises(SandboxConnectionError):
        await sandbox.exec("pip install six")
    assert inner.sandboxes["sb-1"].calls == ["exec:echo hi"]


async def test_after_fault_keeps_side_effects_but_loses_the_reply() -> None:
    provider, inner = make_provider(
        Fault(at="command", when="after", message="websocket closed"),
        Fault(at="upload_file", when="after"),
    )
    sandbox = await provider.create_sandbox(REQUEST)

    chunks: list[str] = []
    with pytest.raises(SandboxConnectionError, match="websocket closed"):
        async for chunk in sandbox.command("run-agent"):
            chunks.append(chunk)
    assert chunks == ["line 1\n", "line 2\n"], "the agent's output arrived before the connection dropped"

    with pytest.raises(SandboxConnectionError):
        await sandbox.upload_file("/tmp/x", b"x")
    assert inner.sandboxes["sb-1"].calls == ["command:run-agent", "upload:/tmp/x"]


async def test_command_failure_on_stream_yields_message_then_raises_exit_code() -> None:
    provider, _ = make_provider(Fault(at="command", effect="command_failure", exit_code=137, message="Killed"))
    sandbox = await provider.create_sandbox(REQUEST)

    chunks: list[str] = []
    with pytest.raises(SandboxCommandError) as excinfo:
        async for chunk in sandbox.command("run-agent"):
            chunks.append(chunk)
    assert chunks == ["Killed"]
    assert excinfo.value.exit_code == 137


async def test_get_and_delete_faults_and_default_messages() -> None:
    provider, inner = make_provider(
        Fault(at="get_sandbox", effect="not_found"),
        Fault(at="delete_sandbox", effect="error"),
    )
    await provider.create_sandbox(REQUEST)

    with pytest.raises(SandboxNotFoundError, match="Injected sandbox not found at get_sandbox"):
        await provider.get_sandbox("sb-1")
    assert (await provider.get_sandbox("sb-1")).id == "sb-1"

    with pytest.raises(SandboxError, match="Injected error at delete_sandbox"):
        await provider.delete_sandbox("sb-1")
    assert inner.deleted == [], "a 'before' delete fault leaves the sandbox in place"
    assert provider.injected[-1].sandbox_id == "sb-1"


async def test_passthrough_surface_delegates_to_inner() -> None:
    provider, inner = make_provider()
    sandbox = await provider.create_sandbox(REQUEST)

    assert provider.capabilities == SandboxCapabilities(snapshot=True)
    assert await sandbox.snapshot("snap") == SnapshotSource(snapshot="snap")
    with pytest.raises(SandboxError, match="does not support snapshots"):
        await provider.delete_snapshot(SnapshotSource(snapshot="snap"))
    assert await sandbox.download_file("/tmp/x") == b"content"
    assert [chunk async for chunk in sandbox.stream_download("/tmp/x")] == [b"content"]
    listed = [listed async for listed in provider.list_sandboxes(SandboxQuery(labels={}))]
    assert [type(listed_sandbox) for listed_sandbox in listed] == [FaultySandbox]
    async with provider:
        pass
    assert inner.closed


async def test_faulty_config_round_trips_and_wraps_inner_provider() -> None:
    config = sandbox_provider_config_from_mapping(
        {
            "type": "faulty",
            "inner": {"type": "docker"},
            "faults": [{"at": "create_sandbox", "effect": "connection_error", "times": 2}],
        }
    )

    assert isinstance(config, FaultyProviderConfig)
    provider = config.create_provider()
    assert isinstance(provider, FaultySandboxProvider)
    assert provider.pending == [Fault(at="create_sandbox", times=2)]
    assert type(provider.inner).__name__ == "DockerSandboxProvider"
    await provider.close()


def test_faulty_config_rejects_nested_faulty_and_unknown_points() -> None:
    with pytest.raises(ValidationError):
        sandbox_provider_config_from_mapping(
            {"type": "faulty", "inner": {"type": "faulty", "inner": {"type": "docker"}}}
        )
    with pytest.raises(ValidationError):
        Fault.model_validate({"at": "reboot"})
