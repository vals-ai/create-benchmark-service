from __future__ import annotations

# pyright: reportPrivateUsage=false

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from modal import Sandbox as ModalSdkSandbox
from modal.exception import ConflictError as ModalConflictError
from modal.exception import ConnectionError as ModalConnectionError
from modal.exception import Error as ModalError
from modal.exception import ImageBuildError as ModalImageBuildError
from modal.exception import InvalidError as ModalInvalidError
from modal.exception import NotFoundError as ModalNotFoundError
from modal.exception import ResourceExhaustedError as ModalResourceExhaustedError

import benchmark_service.sandbox.modal as modal_module
from benchmark_service.sandbox import sandbox_provider_config_from_mapping
from benchmark_service.sandbox.modal import ModalProviderConfig, ModalSandbox, ModalSandboxProvider
from benchmark_service.sandbox.types import (
    DockerfileSource,
    ImageSource,
    Resources,
    SandboxBuildError,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    SnapshotSource,
    TargetedSnapshotSource,
    VolumeMount,
)


def _aio(fn: Any) -> SimpleNamespace:
    return SimpleNamespace(aio=fn)


async def _chunks(*chunks: str | bytes) -> AsyncGenerator[str | bytes, None]:
    for chunk in chunks:
        yield chunk


class FakeProcess:
    def __init__(self, chunks: list[str] | list[bytes], exit_code: int) -> None:
        self.stdout = _chunks(*chunks)
        self.wait = _aio(self._wait)
        self._exit_code = exit_code

    async def _wait(self) -> int:
        return self._exit_code


class HangingProcess(FakeProcess):
    def __init__(self) -> None:
        super().__init__([], 137)
        self.read_started = asyncio.Event()
        self.read_cancelled = asyncio.Event()
        self.stdout = self._stream_until_cancelled()

    async def _stream_until_cancelled(self) -> AsyncGenerator[str, None]:
        yield "line1\n"
        self.read_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.read_cancelled.set()
            raise


class ErrorProcess(FakeProcess):
    def __init__(self, error: ModalError) -> None:
        super().__init__([], 1)
        self._error = error
        self.stdout = self._stream_until_error()

    async def _stream_until_error(self) -> AsyncGenerator[bytes, None]:
        yield b"partial"
        raise self._error


class FakeFilesystem:
    def __init__(self, content: bytes = b"", error: ModalError | None = None) -> None:
        self.content = content
        self.error = error
        self.writes: list[tuple[bytes, str]] = []
        self.reads: list[str] = []
        self.write_bytes = _aio(self._write_bytes)
        self.read_bytes = _aio(self._read_bytes)

    async def _write_bytes(self, content: bytes, remote_path: str) -> None:
        if self.error is not None:
            raise self.error
        self.writes.append((content, remote_path))
        self.content = content

    async def _read_bytes(self, remote_path: str) -> bytes:
        if self.error is not None:
            raise self.error
        self.reads.append(remote_path)
        return self.content


class FakeInnerSandbox:
    def __init__(
        self,
        object_id: str = "sb-123",
        process: FakeProcess | None = None,
        file_content: bytes = b"",
        exec_error: ModalError | None = None,
        file_error: ModalError | None = None,
        poll_result: int | None = None,
        poll_results: list[int | ModalError | None] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.object_id = object_id
        self.commands: list[tuple[str, ...]] = []
        self.command_envs: list[dict[str, str | None] | None] = []
        self.terminated = False
        self._process = process or FakeProcess([], 0)
        self._exec_error = exec_error
        self._poll_result = poll_result
        self._poll_results = poll_results or []
        self._tags = dict(tags or {})
        self.filesystem = FakeFilesystem(file_content, file_error)
        self.outbound_policies: list[dict[str, list[str] | None]] = []
        self.exec = _aio(self._exec)
        self.terminate = _aio(self._terminate)
        self.poll = _aio(self._poll)
        self.get_tags = _aio(self._get_tags)
        self._experimental_set_outbound_network_policy = _aio(self._set_outbound_network_policy)

    async def _exec(
        self,
        *args: str,
        text: bool = True,
        env: dict[str, str | None] | None = None,
    ) -> FakeProcess:
        if self._exec_error is not None:
            raise self._exec_error
        self.commands.append(args)
        self.command_envs.append(env)
        return self._process

    async def _terminate(self, *, wait: bool = False) -> None:
        self.terminated = True
        self.terminate_wait = wait

    async def _get_tags(self) -> dict[str, str]:
        return dict(self._tags)

    async def _poll(self) -> int | None:
        # None means still running, mirroring modal.Sandbox.poll().
        if self._poll_results:
            poll_result = self._poll_results.pop(0)
            if isinstance(poll_result, ModalError):
                raise poll_result
            return poll_result
        return self._poll_result

    async def _set_outbound_network_policy(
        self,
        *,
        outbound_cidr_allowlist: list[str] | None = None,
        outbound_domain_allowlist: list[str] | None = None,
    ) -> None:
        self.outbound_policies.append(
            {
                "outbound_cidr_allowlist": outbound_cidr_allowlist,
                "outbound_domain_allowlist": outbound_domain_allowlist,
            }
        )


class FlakyExecSandbox(FakeInnerSandbox):
    def __init__(self) -> None:
        super().__init__(process=FakeProcess(["ok"], 0))
        self.exec_attempts = 0

    async def _exec(
        self,
        *args: str,
        text: bool = True,
        env: dict[str, str | None] | None = None,
    ) -> FakeProcess:
        self.exec_attempts += 1
        if self.exec_attempts == 1:
            raise ModalConnectionError("modal exec temporarily unavailable")
        return await super()._exec(*args, text=text, env=env)


def _request(
    source: ImageSource | SnapshotSource | TargetedSnapshotSource | DockerfileSource | None = None,
    resources: Resources | None = None,
    labels: dict[str, str] | None = None,
    volumes: list[VolumeMount] | None = None,
) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=source or ImageSource(image="python:3.12"),
        resources=resources or Resources(vcpu=4, memory=8, disk=30),
        name="task-1",
        labels={"run_id": "r1"} if labels is None else labels,
        env_vars={"FOO": "bar"},
        auto_stop_interval=30,
        create_timeout=120,
        volumes=volumes or [],
    )


def _provider(monkeypatch: pytest.MonkeyPatch, sdk_sandbox: Any) -> ModalSandboxProvider:
    client = SimpleNamespace(_close=_aio(_noop))
    app = SimpleNamespace(app_id="ap-1")

    async def from_credentials(token_id: str, token_secret: str) -> Any:
        return client

    async def lookup(name: str, **kwargs: Any) -> Any:
        assert kwargs["create_if_missing"] is True
        return app

    monkeypatch.setattr(modal_module, "Client", SimpleNamespace(from_credentials=_aio(from_credentials)))
    monkeypatch.setattr(modal_module, "App", SimpleNamespace(lookup=_aio(lookup)))
    monkeypatch.setattr(modal_module, "Image", SimpleNamespace(from_registry=_from_registry, from_id=_from_id))
    monkeypatch.setattr(modal_module, "ModalSdkSandbox", sdk_sandbox)
    return ModalSandboxProvider(_config())


async def _noop(*args: Any, **kwargs: Any) -> None:
    return None


def _from_registry(image: str) -> tuple[str, str]:
    return ("image", image)


def _from_id(snapshot: str, **kwargs: Any) -> tuple[str, str]:
    return ("snapshot", snapshot)


def _config() -> ModalProviderConfig:
    return ModalProviderConfig(MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret")


def _sandbox(inner: FakeInnerSandbox, name: str | None = None) -> ModalSandbox:
    return ModalSandbox(cast(ModalSdkSandbox, inner), name=name)


def test_modal_config_reads_secrets_manager_shape() -> None:
    config = sandbox_provider_config_from_mapping(
        {
            "type": "modal",
            "runtime": "vm",
            "MODAL_TOKEN_ID": "id",
            "MODAL_TOKEN_SECRET": "secret",
            "MODAL_ENVIRONMENT": "legacy-ignored",
        }
    )
    assert config == ModalProviderConfig(runtime="vm", MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret")


def test_command_merges_stderr_and_applies_timeout_inside_cwd() -> None:
    command = modal_module._command("echo hi", "/workspace", 60)
    assert command == "{ cd /workspace && timeout 60 echo hi ; } 2>&1"


def test_command_preserves_fractional_timeout() -> None:
    assert "timeout 1.5 " in modal_module._command("true", None, 1.5)


async def test_exec_returns_combined_output() -> None:
    inner = FakeInnerSandbox(process=FakeProcess(["out", "err"], 3))
    sandbox = _sandbox(inner)

    result = await sandbox.exec("boom", timeout=10)

    assert result.exit_code == 3
    assert result.stdout == "outerr"
    assert inner.commands == [("/bin/sh", "-lc", "{ timeout 10 boom ; } 2>&1")]


async def test_exec_wraps_modal_errors() -> None:
    sandbox = _sandbox(FakeInnerSandbox(exec_error=ModalError("boom")))

    with pytest.raises(SandboxError):
        await sandbox.exec("true")


async def test_exec_retries_modal_connection_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cast(Any, modal_module.ModalSandbox._start_process).retry, "sleep", _noop)
    inner = FlakyExecSandbox()
    sandbox = _sandbox(inner)

    result = await sandbox.exec("true")

    assert result.exit_code == 0
    assert result.output == "ok"
    assert inner.exec_attempts == 2


async def test_file_operations_map_not_found_errors() -> None:
    sandbox = _sandbox(FakeInnerSandbox(file_error=ModalNotFoundError("sandbox removed")))

    with pytest.raises(SandboxNotFoundError, match="sandbox removed"):
        await sandbox.upload_file("/tmp/result.txt", b"hello")

    with pytest.raises(SandboxNotFoundError, match="sandbox removed"):
        await sandbox.download_file("/tmp/result.txt")


async def test_command_streams_output_and_raises_on_failure() -> None:
    inner = FakeInnerSandbox(process=FakeProcess(["line1\n", "line2\n"], 7))

    async def delayed_wait() -> int:
        await asyncio.sleep(0.05)
        return 7

    inner._process.wait = _aio(delayed_wait)
    sandbox = _sandbox(inner)

    chunks: list[str] = []
    with pytest.raises(SandboxCommandError) as exc:
        async for chunk in sandbox.command("run"):
            chunks.append(chunk)

    assert chunks == ["line1\n", "line2\n"]
    assert exc.value.exit_code == 7


async def test_command_drains_output_after_process_finishes() -> None:
    inner = FakeInnerSandbox(process=FakeProcess([], 0))

    async def delayed_output() -> AsyncGenerator[str, None]:
        yield "first"
        await asyncio.sleep(0.05)
        yield "late"

    inner._process.stdout = delayed_output()
    sandbox = _sandbox(inner)

    assert [chunk async for chunk in sandbox.command("run")] == ["first", "late"]


async def test_command_drains_continuous_output_after_process_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(modal_module, "_COMMAND_STATUS_POLL_SECONDS", 0.05)
    inner = FakeInnerSandbox(process=FakeProcess([], 0))
    expected_chunks = [f"chunk-{index}" for index in range(20)]

    async def continuous_output() -> AsyncGenerator[str, None]:
        for chunk in expected_chunks:
            yield chunk
            await asyncio.sleep(0.01)

    inner._process.stdout = continuous_output()
    sandbox = _sandbox(inner)

    assert [chunk async for chunk in sandbox.command("run")] == expected_chunks


@pytest.mark.parametrize(
    ("modal_error", "expected_error", "message"),
    [
        (ModalConnectionError("command disconnected"), SandboxConnectionError, "command disconnected"),
        (ModalConflictError("Modal Sandbox is shutting down."), SandboxNotFoundError, "shutting down"),
    ],
)
async def test_command_maps_stream_errors(
    modal_error: ModalError,
    expected_error: type[SandboxError],
    message: str,
) -> None:
    inner = FakeInnerSandbox(process=ErrorProcess(modal_error))
    sandbox = _sandbox(inner)
    chunks: list[str] = []

    with pytest.raises(expected_error, match=message):
        async for chunk in sandbox.command("run"):
            chunks.append(chunk)

    assert chunks == ["b'partial'"]


async def test_command_uses_native_process_environment() -> None:
    secret = "value with spaces; $(touch /tmp/leaked)"
    inner = FakeInnerSandbox(process=FakeProcess(["ok"], 0))
    sandbox = _sandbox(inner)

    chunks = [chunk async for chunk in sandbox.command("run", env_vars={"AGENT_SECRET": secret})]

    assert chunks == ["ok"]
    assert inner.command_envs == [{"AGENT_SECRET": secret}]
    assert all(secret not in part for command in inner.commands for part in command)


async def test_command_rejects_invalid_environment_names_before_provider_call() -> None:
    inner = FakeInnerSandbox()
    sandbox = _sandbox(inner)

    with pytest.raises(ValueError, match="BAD-NAME"):
        _ = [chunk async for chunk in sandbox.command("run", env_vars={"BAD-NAME": "secret"})]

    assert inner.commands == []


async def test_command_maps_terminated_sandbox_to_not_found() -> None:
    inner = FakeInnerSandbox(process=FakeProcess(["line1\n"], 137), poll_results=[None, None, 137])
    sandbox = _sandbox(inner)
    chunks: list[str] = []

    with pytest.raises(SandboxNotFoundError):
        async for chunk in sandbox.command("run"):
            chunks.append(chunk)

    assert chunks == ["line1\n"]


async def test_command_detects_termination_while_output_stalls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Detect remote termination even when Modal leaves the output iterator open.

    Test cases:
    - A stalled stream raises SandboxNotFoundError after the sandbox poll reports termination.
    - A transient poll failure is retried without aborting the command first.
    """
    monkeypatch.setattr(modal_module, "_COMMAND_STATUS_POLL_SECONDS", 0.01)
    monkeypatch.setattr(cast(Any, modal_module.ModalSandbox._raise_if_finished).retry, "sleep", _noop)
    process = HangingProcess()

    async def pending_wait() -> int:
        await asyncio.Event().wait()
        return 137

    process.wait = _aio(pending_wait)
    sandbox = _sandbox(
        FakeInnerSandbox(
            process=process,
            poll_results=[None, ModalConnectionError("temporary poll failure"), 137],
        )
    )

    async def consume_command() -> list[str]:
        return [chunk async for chunk in sandbox.command("run")]

    with pytest.raises(SandboxNotFoundError, match="poll_result=137"):
        await asyncio.wait_for(consume_command(), timeout=1)


async def test_command_unblocks_when_process_finishes_before_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(modal_module, "_COMMAND_STATUS_POLL_SECONDS", 0.01)
    process = HangingProcess()

    async def finished_wait() -> int:
        return 137

    process.wait = _aio(finished_wait)
    sandbox = _sandbox(FakeInnerSandbox(process=process, poll_results=[None, 137]))

    async def consume_command() -> list[str]:
        return [chunk async for chunk in sandbox.command("run")]

    with pytest.raises(SandboxNotFoundError):
        await asyncio.wait_for(consume_command(), timeout=1)

    assert process.read_cancelled.is_set()


async def test_command_cancels_pending_stream_read() -> None:
    process = HangingProcess()
    wait_cancelled = asyncio.Event()

    async def pending_wait() -> int:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            wait_cancelled.set()
            raise
        return 137

    process.wait = _aio(pending_wait)
    sandbox = _sandbox(FakeInnerSandbox(process=process))

    async def consume_command() -> None:
        async for _ in sandbox.command("run"):
            pass

    consumer = asyncio.create_task(consume_command())
    await asyncio.wait_for(process.read_started.wait(), timeout=1)

    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer

    assert process.read_cancelled.is_set()
    assert wait_cancelled.is_set()


async def test_command_reports_modal_poll_result_when_sandbox_finished() -> None:
    """Verify finished Modal sandboxes include their poll result in the surfaced error.

    Test cases:
    - A terminated sandbox poll result is included in the SandboxNotFoundError message.
    """
    sandbox = _sandbox(FakeInnerSandbox(process=FakeProcess([], 137), poll_results=[None, 137]))

    with pytest.raises(SandboxNotFoundError, match="poll_result=137"):
        async for _ in sandbox.command("run"):
            pass


async def test_upload_file_uses_modal_filesystem() -> None:
    inner = FakeInnerSandbox()
    sandbox = _sandbox(inner)

    await sandbox.upload_file("/tmp/nested/problem.txt", b"hello")

    assert inner.filesystem.writes == [(b"hello", "/tmp/nested/problem.txt")]
    assert inner.filesystem.content == b"hello"


async def test_file_operations_raise_not_found_when_sandbox_finished() -> None:
    sandbox = _sandbox(FakeInnerSandbox(poll_result=137))

    with pytest.raises(SandboxNotFoundError):
        await sandbox.upload_file("/tmp/result.txt", b"hello")

    with pytest.raises(SandboxNotFoundError):
        await sandbox.download_file("/tmp/result.txt")


async def test_download_file_returns_bytes() -> None:
    inner = FakeInnerSandbox(file_content=b"data")
    sandbox = _sandbox(inner)

    assert await sandbox.download_file("/tmp/out.bin") == b"data"
    assert inner.filesystem.reads == ["/tmp/out.bin"]


async def test_stream_download_yields_chunks() -> None:
    """Verify Modal file downloads stream binary chunks without buffering the whole file.

    Test cases:
    - Multiple binary chunks are yielded in order.
    """
    inner = FakeInnerSandbox(process=FakeProcess([b"hello", b" world"], 0), file_content=b"buffered fallback")
    sandbox = _sandbox(inner)

    chunks = [chunk async for chunk in sandbox.stream_download("/tmp/out.bin")]

    assert chunks == [b"hello", b" world"]


class ConnectionLostThenApplyPolicySandbox(FakeInnerSandbox):
    def __init__(self) -> None:
        super().__init__()
        self.policy_attempts = 0

    async def _set_outbound_network_policy(
        self,
        *,
        outbound_cidr_allowlist: list[str] | None = None,
        outbound_domain_allowlist: list[str] | None = None,
    ) -> None:
        self.policy_attempts += 1
        if self.policy_attempts == 1:
            raise ModalConnectionError("outbound policy update disconnected")
        await super()._set_outbound_network_policy(
            outbound_cidr_allowlist=outbound_cidr_allowlist,
            outbound_domain_allowlist=outbound_domain_allowlist,
        )


async def test_egress_rule_updates_retry_connection_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transport failure while setting the outbound policy is retried by the provider."""

    async def _noop(_seconds: float) -> None:
        pass

    monkeypatch.setattr(cast(Any, modal_module.ModalSandbox._set_outbound_network_policy).retry, "sleep", _noop)
    inner = ConnectionLostThenApplyPolicySandbox()
    sandbox = _sandbox(inner)

    await sandbox.block_all_egress()

    assert inner.policy_attempts == 2
    assert inner.outbound_policies == [{"outbound_cidr_allowlist": [], "outbound_domain_allowlist": []}]


async def test_egress_rule_updates_replace_outbound_policy() -> None:
    """Verify Modal egress updates call the SDK outbound policy API.

    Test cases:
    - URLs, domains, and IPv4 addresses are split into Modal domain and CIDR allowlists.
    - Deny-all uses empty provider allowlists without overloading modify_egress_rules([]).
    - Clearing egress rules uses an allow-all policy that Modal accepts on a running sandbox.
    """
    inner = FakeInnerSandbox(process=FakeProcess(["198.51.100.8\n198.51.100.9\n"], 0))
    sandbox = _sandbox(inner)

    await sandbox.modify_egress_rules(["https://api.openai.com/v1", "github.com", "203.0.113.10"])

    assert inner.outbound_policies[-1] == {
        "outbound_cidr_allowlist": ["203.0.113.10/32", "198.51.100.8/32", "198.51.100.9/32"],
        "outbound_domain_allowlist": ["api.openai.com", "github.com"],
    }

    with pytest.raises(ValueError, match="allowed addresses cannot be empty"):
        await sandbox.modify_egress_rules([])

    await sandbox.block_all_egress()

    assert inner.outbound_policies[-1] == {
        "outbound_cidr_allowlist": [],
        "outbound_domain_allowlist": [],
    }

    await sandbox.clear_egress_rules()

    assert inner.outbound_policies[-1] == {
        "outbound_cidr_allowlist": ["0.0.0.0/0"],
        "outbound_domain_allowlist": ["*"],
    }


async def test_create_sandbox_maps_request(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = FakeInnerSandbox()
    captured: dict[str, Any] = {}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured["entrypoint"] = args
        captured.update(kwargs)
        return inner

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    sandbox = await provider.create_sandbox(_request())

    assert sandbox.id == "sb-123"
    assert sandbox.name == "task-1"
    # No entrypoint args: an argless Modal sandbox idles until timeout.
    assert captured["entrypoint"] == ()
    assert captured["image"] == ("image", "python:3.12")
    assert captured["env"] == {"FOO": "bar"}
    assert captured["tags"] == {"run_id": "r1"}
    assert captured["cpu"] == 4.0
    assert captured["memory"] == 8192
    assert captured["idle_timeout"] == 1800
    assert captured["timeout"] == 86400
    assert captured["block_network"] is False
    assert captured["outbound_cidr_allowlist"] == ["0.0.0.0/0"]
    assert captured["outbound_domain_allowlist"] == ["*"]
    assert captured["runtime"] == "gvisor"
    assert captured["experimental_options"] == {"enable_docker": True}
    assert captured["gpu"] is None


def _with_dockerfile_builds(monkeypatch: pytest.MonkeyPatch, built: list[tuple[str, dict[str, Any]]]) -> None:
    def from_dockerfile(path: str, **kwargs: Any) -> tuple[str, str]:
        built.append((path, kwargs))
        return ("dockerfile", path)

    monkeypatch.setattr(
        modal_module,
        "Image",
        SimpleNamespace(from_registry=_from_registry, from_id=_from_id, from_dockerfile=from_dockerfile),
    )


async def test_create_sandbox_builds_a_dockerfile_source_from_its_folder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, Any] = {}
    built: list[tuple[str, dict[str, Any]]] = []

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return FakeInnerSandbox()

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))
    _with_dockerfile_builds(monkeypatch, built)
    dockerfile = str(tmp_path / "Dockerfile")

    await provider.create_sandbox(_request(DockerfileSource(dockerfile=dockerfile)))

    assert built == [(dockerfile, {"context_dir": str(tmp_path)})]
    assert captured["image"] == ("dockerfile", dockerfile)


async def test_create_sandbox_reports_a_failed_build_with_modals_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        raise ModalImageBuildError("Image build failed\nstep 2 exited with 1", "im-123")

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))
    _with_dockerfile_builds(monkeypatch, [])

    with pytest.raises(SandboxBuildError, match="Modal could not build the Dockerfile") as error:
        await provider.create_sandbox(_request(DockerfileSource(dockerfile=str(tmp_path / "Dockerfile"))))

    assert error.value.build_log == "Image build failed\nstep 2 exited with 1"


async def test_create_sandbox_uses_vm_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return FakeInnerSandbox()

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))
    provider._config.runtime = "vm"

    await provider.create_sandbox(_request())

    assert captured["runtime"] == "vm"
    assert captured["experimental_options"] is None


async def test_create_sandbox_maps_gpu_request(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = FakeInnerSandbox()
    captured: dict[str, Any] = {}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return inner

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    await provider.create_sandbox(_request(resources=Resources(vcpu=4, memory=8, disk=30, gpu=2, gpu_type="H100")))

    assert captured["gpu"] == "H100:2"


async def test_create_sandbox_requires_gpu_type_for_gpu(monkeypatch: pytest.MonkeyPatch) -> None:
    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        raise AssertionError("create should not be called")

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    with pytest.raises(SandboxError, match="requires gpu_type"):
        await provider.create_sandbox(_request(resources=Resources(vcpu=4, memory=8, disk=30, gpu=1)))


async def test_create_sandbox_blocks_network_without_conflicting_allowlists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return FakeInnerSandbox()

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))
    request = _request().model_copy(update={"network_block_all": True})

    await provider.create_sandbox(request)

    assert captured["block_network"] is True
    assert captured["outbound_cidr_allowlist"] is None
    assert captured["outbound_domain_allowlist"] is None


async def test_create_sandbox_uses_modal_safe_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the requested name while Modal receives a safe allocation name."""
    inner = FakeInnerSandbox()
    captured: dict[str, Any] = {}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return inner

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))
    request = SandboxCreateRequest(
        source=ImageSource(image="python:3.12"),
        resources=Resources(vcpu=4, memory=8, disk=30),
        name=f"dataset/task:with/slashes-{'x' * 80}",
        labels={"run_id": "r1"},
        env_vars={"FOO": "bar"},
        auto_stop_interval=30,
        create_timeout=120,
    )

    sandbox = await provider.create_sandbox(request)

    assert sandbox.name == request.name
    assert sandbox.labels == request.labels
    assert "/" not in captured["name"]
    assert ":" not in captured["name"]
    assert len(captured["name"]) <= 64


async def test_create_sandbox_does_not_retry_ambiguous_connection_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        nonlocal attempts
        attempts += 1
        raise ModalConnectionError("modal create outcome unknown")

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    with pytest.raises(SandboxConnectionError, match="outcome unknown"):
        await provider.create_sandbox(_request())
    assert attempts == 1


async def test_create_sandbox_restores_snapshot_source(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = FakeInnerSandbox()
    captured: dict[str, Any] = {}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return inner

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    sandbox = await provider.create_sandbox(_request(source=SnapshotSource(snapshot="im-123")))

    assert sandbox.id == "sb-123"
    assert captured["image"] == ("snapshot", "im-123")


async def test_create_sandbox_rejects_targeted_snapshot_source(monkeypatch: pytest.MonkeyPatch) -> None:
    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        raise AssertionError("create should not be called")

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    with pytest.raises(SandboxError, match="does not support source type: targeted_snapshot"):
        await provider.create_sandbox(
            _request(source=TargetedSnapshotSource(snapshot="masscan-snapshot", target="us-west-3"))
        )


async def test_get_sandbox_raises_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    async def from_id(instance_id: str, **kwargs: Any) -> Any:
        raise ModalNotFoundError(f"missing {instance_id}")

    provider = _provider(monkeypatch, SimpleNamespace(from_id=_aio(from_id)))

    with pytest.raises(SandboxNotFoundError):
        await provider.get_sandbox("sb-missing")


async def test_get_sandbox_maps_invalid_id_to_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    async def from_id(instance_id: str, **kwargs: Any) -> Any:
        raise ModalInvalidError(f"Invalid Sandbox ID: {instance_id!r}")

    provider = _provider(monkeypatch, SimpleNamespace(from_id=_aio(from_id)))

    with pytest.raises(SandboxNotFoundError, match="id=sandbox-name"):
        await provider.get_sandbox("sandbox-name")


async def test_get_sandbox_fetches_modal_tags_with_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = FakeInnerSandbox(tags={"Id": "benchmark-1", "Task": "task-1"})
    attempts = 0

    async def get_tags() -> dict[str, str]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ModalConnectionError("tags temporarily unavailable")
        return {"Id": "benchmark-1", "Task": "task-1"}

    inner.get_tags = _aio(get_tags)

    async def from_id(instance_id: str, **kwargs: Any) -> FakeInnerSandbox:
        return inner

    monkeypatch.setattr(cast(Any, ModalSandboxProvider._fetch_labels).retry, "sleep", _noop)
    provider = _provider(monkeypatch, SimpleNamespace(from_id=_aio(from_id)))

    sandbox = await provider.get_sandbox("sb-123")

    assert sandbox.labels == {"Id": "benchmark-1", "Task": "task-1"}
    assert attempts == 2


async def test_get_sandbox_raises_not_found_for_finished_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    async def from_id(instance_id: str, **kwargs: Any) -> Any:
        return FakeInnerSandbox(object_id=instance_id, poll_result=137)

    provider = _provider(monkeypatch, SimpleNamespace(from_id=_aio(from_id)))

    with pytest.raises(SandboxNotFoundError, match="id=sb-finished"):
        await provider.get_sandbox("sb-finished")


async def test_delete_sandbox_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    async def from_id(instance_id: str, **kwargs: Any) -> Any:
        raise ModalNotFoundError(f"missing {instance_id}")

    provider = _provider(monkeypatch, SimpleNamespace(from_id=_aio(from_id)))

    await provider.delete_sandbox("sb-missing")


async def test_delete_sandbox_terminates(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = FakeInnerSandbox()

    async def from_id(instance_id: str, **kwargs: Any) -> Any:
        assert instance_id == "sb-123"
        return inner

    provider = _provider(monkeypatch, SimpleNamespace(from_id=_aio(from_id)))

    await provider.delete_sandbox("sb-123")

    assert inner.terminated
    assert inner.terminate_wait is True


async def test_list_sandboxes_filters_by_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = FakeInnerSandbox(tags={"run_id": "r1", "Task": "task-1"})
    finished = FakeInnerSandbox(object_id="sb-finished", poll_result=137)
    captured: dict[str, Any] = {}

    def list_sandboxes(**kwargs: Any) -> AsyncGenerator[FakeInnerSandbox, None]:
        captured.update(kwargs)

        async def iterate() -> AsyncGenerator[FakeInnerSandbox, None]:
            yield inner
            yield finished

        return iterate()

    provider = _provider(monkeypatch, SimpleNamespace(list=_aio(list_sandboxes)))

    sandboxes = [sandbox async for sandbox in provider.list_sandboxes(SandboxQuery(labels={"run_id": "r1"}))]

    assert [sandbox.id for sandbox in sandboxes] == ["sb-123"]
    assert sandboxes[0].labels == {"run_id": "r1", "Task": "task-1"}
    assert captured["app_id"] == "ap-1"
    assert captured["tags"] == {"run_id": "r1"}


async def test_list_sandboxes_rejects_creation_time_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _provider(monkeypatch, SimpleNamespace())
    query = SandboxQuery(
        labels={},
        created_at_lte=datetime(2026, 7, 24, 12, 30, tzinfo=UTC),
    )

    with pytest.raises(SandboxError, match="does not support filtering by creation time"):
        _ = [sandbox async for sandbox in provider.list_sandboxes(query)]


async def test_create_sandbox_reports_resource_exhaustion_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0
    exhausted = ModalResourceExhaustedError("Insufficient capacity to create sandbox.")

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        nonlocal attempts
        attempts += 1
        raise exhausted

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    with pytest.raises(SandboxError, match="resources exhausted: Insufficient capacity") as error:
        await provider.create_sandbox(_request().model_copy(update={"create_timeout": 1}))
    assert error.value.__cause__ is exhausted
    assert attempts == 1


async def test_create_sandbox_without_volumes_passes_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default stays byte-identical to pre-volume behaviour."""
    captured: dict[str, Any] = {}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return FakeInnerSandbox()

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))
    await provider.create_sandbox(_request())

    assert "volumes" not in captured


async def test_create_sandbox_mounts_named_volumes(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fixture too large to bake into an image is mounted, keyed by path."""
    captured: dict[str, Any] = {}
    looked_up: list[tuple[str, bool]] = []

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return FakeInnerSandbox()

    def from_name(name: str, create_if_missing: bool = False, client: Any = None) -> str:
        looked_up.append((name, create_if_missing))
        return f"vol::{name}"

    monkeypatch.setattr(modal_module, "Volume", SimpleNamespace(from_name=from_name))
    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    await provider.create_sandbox(_request(volumes=[VolumeMount(name="kspbench-steam", mount_path="/vol")]))

    assert captured["volumes"] == {"/vol": "vol::kspbench-steam"}
    assert looked_up == [("kspbench-steam", False)]


async def test_create_sandbox_applies_read_only_and_run_scoped_subpath(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    mount_options: list[tuple[bool | None, str | None]] = []
    volume = SimpleNamespace()

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return FakeInnerSandbox()

    def with_mount_options(*, read_only: bool | None = None, sub_path: str | None = None) -> Any:
        mount_options.append((read_only, sub_path))
        return ("configured-volume", read_only, sub_path)

    volume.with_mount_options = with_mount_options

    def from_name(name: str, create_if_missing: bool = False, client: Any = None) -> Any:
        return volume

    monkeypatch.setattr(modal_module, "Volume", SimpleNamespace(from_name=from_name))
    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    await provider.create_sandbox(
        _request(
            labels={"run-id": "run-123"},
            volumes=[
                VolumeMount(
                    name="fixtures",
                    mount_path="/fixtures",
                    read_only=True,
                    subpath="runs/{run_id}",
                )
            ],
        )
    )

    assert mount_options == [(True, "runs/run-123")]
    assert captured["volumes"] == {
        "/fixtures": ("configured-volume", True, "runs/run-123"),
    }


async def test_create_sandbox_reports_a_missing_volume_clearly(monkeypatch: pytest.MonkeyPatch) -> None:
    """A run must not start with an empty directory where its fixture belongs."""

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        raise ModalNotFoundError("Volume 'absent' not found")

    def from_name(name: str, create_if_missing: bool = False, client: Any = None) -> str:
        return f"vol::{name}"

    monkeypatch.setattr(modal_module, "Volume", SimpleNamespace(from_name=from_name))
    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    with pytest.raises(SandboxError, match="volume mount failed") as raised:
        await provider.create_sandbox(_request(volumes=[VolumeMount(name="absent", mount_path="/vol")]))

    assert type(raised.value) is SandboxError
