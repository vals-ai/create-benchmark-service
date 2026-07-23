from __future__ import annotations

# pyright: reportPrivateUsage=false

import asyncio
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any, cast

import pytest
from modal import Sandbox as ModalSdkSandbox
from modal.exception import ConnectionError as ModalConnectionError
from modal.exception import Error as ModalError
from modal.exception import InvalidError as ModalInvalidError
from modal.exception import NotFoundError as ModalNotFoundError

import benchmark_service.sandbox.modal as modal_module
from benchmark_service.sandbox import sandbox_provider_config_from_mapping
from benchmark_service.sandbox.modal import ModalProviderConfig, ModalSandbox, ModalSandboxProvider
from benchmark_service.sandbox.types import (
    ImageSource,
    Resources,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
    SnapshotSource,
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
        self.stdout = self._stream_until_cancelled()

    async def _stream_until_cancelled(self) -> AsyncGenerator[str, None]:
        yield "line1\n"
        await asyncio.Event().wait()


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
    ) -> None:
        self.object_id = object_id
        self.commands: list[tuple[str, ...]] = []
        self.command_envs: list[dict[str, str | None] | None] = []
        self.terminated = False
        self._process = process or FakeProcess([], 0)
        self._exec_error = exec_error
        self._poll_result = poll_result
        self._poll_results = poll_results or []
        self.filesystem = FakeFilesystem(file_content, file_error)
        self.outbound_policies: list[dict[str, list[str] | None]] = []
        self.exec = _aio(self._exec)
        self.terminate = _aio(self._terminate)
        self.poll = _aio(self._poll)
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

    async def _terminate(self) -> None:
        self.terminated = True

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
    source: ImageSource | SnapshotSource | None = None,
    resources: Resources | None = None,
) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=source or ImageSource(image="python:3.12"),
        resources=resources or Resources(vcpu=4, memory=8, disk=30),
        name="task-1",
        labels={"run_id": "r1"},
        env_vars={"FOO": "bar"},
        auto_stop_interval=30,
        create_timeout=120,
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
    if not hasattr(sdk_sandbox, "from_name"):
        # Default: no existing sandbox holds the requested name.
        async def _no_named_sandbox(*args: Any, **kwargs: Any) -> Any:
            raise ModalNotFoundError("no sandbox with that name")

        sdk_sandbox.from_name = _aio(_no_named_sandbox)
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
            "MODAL_TOKEN_ID": "id",
            "MODAL_TOKEN_SECRET": "secret",
            "MODAL_ENVIRONMENT": "legacy-ignored",
        }
    )
    assert config == ModalProviderConfig(MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret")


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
    sandbox = _sandbox(inner)

    chunks: list[str] = []
    with pytest.raises(SandboxCommandError) as exc:
        async for chunk in sandbox.command("run"):
            chunks.append(chunk)

    assert chunks == ["line1\n", "line2\n"]
    assert exc.value.exit_code == 7


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
    sandbox = _sandbox(
        FakeInnerSandbox(
            process=HangingProcess(),
            poll_results=[None, ModalConnectionError("temporary poll failure"), 137],
        )
    )

    async def consume_command() -> list[str]:
        return [chunk async for chunk in sandbox.command("run")]

    with pytest.raises(SandboxNotFoundError, match="poll_result=137"):
        await asyncio.wait_for(consume_command(), timeout=1)


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


async def test_egress_rule_updates_replace_outbound_policy() -> None:
    """Verify Modal egress updates call the SDK outbound policy API.

    Test cases:
    - URLs, domains, and IPv4 addresses are split into Modal domain and CIDR allowlists.
    - Clearing egress rules restores Modal's open outbound policy.
    """
    inner = FakeInnerSandbox(process=FakeProcess(["198.51.100.8\n198.51.100.9\n"], 0))
    sandbox = _sandbox(inner)

    await sandbox.modify_egress_rules(["https://api.openai.com/v1", "github.com", "203.0.113.10"])

    assert inner.outbound_policies[-1] == {
        "outbound_cidr_allowlist": ["203.0.113.10/32", "198.51.100.8/32", "198.51.100.9/32"],
        "outbound_domain_allowlist": ["api.openai.com", "github.com"],
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
    # Nested-Docker capability is requested unconditionally, matching Daytona
    # sandboxes which always support it.
    assert captured["experimental_options"] == {"enable_docker": True}
    assert captured["gpu"] is None


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
    """Verify Modal receives a safe name while callers keep the requested sandbox name.

    Test cases:
    - Unsafe and long task names are normalized before Modal lookup and creation.
    - The returned sandbox name remains the original benchmark task name.
    """
    inner = FakeInnerSandbox()
    captured: dict[str, Any] = {"lookups": []}

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        captured.update(kwargs)
        return inner

    async def from_name(app_name: str, name: str, **kwargs: Any) -> FakeInnerSandbox:
        captured["lookups"].append(name)
        raise ModalNotFoundError("no sandbox with that name")

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create), from_name=_aio(from_name)))
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
    assert captured["lookups"] == [captured["name"]]
    assert "/" not in captured["name"]
    assert ":" not in captured["name"]
    assert len(captured["name"]) <= 64


def test_modal_sandbox_name_avoids_app_id_shape() -> None:
    """Verify names shaped like Modal app IDs are not passed through unchanged.

    Test cases:
    - A task name matching Modal's app-id shape is prefixed before SDK use.
    """
    modal_app_id_name = "ap-" + ("a" * 22)

    assert modal_module._modal_sandbox_name(modal_app_id_name) == f"sandbox-{modal_app_id_name}"


async def test_create_sandbox_retries_modal_connection_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cast(Any, modal_module.ModalSandboxProvider.create_sandbox).retry, "sleep", _noop)
    inner = FakeInnerSandbox()
    attempts = 0

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ModalConnectionError("modal create temporarily unavailable")
        return inner

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create)))

    sandbox = await provider.create_sandbox(_request())

    assert sandbox.id == "sb-123"
    assert attempts == 2


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


async def test_list_sandboxes_filters_by_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = FakeInnerSandbox()
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
    assert captured["app_id"] == "ap-1"
    assert captured["tags"] == {"run_id": "r1"}


async def test_create_sandbox_reuses_running_sandbox_with_same_name(monkeypatch: pytest.MonkeyPatch) -> None:
    running = FakeInnerSandbox(object_id="sb-existing")  # poll_result=None: still running
    created: list[str] = []

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        created.append(kwargs["name"])
        return FakeInnerSandbox(object_id="sb-new")

    async def from_name(app_name: str, name: str, **kwargs: Any) -> FakeInnerSandbox:
        assert (app_name, name) == (modal_module._APP_NAME, "task-1")
        return running

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create), from_name=_aio(from_name)))

    sandbox = await provider.create_sandbox(_request())

    assert sandbox.id == "sb-existing"
    assert sandbox.name == "task-1"
    assert created == []


async def test_create_sandbox_ignores_finished_sandbox_with_same_name(monkeypatch: pytest.MonkeyPatch) -> None:
    finished = FakeInnerSandbox(object_id="sb-finished", poll_result=0)  # exited

    async def create(*args: str, **kwargs: Any) -> FakeInnerSandbox:
        return FakeInnerSandbox(object_id="sb-new")

    async def from_name(app_name: str, name: str, **kwargs: Any) -> FakeInnerSandbox:
        return finished

    provider = _provider(monkeypatch, SimpleNamespace(create=_aio(create), from_name=_aio(from_name)))

    sandbox = await provider.create_sandbox(_request())

    assert sandbox.id == "sb-new"
