from types import SimpleNamespace
from typing import Any, cast

from daytona import SandboxState

from benchmark_service.sandbox import DaytonaSandbox, DaytonaSandboxProvider, ImageSource, Resources, SandboxCreateRequest


class _Process:
    command: str | None = None

    async def exec(self, command: str) -> SimpleNamespace:
        self.command = command
        return SimpleNamespace(exit_code=0, result="")


class _Fs:
    async def download_file_stream(self, remote_path: str) -> Any:
        assert remote_path == "/tmp/result.txt"

        async def chunks() -> Any:
            yield b"hello"
            yield b" world"

        return chunks()


class _Inner:
    id = "sandbox-id"
    name = "sandbox-name"
    state = SandboxState.STARTED

    def __init__(self) -> None:
        self.process = _Process()
        self.fs = _Fs()
        self.autostop_interval: int | None = None

    async def set_autostop_interval(self, interval: int) -> None:
        self.autostop_interval = interval

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        assert timeout == 0


class _Daytona:
    def __init__(self, sandbox: _Inner) -> None:
        self.sandbox = sandbox
        self.created = False
        self.deleted = False

    async def get(self, instance_id: str) -> _Inner:
        assert instance_id == self.sandbox.name
        return self.sandbox

    async def create(self, *_args: object, **_kwargs: object) -> _Inner:
        self.created = True
        return self.sandbox

    async def delete(self, sandbox: _Inner) -> None:
        assert sandbox is self.sandbox
        self.deleted = True


def _provider(daytona: _Daytona) -> DaytonaSandboxProvider:
    provider = DaytonaSandboxProvider.__new__(DaytonaSandboxProvider)
    provider._daytona = cast(Any, daytona)  # pyright: ignore[reportPrivateUsage]
    return provider


def _request(name: str) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=ImageSource(image="python:3.12"),
        resources=Resources(vcpu=2, memory=4, disk=10),
        name=name,
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )


async def test_daytona_command_applies_timeout_inside_cwd() -> None:
    inner = _Inner()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", cwd="/workspace", timeout=60)

    assert inner.process.command == "cd /workspace && timeout 60 pytest"


async def test_daytona_command_preserves_fractional_timeout() -> None:
    inner = _Inner()
    sandbox = DaytonaSandbox(cast(Any, inner))

    await sandbox.exec("pytest", timeout=0.5)

    assert inner.process.command == "timeout 0.5 pytest"


async def test_daytona_download_file_streams_content() -> None:
    sandbox = DaytonaSandbox(cast(Any, _Inner()))

    assert await sandbox.download_file("/tmp/result.txt") == b"hello world"


async def test_daytona_provider_reuses_started_sandbox() -> None:
    inner = _Inner()
    daytona = _Daytona(inner)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.inner is inner
    assert daytona.created is False


async def test_daytona_provider_delete_sets_autostop_before_delete() -> None:
    inner = _Inner()
    daytona = _Daytona(inner)

    await _provider(daytona).delete_sandbox(inner.name)

    assert inner.autostop_interval == 1
    assert daytona.deleted is True
