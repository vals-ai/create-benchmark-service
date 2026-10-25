"""Dockerfile sandbox source checks; run pytest tests/test_dockerfile_source.py."""

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from benchmark_service import (
    DockerfileSource,
    ImageSource,
    Resources,
    SandboxBuildError,
    SandboxCreateRequest,
    SandboxError,
)
from benchmark_service.sandbox.local.docker import DockerSandboxProvider
from benchmark_service.schemas import RetrieveTaskResponse

DOCKERFILE = "/tasks/a/environment/Dockerfile"
IMAGE_ID = "sha256:0123abcd"


def test_a_relative_dockerfile_path_is_rejected() -> None:
    with pytest.raises(ValueError, match="must be an absolute path"):
        DockerfileSource(dockerfile="environment/Dockerfile")


def test_the_source_survives_the_wire_and_has_a_legacy_image_name() -> None:
    response = RetrieveTaskResponse(
        source=DockerfileSource(dockerfile=DOCKERFILE),
        problem_path="/tmp/problem.txt",
        cwd="/workspace",
        agent_timeout=60.0,
        resources=Resources(vcpu=1, memory=1, disk=1),
    )

    restored = RetrieveTaskResponse.model_validate_json(response.model_dump_json())

    assert restored.source == DockerfileSource(dockerfile=DOCKERFILE)
    assert restored.docker_image == "dockerfile+source-required"
    request = SandboxCreateRequest.model_validate_json(
        SandboxCreateRequest(
            source=restored.source,
            resources=restored.resources,
            name="task",
            labels={},
            env_vars={},
            auto_stop_interval=10,
            create_timeout=60,
        ).model_dump_json()
    )
    assert request.source == restored.source


class FakeBuild:
    def __init__(self, output: bytes, returncode: int, *, hangs: bool = False) -> None:
        self.stdout = asyncio.StreamReader()
        self.stdout.feed_data(output)
        if not hangs:
            self.stdout.feed_eof()
        self.returncode: int | None = None
        self._exit_code = returncode

    async def wait(self) -> int:
        if self.returncode is None:
            self.returncode = self._exit_code
        return self.returncode

    def kill(self) -> None:
        self.returncode = -9


def _provider(docker_host: str | None = None) -> DockerSandboxProvider:
    provider = DockerSandboxProvider.__new__(DockerSandboxProvider)
    provider._docker_host = docker_host  # pyright: ignore[reportPrivateUsage]
    return provider


def _run_builds(monkeypatch: pytest.MonkeyPatch, build: FakeBuild) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def create_subprocess_exec(*args: Any, **kwargs: Any) -> FakeBuild:
        calls.append((args, kwargs))
        Path(args[args.index("--iidfile") + 1]).write_text(IMAGE_ID + "\n")
        return build

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)
    return calls


async def test_the_dockerfile_is_built_from_its_folder_and_the_image_id_is_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_builds(monkeypatch, FakeBuild(b"done\n", 0))

    image = await _provider()._build_image(DockerfileSource(dockerfile=DOCKERFILE), 60)  # pyright: ignore[reportPrivateUsage]

    assert image == IMAGE_ID
    ((args, _),) = calls
    assert args[:3] == ("docker", "build", "--progress=plain")
    assert args[-3:] == ("--file", DOCKERFILE, "/tasks/a/environment")


async def test_the_build_uses_the_docker_host_the_provider_was_made_with(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DOCKER_CONTEXT", "other")
    calls = _run_builds(monkeypatch, FakeBuild(b"", 0))

    await _provider("tcp://builder:2375")._build_image(DockerfileSource(dockerfile=DOCKERFILE), 60)  # pyright: ignore[reportPrivateUsage]

    ((args, kwargs),) = calls
    assert args[:3] == ("docker", "--host", "tcp://builder:2375")
    assert "DOCKER_CONTEXT" not in kwargs["env"]


async def test_a_failed_build_raises_with_the_tail_of_its_log(monkeypatch: pytest.MonkeyPatch) -> None:
    _run_builds(monkeypatch, FakeBuild(b"a" * 70_000 + b"step 2 failed\n", 1))

    with pytest.raises(SandboxBuildError, match="Failed to build") as error:
        await _provider()._build_image(DockerfileSource(dockerfile=DOCKERFILE), 60)  # pyright: ignore[reportPrivateUsage]

    assert len(error.value.build_log) == 64 * 1024
    assert error.value.build_log.endswith("step 2 failed\n")


async def test_a_build_that_outruns_its_timeout_is_killed_and_keeps_its_partial_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build = FakeBuild(b"step 1 running\n", 0, hangs=True)
    _run_builds(monkeypatch, build)

    with pytest.raises(SandboxBuildError, match="Timed out building") as error:
        await _provider()._build_image(DockerfileSource(dockerfile=DOCKERFILE), 0.05)  # pyright: ignore[reportPrivateUsage]

    assert error.value.build_log == "step 1 running\n"
    assert build.returncode == -9


async def test_a_missing_docker_cli_is_a_sandbox_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def create_subprocess_exec(*_args: Any, **_kwargs: Any) -> FakeBuild:
        raise FileNotFoundError("docker")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)

    with pytest.raises(SandboxError, match="requires the docker CLI"):
        await _provider()._build_image(DockerfileSource(dockerfile=DOCKERFILE), 60)  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("source", [ImageSource(image="python:3.12-slim"), DockerfileSource(dockerfile=DOCKERFILE)])
async def test_an_unsupported_request_is_refused_before_any_pull_or_build(source: ImageSource | DockerfileSource) -> None:
    provider = _provider()
    pull = AsyncMock()
    build = AsyncMock()
    provider._ensure_image = pull  # pyright: ignore[reportPrivateUsage]
    provider._build_image = build  # pyright: ignore[reportPrivateUsage]
    request = SandboxCreateRequest(
        source=source,
        resources=Resources(vcpu=1, memory=1, disk=1, gpu=1, gpu_type="H100"),
        name="task",
        labels={},
        env_vars={},
        auto_stop_interval=10,
        create_timeout=60,
    )

    with pytest.raises(SandboxError, match="does not support GPUs"):
        await provider.create_sandbox(request)

    pull.assert_not_awaited()
    build.assert_not_awaited()


def test_a_build_timeout_is_added_to_the_creation_timeout_of_a_dockerfile_source_only() -> None:
    def request(source: ImageSource | DockerfileSource) -> SandboxCreateRequest:
        return SandboxCreateRequest(
            source=source,
            resources=Resources(vcpu=1, memory=1, disk=1),
            name="task",
            labels={},
            env_vars={},
            auto_stop_interval=10,
            create_timeout=60,
        )

    assert request(DockerfileSource(dockerfile=DOCKERFILE, build_timeout=600)).provider_timeout == 660
    assert request(DockerfileSource(dockerfile=DOCKERFILE)).provider_timeout == 60
    assert request(ImageSource(image="python:3.12-slim")).provider_timeout == 60


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_a_build_timeout_must_be_a_positive_number(timeout: float) -> None:
    with pytest.raises(ValueError, match="build_timeout"):
        DockerfileSource(dockerfile=DOCKERFILE, build_timeout=timeout)


async def test_a_local_build_gets_its_own_timeout_and_the_container_keeps_the_creation_timeout() -> None:
    provider = _provider()
    build = AsyncMock(side_effect=SandboxError("stop"))
    provider._build_image = build  # pyright: ignore[reportPrivateUsage]
    source = DockerfileSource(dockerfile=DOCKERFILE, build_timeout=900)
    request = SandboxCreateRequest(
        source=source,
        resources=Resources(vcpu=1, memory=1, disk=1),
        name="task",
        labels={},
        env_vars={},
        auto_stop_interval=10,
        create_timeout=60,
    )

    with pytest.raises(SandboxError, match="stop"):
        await provider.create_sandbox(request)

    build.assert_awaited_once_with(source, 900)
