"""Dockerfile sandbox source checks; run pytest tests/test_dockerfile_source.py."""

import asyncio
from typing import Any

import pytest

from benchmark_service import (
    DockerfileSource,
    Resources,
    SandboxBuildError,
    SandboxCreateRequest,
    SandboxError,
)
from benchmark_service.sandbox.local.docker import DockerSandboxProvider
from benchmark_service.schemas import RetrieveTaskResponse

DOCKERFILE = "/tasks/a/environment/Dockerfile"


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
        return build

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)
    return calls


async def test_the_dockerfile_is_built_from_its_folder_under_a_tag_that_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_builds(monkeypatch, FakeBuild(b"done\n", 0))

    tag = await _provider()._build_image(DockerfileSource(dockerfile=DOCKERFILE), 60)  # pyright: ignore[reportPrivateUsage]

    assert tag.startswith("cbs-build:")
    ((args, _),) = calls
    assert args == ("docker", "build", "--progress=plain", "--tag", tag, "--file", DOCKERFILE, "/tasks/a/environment")
    _run_builds(monkeypatch, FakeBuild(b"", 0))
    assert await _provider()._build_image(DockerfileSource(dockerfile=DOCKERFILE), 60) == tag  # pyright: ignore[reportPrivateUsage]


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
