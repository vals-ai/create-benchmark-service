from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Mapping
from datetime import datetime
from pathlib import PurePosixPath
from string import Formatter
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, Field, model_validator


class ImageSource(BaseModel):
    type: Literal["image"] = "image"
    image: str


class SnapshotSource(BaseModel):
    type: Literal["snapshot"] = "snapshot"
    snapshot: str


class TargetedSnapshotSource(BaseModel):
    type: Literal["targeted_snapshot"] = "targeted_snapshot"
    snapshot: str
    target: str = Field(min_length=1)


BaseSandboxSource = Annotated[ImageSource | SnapshotSource, Field(discriminator="type")]


class ComposeSource(BaseModel):
    type: Literal["compose"] = "compose"
    outer: BaseSandboxSource
    service: str = "main"
    compose_command: str = "docker compose"


SandboxSource = Annotated[
    ImageSource | SnapshotSource | TargetedSnapshotSource | ComposeSource,
    Field(discriminator="type"),
]

_ENV_VAR_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_COMMAND_ENV_NAMES = frozenset({"LANG", "TERM"})
_VOLUME_SUBPATH_FORMATTER = Formatter()
_VOLUME_SUBPATH_RUN_ID_FIELD = "run_id"


def validate_command_env(env_vars: Mapping[str, str] | None) -> dict[str, str]:
    env = dict(env_vars or {})
    invalid_names = [name for name in env if _ENV_VAR_NAME.fullmatch(name) is None]
    if invalid_names:
        raise ValueError(f"Invalid environment variable names: {', '.join(invalid_names)}")
    reserved_names = sorted(env.keys() & _RESERVED_COMMAND_ENV_NAMES)
    if reserved_names:
        raise ValueError(f"Reserved command environment variable names: {', '.join(reserved_names)}")
    return env


def _validate_rendered_volume_subpath(subpath: str) -> None:
    path = PurePosixPath(subpath)
    if path.is_absolute() or str(path) == "." or ".." in path.parts:
        raise ValueError(f"subpath must be a non-empty relative path without '..', got {subpath!r}")


def _volume_subpath_fields(template: str) -> list[str]:
    fields: list[str] = []
    try:
        parsed = _VOLUME_SUBPATH_FORMATTER.parse(template)
        for _, field_name, format_spec, conversion in parsed:
            if field_name is None:
                continue
            if field_name != _VOLUME_SUBPATH_RUN_ID_FIELD or format_spec or conversion:
                raise ValueError("subpath supports only the {run_id} placeholder")
            fields.append(field_name)
    except ValueError as exc:
        raise ValueError(f"invalid volume subpath template: {exc}") from exc
    return fields


class Resources(BaseModel):
    vcpu: int = Field(description="Logical sandbox CPU count")
    memory: int = Field(description="Sandbox memory")
    disk: int = Field(description="Sandbox ephemeral disk")
    gpu: int = Field(default=0, ge=0, description="Number of GPUs to allocate")
    gpu_type: str | None = Field(default=None, description="GPU type to allocate, e.g. 'H100' (provider-specific)")

    @model_validator(mode="after")
    def _validate_gpu(self) -> Self:
        if self.gpu_type is not None and self.gpu < 1:
            raise ValueError("gpu_type requires gpu >= 1")
        return self


class VolumeMount(BaseModel):
    """A named, persistent volume attached to a sandbox at a fixed path.

    Distinct from the ephemeral disk in Resources: a volume outlives the sandbox
    and is shared by every sandbox that mounts it. Benchmarks whose fixture is
    far larger than their code need this — baking multi-gigabyte assets into an
    image makes every cold start pay for them, and rebuilding the image on any
    change is slow enough to discourage changing them at all.
    """

    name: str = Field(min_length=1, description="Provider-side volume name")
    mount_path: str = Field(min_length=1, description="Absolute path inside the sandbox")
    read_only: bool = Field(default=False, description="Mount without write access")
    create_if_missing: bool = Field(
        default=False,
        description="Create the volume when absent rather than failing; leave off for "
        "fixtures a run must not silently start without",
    )
    subpath: str | None = Field(
        default=None,
        min_length=1,
        description="Relative directory within the volume to mount; {run_id} is resolved from "
        "the sandbox run-id or run_id label. A read-only subpath must already exist.",
    )

    @model_validator(mode="after")
    def _validate_paths(self) -> Self:
        if not self.mount_path.startswith("/"):
            raise ValueError(f"mount_path must be absolute, got {self.mount_path!r}")
        if self.subpath is not None:
            _volume_subpath_fields(self.subpath)
            _validate_rendered_volume_subpath(self.subpath.format(run_id="run"))
        return self


class SandboxCreateRequest(BaseModel):
    source: SandboxSource
    resources: Resources
    name: str
    labels: dict[str, str]
    env_vars: dict[str, str]
    auto_stop_interval: int
    create_timeout: int
    network_block_all: bool = False
    sandbox_secrets: dict[str, str] = Field(
        default_factory=dict,
        description="Provider-managed secret references keyed by environment variable name",
    )
    volumes: list[VolumeMount] = Field(
        default_factory=list,
        description="Persistent volumes to attach; empty keeps existing behaviour",
    )

    @model_validator(mode="after")
    def _validate_request(self) -> Self:
        validate_command_env(self.sandbox_secrets)
        blank_secret_names = sorted(name for name, reference in self.sandbox_secrets.items() if not reference.strip())
        if blank_secret_names:
            raise ValueError(f"provider secret references cannot be blank: {', '.join(blank_secret_names)}")
        overlapping_env = sorted(self.env_vars.keys() & self.sandbox_secrets.keys())
        if overlapping_env:
            raise ValueError(
                f"environment variables cannot be both plaintext and provider-managed secrets: {', '.join(overlapping_env)}"
            )

        seen: set[str] = set()
        for volume in self.volumes:
            if volume.mount_path in seen:
                raise ValueError(f"duplicate volume mount_path: {volume.mount_path}")
            seen.add(volume.mount_path)
        return self


class SandboxQuery(BaseModel):
    labels: dict[str, str]
    page_size: int = 10
    created_at_lte: AwareDatetime | None = Field(
        default=None,
        description="Inclusive creation-time upper bound; providers must apply it or raise SandboxError",
    )


class MissingSandboxConfigError(ValueError):
    pass


class SandboxError(Exception):
    pass


def resolve_volume_subpath(mount: VolumeMount, labels: Mapping[str, str]) -> str | None:
    """Resolve a mount's run-scoped subpath without silently sharing unlabeled runs."""
    if mount.subpath is None:
        return None

    fields = _volume_subpath_fields(mount.subpath)
    run_id = labels.get("run-id") or labels.get("run_id")
    if _VOLUME_SUBPATH_RUN_ID_FIELD in fields and not run_id:
        raise SandboxError(f"Volume {mount.name!r} subpath requires a non-empty 'run-id' or 'run_id' sandbox label")

    rendered = mount.subpath.format(run_id=run_id)
    try:
        _validate_rendered_volume_subpath(rendered)
    except ValueError as exc:
        raise SandboxError(f"Invalid resolved subpath for volume {mount.name!r}: {exc}") from exc
    return rendered


class SandboxNotFoundError(SandboxError):
    pass


class SandboxConnectionError(SandboxError):
    pass


class SandboxCommandError(SandboxError):
    def __init__(self, exit_code: int) -> None:
        self.exit_code = exit_code
        super().__init__(f"Command failed with exit code {exit_code}")


class ExecResult(BaseModel):
    exit_code: int
    output: str = ""

    @property
    def stdout(self) -> str:
        return self.output


class Sandbox(ABC):
    @property
    @abstractmethod
    def id(self) -> str: ...

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def state(self) -> str: ...

    @property
    def labels(self) -> Mapping[str, str] | None:
        """Provider-reported labels, or None when unavailable."""
        return getattr(self, "_labels", None)

    @labels.setter
    def labels(self, value: Mapping[str, str] | None) -> None:
        self._labels = value

    @property
    def created_at(self) -> datetime | None:
        """Provider-reported creation time in UTC, or None when unavailable."""
        return getattr(self, "_created_at", None)

    @created_at.setter
    def created_at(self, value: datetime | None) -> None:
        self._created_at = value

    @abstractmethod
    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult: ...

    @abstractmethod
    def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]: ...

    @abstractmethod
    async def upload_file(self, remote_path: str, content: bytes) -> None: ...

    @abstractmethod
    async def download_file(self, remote_path: str) -> bytes: ...

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        """Stream a file's content in chunks without buffering the whole file in memory.

        Providers that support chunked reads should override this; the default falls back
        to a full in-memory download.
        """
        yield await self.download_file(remote_path)

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        raise SandboxError("Sandbox provider does not support modifying egress rules")

    async def clear_egress_rules(self) -> None:
        raise SandboxError("Sandbox provider does not support modifying egress rules")


class SandboxProvider(ABC):
    @abstractmethod
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox: ...

    @abstractmethod
    async def get_sandbox(self, instance_id: str) -> Sandbox: ...

    @abstractmethod
    async def delete_sandbox(self, instance_id: str) -> None: ...

    @abstractmethod
    def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]: ...

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()
