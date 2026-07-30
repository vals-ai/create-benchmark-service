from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime
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


def validate_command_env(env_vars: Mapping[str, str] | None) -> dict[str, str]:
    env = dict(env_vars or {})
    invalid_names = [name for name in env if _ENV_VAR_NAME.fullmatch(name) is None]
    if invalid_names:
        raise ValueError(f"Invalid environment variable names: {', '.join(invalid_names)}")
    reserved_names = sorted(env.keys() & _RESERVED_COMMAND_ENV_NAMES)
    if reserved_names:
        raise ValueError(f"Reserved command environment variable names: {', '.join(reserved_names)}")
    return env


# Valkyrie labels a run "Id"; this service's own grading path labels it "run-id".
BENCHMARK_ID_LABELS = ("Id", "run-id")
TASK_ID_LABELS = ("Task",)
SUB_PATH_PLACEHOLDERS = ("benchmark_id", "task_id", "sandbox_name")


def _first_label(labels: Mapping[str, str], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = labels.get(key)
        if value:
            return value
    return None


def _template_field_names(template: str) -> list[str]:
    return [field for _, field, _, _ in Formatter().parse(template) if field is not None]


class SandboxVolume(BaseModel):
    name: str = Field(min_length=1, description="Provider volume name")
    mount_path: str = Field(min_length=1, description="Absolute path where the volume is mounted")
    read_only: bool = False
    create_if_missing: bool = False
    sub_path_template: str | None = Field(
        default=None,
        min_length=1,
        description=("Optional provider-volume subdirectory. Supports {benchmark_id}, {task_id}, and {sandbox_name}."),
    )

    @model_validator(mode="after")
    def _validate_volume(self) -> Self:
        if not self.mount_path.startswith("/"):
            raise ValueError(f"mount_path must be absolute: {self.mount_path}")
        if self.sub_path_template is None:
            return self
        try:
            fields = _template_field_names(self.sub_path_template)
        except ValueError as exc:
            raise ValueError(f"Malformed sub_path_template {self.sub_path_template!r}: {exc}") from exc
        unknown = sorted(set(fields) - set(SUB_PATH_PLACEHOLDERS))
        if unknown:
            raise ValueError(
                f"Unknown sub_path_template placeholders: {', '.join(unknown)}; "
                f"supported: {', '.join(SUB_PATH_PLACEHOLDERS)}"
            )
        return self

    def resolve_sub_path(self, labels: Mapping[str, str], sandbox_name: str) -> str | None:
        """Return this volume's per-sandbox subdirectory, or None when unscoped.

        Arguments
        - labels: Sandbox labels carrying the run and task identity.
        - sandbox_name: Provider-facing sandbox name.

        Returns
        The interpolated subdirectory, or None when no template is set.

        Raises
        SandboxError when the template references identity the labels do not
        carry. Interpolating a missing value would collapse distinct runs onto
        one shared subdirectory, which is the opposite of what a template is for.
        """
        if self.sub_path_template is None:
            return None
        values = {
            "benchmark_id": _first_label(labels, BENCHMARK_ID_LABELS),
            "task_id": _first_label(labels, TASK_ID_LABELS),
            "sandbox_name": sandbox_name or None,
        }
        missing = sorted({name for name in _template_field_names(self.sub_path_template) if not values[name]})
        if missing:
            raise SandboxError(
                f"Volume {self.name!r} sub_path_template requires sandbox identity for: {', '.join(missing)}"
            )
        return self.sub_path_template.format(**values)


class Resources(BaseModel):
    vcpu: int = Field(description="Logical sandbox CPU count")
    memory: int = Field(description="Sandbox memory")
    disk: int = Field(description="Sandbox ephemeral disk")
    gpu: int = Field(default=0, ge=0, description="Number of GPUs to allocate")
    gpu_type: str | None = Field(default=None, description="GPU type to allocate, e.g. 'H100' (provider-specific)")
    volumes: list[SandboxVolume] = Field(
        default_factory=list,
        description="Durable volumes to mount into the sandbox",
    )

    @model_validator(mode="after")
    def _validate_resources(self) -> Self:
        if self.gpu_type is not None and self.gpu < 1:
            raise ValueError("gpu_type requires gpu >= 1")
        # Modal keys its mounts by path, so a repeated mount_path would silently drop
        # a volume the benchmark asked for. Rejected here so both adapters agree.
        mount_paths = [volume.mount_path for volume in self.volumes]
        duplicates = sorted({path for path in mount_paths if mount_paths.count(path) > 1})
        if duplicates:
            raise ValueError(f"Duplicate volume mount_path: {', '.join(duplicates)}")
        return self


class SandboxCreateRequest(BaseModel):
    source: SandboxSource
    resources: Resources
    name: str = Field(min_length=1)
    labels: dict[str, str]
    env_vars: dict[str, str]
    auto_stop_interval: int
    create_timeout: int
    network_block_all: bool = False


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
