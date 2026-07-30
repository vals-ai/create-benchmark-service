from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Mapping
from datetime import datetime
from string import Formatter
from typing import Annotated, ClassVar, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


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
_VOLUME_SUBPATH_SEGMENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_INVALID_VOLUME_LABEL_CHARS = re.compile(r"[^A-Za-z0-9_.-]")
_RESERVED_COMMAND_ENV_NAMES = frozenset({"LANG", "TERM"})
_VOLUME_TEMPLATE_FIELDS = frozenset({"benchmark_id", "task_id", "sandbox_name"})
_PROHIBITED_VOLUME_MOUNT_ROOTS = frozenset(
    {
        "/bin",
        "/boot",
        "/dev",
        "/etc",
        "/lib",
        "/lib64",
        "/proc",
        "/sbin",
        "/sys",
    }
)


def validate_command_env(env_vars: Mapping[str, str] | None) -> dict[str, str]:
    env = dict(env_vars or {})
    invalid_names = [name for name in env if _ENV_VAR_NAME.fullmatch(name) is None]
    if invalid_names:
        raise ValueError(f"Invalid environment variable names: {', '.join(invalid_names)}")
    reserved_names = sorted(env.keys() & _RESERVED_COMMAND_ENV_NAMES)
    if reserved_names:
        raise ValueError(f"Reserved command environment variable names: {', '.join(reserved_names)}")
    return env


def _volume_label_segment(value: str) -> str:
    sanitized = _INVALID_VOLUME_LABEL_CHARS.sub("_", value)
    if sanitized == value:
        return value
    digest = hashlib.sha256(value.encode()).hexdigest()[:10]
    return f"{sanitized}-{digest}"


def render_volume_sub_path(
    template: str | None,
    labels: Mapping[str, str],
    sandbox_name: str,
) -> str | None:
    if template is None:
        return None

    values = {
        "benchmark_id": labels.get("Id") or labels.get("run-id"),
        "task_id": labels.get("Task"),
        "sandbox_name": sandbox_name,
    }
    fields = {field_name for _, field_name, _, _ in Formatter().parse(template) if field_name is not None}
    missing = sorted(field for field in fields if not values[field])
    if missing:
        raise SandboxError(f"Missing labels for volume sub_path_template fields: {', '.join(missing)}")

    sanitized_values = {field: _volume_label_segment(value) for field, value in values.items() if value is not None}
    rendered = template.format(**sanitized_values)
    parts = rendered.split("/")
    if any(part in {"", ".", ".."} or _VOLUME_SUBPATH_SEGMENT.fullmatch(part) is None for part in parts):
        raise SandboxError("volume sub_path_template rendered an invalid path")
    return "/".join(parts)


class SandboxVolume(BaseModel):
    name: str = Field(min_length=1, description="Provider volume name")
    mount_path: str = Field(description="Absolute path where the volume is mounted")
    read_only: bool = False
    create_if_missing: bool = False
    sub_path_template: str | None = Field(
        default=None,
        description=("Optional provider-volume subdirectory. Supports {benchmark_id}, {task_id}, and {sandbox_name}."),
    )

    @model_validator(mode="after")
    def _validate_mount(self) -> Self:
        path_parts = self.mount_path.split("/")
        if (
            not self.mount_path.startswith("/")
            or self.mount_path == "/"
            or "" in path_parts[1:]
            or "." in path_parts
            or ".." in path_parts
        ):
            raise ValueError("volume mount_path must be an absolute normalized non-root path")
        if any(
            self.mount_path == root or self.mount_path.startswith(f"{root}/") for root in _PROHIBITED_VOLUME_MOUNT_ROOTS
        ):
            raise ValueError("volume mount_path must not target a system directory")

        if self.sub_path_template is None:
            return self
        if self.sub_path_template.startswith("/"):
            raise ValueError("volume sub_path_template must be relative")
        try:
            parsed_template = list(Formatter().parse(self.sub_path_template))
        except ValueError as exc:
            raise ValueError("volume sub_path_template has invalid formatting") from exc
        if any(format_spec or conversion for _, _, format_spec, conversion in parsed_template):
            raise ValueError("volume sub_path_template does not support conversions or format specifications")
        fields = {field_name for _, field_name, _, _ in parsed_template if field_name is not None}
        unsupported = sorted(fields - _VOLUME_TEMPLATE_FIELDS)
        if unsupported:
            raise ValueError(f"Unsupported volume sub_path_template fields: {', '.join(unsupported)}")
        if any(part in {"", ".", ".."} for part in self.sub_path_template.split("/")):
            raise ValueError("volume sub_path_template must be a normalized relative path")
        return self


class SandboxProviderCapabilities(BaseModel):
    model_config = ConfigDict(frozen=True)

    durable_volumes: bool = False
    volume_creation: bool = False
    volume_subpaths: bool = False
    read_only_volume_mounts: bool = False


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
        mount_paths = [volume.mount_path for volume in self.volumes]
        if len(mount_paths) != len(set(mount_paths)):
            raise ValueError("volume mount_path values must be unique")
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


class UnsupportedSandboxCapabilityError(SandboxError):
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
    capabilities: ClassVar[SandboxProviderCapabilities] = SandboxProviderCapabilities()

    def validate_create_request(self, request: SandboxCreateRequest) -> None:
        volumes = request.resources.volumes
        if volumes and not self.capabilities.durable_volumes:
            raise UnsupportedSandboxCapabilityError(f"{type(self).__name__} does not support durable volume mounts")
        if any(volume.create_if_missing for volume in volumes) and not self.capabilities.volume_creation:
            raise UnsupportedSandboxCapabilityError(f"{type(self).__name__} does not support creating durable volumes")
        if any(volume.sub_path_template is not None for volume in volumes) and not self.capabilities.volume_subpaths:
            raise UnsupportedSandboxCapabilityError(f"{type(self).__name__} does not support durable volume subpaths")
        if any(volume.read_only for volume in volumes) and not self.capabilities.read_only_volume_mounts:
            raise UnsupportedSandboxCapabilityError(
                f"{type(self).__name__} does not support read-only durable volume mounts"
            )

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
