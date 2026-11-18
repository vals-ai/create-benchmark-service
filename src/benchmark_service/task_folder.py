"""Write a Harbor-style task folder into a sandbox for the agent.

A service describes the folder with a `TaskFolder` and calls `write_task_folder` from its own
`setup_task`. The framework owns the folder path, the layout (`instruction.md` and `task.toml` at
the root), the `task.toml` writer, the directory creation, and the uploads. The service owns the
instruction text, the metadata, and any extra files, which go in by path relative to the folder.
"""

import json
import re
import shlex
import tomllib
from collections.abc import Iterable, Mapping
from pathlib import PurePosixPath
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from benchmark_service.sandbox import Sandbox

DEFAULT_TASK_DIR = "/task"
INSTRUCTION_FILE = "instruction.md"
TASK_FILE = "task.toml"
TASK_SCHEMA_VERSION = "1.3"

MetadataValue = str | int | float | bool

_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")


def _toml_string(value: str) -> str:
    # A JSON string is a TOML basic string, except for DEL, which TOML does not allow unescaped.
    return json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")


def _toml_value(value: MetadataValue) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return _toml_string(value)
    return repr(value)


def _toml_table(name: str, table: Mapping[str, MetadataValue]) -> str:
    lines = [f"[{name}]"]
    for key, value in table.items():
        lines.append(f"{key if _BARE_KEY.fullmatch(key) else _toml_string(key)} = {_toml_value(value)}")
    return "\n".join(lines) + "\n"


class TaskFolder(BaseModel):
    """The contents of one task folder.

    `instruction` is written to `instruction.md` exactly as given, as UTF-8. `metadata` becomes
    the `[metadata]` table of `task.toml`. `agent_timeout_sec`, when set, becomes
    `[agent].timeout_sec`; with `None`, `task.toml` has no `[agent]` table. `extra_files` maps a
    path relative to the folder to the file content. A `str` content is encoded as UTF-8.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    instruction: str
    metadata: dict[str, MetadataValue] = Field(default_factory=dict[str, MetadataValue])
    agent_timeout_sec: float | None = Field(default=None, gt=0)
    extra_files: dict[str, bytes | str] = Field(default_factory=dict[str, bytes | str])

    @field_validator("extra_files")
    @classmethod
    def _check_extra_paths(cls, files: dict[str, bytes | str]) -> dict[str, bytes | str]:
        _check_file_paths(files)
        return files

    def task_toml(self) -> str:
        """The `task.toml` text: Harbor's task file with `schema_version`, `[metadata]`, `[agent]`."""
        data: dict[str, Any] = {"schema_version": TASK_SCHEMA_VERSION, "metadata": dict(self.metadata)}
        if self.agent_timeout_sec is not None:
            data["agent"] = {"timeout_sec": self.agent_timeout_sec}
        text = f"schema_version = {_toml_string(TASK_SCHEMA_VERSION)}\n\n" + _toml_table("metadata", self.metadata)
        if self.agent_timeout_sec is not None:
            text += "\n" + _toml_table("agent", {"timeout_sec": self.agent_timeout_sec})
        # Refuse the text if it does not read back as the same data, for example for a NaN value.
        try:
            parsed = tomllib.loads(text)
        except tomllib.TOMLDecodeError as error:
            raise ValueError(f"task.toml is not valid TOML for metadata {self.metadata!r}: {error}") from error
        if parsed != data:
            raise ValueError(f"task.toml does not round-trip for metadata {self.metadata!r}")
        return text

    def files(self) -> dict[str, bytes]:
        """Every file of the folder, keyed by path relative to the folder, in upload order."""
        # `frozen` does not stop a change to the mapping after construction, so check the paths again.
        _check_file_paths(self.extra_files)
        files = {
            INSTRUCTION_FILE: self.instruction.encode("utf-8"),
            TASK_FILE: self.task_toml().encode("utf-8"),
        }
        for path, content in self.extra_files.items():
            files[path] = content.encode("utf-8") if isinstance(content, str) else content
        return files


def _check_relative_path(path: str) -> None:
    pure = PurePosixPath(path)
    if not path or pure.is_absolute():
        raise ValueError(f"Extra file path must be relative to the task folder: {path!r}")
    if ".." in pure.parts:
        raise ValueError(f"Extra file path must not contain '..': {path!r}")
    if pure.as_posix() != path or "\\" in path or path == ".":
        raise ValueError(f"Extra file path must be a plain POSIX path such as 'a/b.json': {path!r}")
    if path in (INSTRUCTION_FILE, TASK_FILE):
        raise ValueError(f"Extra file path is reserved by the task folder: {path!r}")


def _check_file_paths(paths: Iterable[str]) -> None:
    """Refuse a bad path, and a path below another file of the folder, which cannot be written."""
    files = {INSTRUCTION_FILE, TASK_FILE}
    for path in paths:
        _check_relative_path(path)
        files.add(path)
    for path in sorted(files):
        for parent in PurePosixPath(path).parents:
            if str(parent) in files:
                raise ValueError(f"Extra file path {path!r} is below {str(parent)!r}, which is a file")


async def write_task_folder(sandbox: Sandbox, folder: TaskFolder, *, task_dir: str = DEFAULT_TASK_DIR) -> None:
    """Create `task_dir` and its subdirectories in the sandbox, then upload each file of `folder`.

    Raises `RuntimeError` when the sandbox cannot create the directories.
    """
    root = PurePosixPath(task_dir)
    if not root.is_absolute():
        raise ValueError(f"task_dir must be an absolute path: {task_dir!r}")
    files = folder.files()
    # Not every sandbox provider creates parent directories on upload.
    directories = sorted({str(root)} | {str((root / path).parent) for path in files})
    made = await sandbox.exec("mkdir -p " + " ".join(shlex.quote(d) for d in directories))
    if made.exit_code != 0:
        raise RuntimeError(f"Could not create {task_dir} in the sandbox: {made.output}")
    for path, content in files.items():
        await sandbox.upload_file(str(root / path), content)
