"""Write a Harbor dataset: task folders that grade themselves, for Valkyrie and plain Harbor.

An exporter maps each native task record to a `HarborTask` and passes them to `write_dataset`. The
module owns the folder layout and the `task.toml` writer. The exporter owns the instruction, the
metadata, the agent environment, the pinned grader image, and the files the grader reads.
"""

import json
import re
import tomllib
from collections.abc import Iterable, Mapping
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from benchmark_service.task_folder import (
    INSTRUCTION_FILE,
    TASK_FILE,
    TASK_SCHEMA_VERSION,
    check_file_paths,
    toml_string,
)

type TomlScalar = str | int | float | bool
type TomlValue = TomlScalar | list[TomlScalar] | dict[str, TomlValue]

METRIC_FILE = "metric.py"
HARBOR_JOB_FILE = "harbor.yaml"
VERIFIER_RESULT_FILE = "details.json"
TESTS_MOUNT = "/tests"

_TASK_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_TASK_NAME = re.compile(r"[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*")
_PINNED_IMAGE = re.compile(r"\S+@sha256:[0-9a-f]{64}")
_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")


def _require_pinned(image: str) -> str:
    if not _PINNED_IMAGE.fullmatch(image):
        raise ValueError(f"Pin the image by digest, as name@sha256:<64 hex>: {image!r}")
    return image


class Verifier(BaseModel):
    """How a task grades itself, in its own sandbox built from a pinned grader image.

    `tests/Dockerfile` is `FROM image` with `files` copied to `/tests`, so the agent never sees
    them. `tests/test.sh` execs `command`, which writes `/logs/verifier/reward.json` and the judge
    details to `/logs/verifier/details.json`. `env` values are `task.toml` references such as
    `"${MODEL_GATEWAY_URL:-}"`, which Valkyrie and Harbor both resolve at verification time.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    image: str
    command: str
    files: dict[str, bytes | str] = Field(default_factory=dict[str, bytes | str])
    env: dict[str, str] = Field(default_factory=dict[str, str])
    timeout_sec: float = Field(default=1800.0, gt=0)
    cpus: float = Field(default=1.0, gt=0)
    memory_mb: int = Field(default=1024, gt=0)
    storage_mb: int = Field(default=1024, gt=0)

    @model_validator(mode="after")
    def _check(self) -> "Verifier":
        _require_pinned(self.image)
        if not self.command.strip() or "\n" in self.command:
            raise ValueError("The verifier command must be one non-empty line")
        return self


class HarborTask(BaseModel):
    """One task folder of a Harbor dataset.

    `name` is Harbor's registry name, `<org>/<task>` in lowercase, and must stay the same across
    exports. The agent sandbox comes from exactly one of `environment_image` (pinned by digest) or
    `environment_dockerfile`, whose build context is `environment/` with `environment_files`.
    `solution`, when set, becomes `solution/solve.sh`. `files` holds any other file of the folder
    by relative path, outside `environment/`, `solution/` and `tests/`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    task_id: str
    name: str
    instruction: str
    verifier: Verifier
    environment_image: str | None = None
    environment_dockerfile: str | None = None
    environment_files: dict[str, bytes | str] = Field(default_factory=dict[str, bytes | str])
    metadata: dict[str, TomlValue] = Field(default_factory=dict[str, TomlValue])
    agent_timeout_sec: float | None = Field(default=None, gt=0)
    artifacts: list[str] = Field(default_factory=list[str])
    solution: str | None = None
    files: dict[str, bytes | str] = Field(default_factory=dict[str, bytes | str])

    @model_validator(mode="after")
    def _check(self) -> "HarborTask":
        if not _TASK_ID.fullmatch(self.task_id):
            raise ValueError(f"Not a valid task ID: {self.task_id!r}")
        if not _TASK_NAME.fullmatch(self.name):
            raise ValueError(f"The task name must be lowercase <org>/<task>: {self.name!r}")
        if (self.environment_image is None) == (self.environment_dockerfile is None):
            raise ValueError("Set exactly one of environment_image and environment_dockerfile")
        if self.environment_image is not None:
            _require_pinned(self.environment_image)
            if self.environment_files:
                raise ValueError("environment_files need environment_dockerfile")
        if "valkyrie" in self.metadata:
            raise ValueError("metadata.valkyrie is written by HarborTask")
        for path in self.files:
            if path.split("/")[0] in ("environment", "solution", "tests"):
                raise ValueError(f"Put {path!r} in environment_files, solution or verifier.files instead")
        self.files_by_path()
        return self

    def task_toml(self) -> str:
        """The `task.toml` text. It reads back as exactly the data it was written from."""
        verifier = self.verifier
        environment: dict[str, TomlValue] = {}
        if self.environment_image is not None:
            environment["docker_image"] = self.environment_image
        data: dict[str, TomlValue] = {
            "schema_version": TASK_SCHEMA_VERSION,
            "task": {"name": self.name},
            **({"artifacts": list(self.artifacts)} if self.artifacts else {}),
            "metadata": {**self.metadata, "valkyrie": {"verifier_result_file": VERIFIER_RESULT_FILE}},
            **({"agent": {"timeout_sec": self.agent_timeout_sec}} if self.agent_timeout_sec is not None else {}),
            **({"environment": environment} if environment else {}),
            "verifier": {
                "environment_mode": "separate",
                "timeout_sec": verifier.timeout_sec,
                "environment": {
                    "cpus": verifier.cpus,
                    "memory_mb": verifier.memory_mb,
                    "storage_mb": verifier.storage_mb,
                },
                **({"env": dict(verifier.env)} if verifier.env else {}),
            },
        }
        text = _toml_document(data)
        try:
            parsed = tomllib.loads(text)
        except tomllib.TOMLDecodeError as error:
            raise ValueError(f"task.toml for {self.task_id!r} is not valid TOML: {error}") from error
        if parsed != data:
            raise ValueError(f"task.toml for {self.task_id!r} does not round-trip")
        return text

    def files_by_path(self) -> dict[str, bytes]:
        """Every file of the folder, keyed by path relative to the folder."""
        tests = {f"tests/{path}": content for path, content in self.verifier.files.items()}
        environment = {f"environment/{path}": content for path, content in self.environment_files.items()}
        generated: dict[str, bytes | str] = {
            "tests/Dockerfile": f"FROM {self.verifier.image}\nCOPY . {TESTS_MOUNT}/\n",
            "tests/test.sh": f"#!/bin/sh\nset -eu\nexec {self.verifier.command}\n",
        }
        if self.environment_dockerfile is not None:
            generated["environment/Dockerfile"] = self.environment_dockerfile
        if self.solution is not None:
            generated["solution/solve.sh"] = self.solution
        parts = (generated, tests, environment, self.files)
        paths = [path for part in parts for path in part]
        duplicates = sorted({path for path in paths if paths.count(path) > 1})
        if duplicates:
            raise ValueError(f"Task {self.task_id!r} writes these paths twice: {duplicates}")
        check_file_paths(paths)
        files = {INSTRUCTION_FILE: self.instruction.encode(), TASK_FILE: self.task_toml().encode()}
        for part in parts:
            files.update({path: c.encode() if isinstance(c, str) else c for path, c in part.items()})
        return files


def write_dataset(root: Path, tasks: Iterable[HarborTask], *, metric: str) -> list[str]:
    """Write each task folder under `root`, plus `metric.py` and a `harbor.yaml` that selects it.

    `root` must not exist yet. Returns the task IDs in the order written. Push the folder with
    `valkyrie dataset push`, or run it with `harbor run --config harbor.yaml`.
    """
    tasks = list(tasks)
    task_ids = [task.task_id for task in tasks]
    if not tasks:
        raise ValueError("A dataset needs at least one task")
    folded = [task_id.casefold() for task_id in task_ids]
    if len(set(folded)) != len(folded):
        raise ValueError(f"Duplicate task IDs: {sorted({t for t in task_ids if folded.count(t.casefold()) > 1})}")
    root.mkdir(parents=True, exist_ok=False)
    for task in tasks:
        for path, content in task.files_by_path().items():
            target = root / task.task_id / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            if path in ("tests/test.sh", "solution/solve.sh"):
                target.chmod(0o755)
    (root / METRIC_FILE).write_text(metric)
    (root / HARBOR_JOB_FILE).write_text(
        "datasets:\n  - path: .\nmetrics:\n  - type: uv-script\n    kwargs:\n      script_path: metric.py\n"
    )
    return task_ids


def _toml_key(key: str) -> str:
    return key if _BARE_KEY.fullmatch(key) else toml_string(key)


def _toml_value(value: TomlScalar | list[TomlScalar]) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return toml_string(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    return json.dumps(value) if isinstance(value, int) else repr(float(value))


def _toml_document(data: Mapping[str, TomlValue]) -> str:
    lines: list[str] = []

    def table(name: str, values: Mapping[str, TomlValue]) -> None:
        scalars = {key: value for key, value in values.items() if not isinstance(value, dict)}
        if name and (scalars or not values):
            lines.append(f"\n[{name}]")
        lines.extend(f"{_toml_key(key)} = {_toml_value(value)}" for key, value in scalars.items())
        for key, value in values.items():
            if isinstance(value, dict):
                table(f"{name}.{_toml_key(key)}" if name else _toml_key(key), value)

    table("", data)
    return "\n".join(lines).lstrip("\n") + "\n"
