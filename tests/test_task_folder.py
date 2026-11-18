"""Tests for the shared task folder module."""

import json
import tomllib
from collections.abc import AsyncGenerator, Mapping
from typing import cast

import pytest
from pydantic import ValidationError

from benchmark_service import TaskFolder, write_task_folder
from benchmark_service.sandbox import ExecResult, Sandbox


class RecordingSandbox(Sandbox):
    """Records each exec command and each upload. No network."""

    def __init__(self, exec_result: ExecResult | None = None) -> None:
        self.commands: list[str] = []
        self.uploads: list[tuple[str, bytes]] = []
        self._exec_result = exec_result or ExecResult(exit_code=0, output="")

    @property
    def id(self) -> str:
        return "recording-sandbox"

    @property
    def name(self) -> str:
        return "recording-sandbox"

    @property
    def state(self) -> str:
        return "started"

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.commands.append(command)
        return self._exec_result

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        return
        yield  # marks this as an (empty) async generator

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.uploads.append((remote_path, content))

    async def download_file(self, remote_path: str) -> bytes:
        return dict(self.uploads)[remote_path]


# The task.toml writers as the services have them. A2J: a2j-benchmark-service dev at b5a72a3
# (2026-11-15). SNAP and Medicaid: snap-benchmark-service#47 at 595f297 and
# medicaid-benchmark-service#12 at 63cbdae (2026-11-16), which have the same writer.
def _a2j_task_toml(task_id: str, timeout_sec: float) -> str:
    return (
        'schema_version = "1.3"\n'
        "\n"
        "[metadata]\n"
        'benchmark = "a2j"\n'
        f"task_id = {json.dumps(task_id)}\n"
        "\n"
        "[agent]\n"
        "# Informational. The Tracker enforces this limit from retrieve_task's\n"
        "# agent_timeout. Nothing reads this value yet.\n"
        f"timeout_sec = {float(timeout_sec)!r}\n"
    )


def _snap_medicaid_task_toml(benchmark: str, task_id: str, timeout_sec: float | None) -> str:
    text = (
        f'schema_version = "1.3"\n\n[metadata]\nbenchmark = {json.dumps(benchmark)}\ntask_id = {json.dumps(task_id)}\n'
    )
    if timeout_sec is not None:
        text += (
            "\n"
            "[agent]\n"
            "# Informational. The Tracker enforces this limit from retrieve_task's\n"
            "# agent_timeout. Nothing reads this value yet.\n"
            f"timeout_sec = {float(timeout_sec)!r}\n"
        )
    return text


def _parsed_with_types(text: str) -> object:
    """Parsed TOML with each leaf paired with its type, so 10800 and 10800.0 differ."""

    def walk(value: object) -> object:
        if isinstance(value, dict):
            table = cast(dict[str, object], value)
            return {key: walk(item) for key, item in table.items()}
        return (type(value).__name__, value)

    return walk(tomllib.loads(text))


CONDUCTOR_OPTIONS = {"name": "snap", "target_max_turns": 50, "wind_down": "two_stage", "page_tool": "store"}


def test_a2j_task_toml_matches_service() -> None:
    folder = TaskFolder(
        instruction="What are my rights?",
        metadata={"benchmark": "a2j", "task_id": "a2j-0042"},
        agent_timeout_sec=10800,
    )
    assert _parsed_with_types(folder.task_toml()) == _parsed_with_types(_a2j_task_toml("a2j-0042", 10800.0))


@pytest.mark.parametrize("benchmark", ["snap", "medicaid"])
def test_snap_and_medicaid_task_toml_match_service(benchmark: str) -> None:
    # Neither service gives an agent time limit, so task.toml has no [agent] table.
    folder = TaskFolder(instruction="q", metadata={"benchmark": benchmark, "task_id": f"{benchmark}-7"})
    text = folder.task_toml()
    assert _parsed_with_types(text) == _parsed_with_types(_snap_medicaid_task_toml(benchmark, f"{benchmark}-7", None))
    assert "agent" not in tomllib.loads(text)


@pytest.mark.parametrize(
    "task_id",
    [
        'quote " and backslash \\ and é and\na line break',
        'odd"id\\x',
        "tab\tand bell\x07and delete\x7f",
        "astral \U0001f600",
        "",
    ],
)
def test_task_toml_escapes_strings(task_id: str) -> None:
    folder = TaskFolder(instruction="q", metadata={"task_id": task_id, "key with space": task_id})
    metadata = tomllib.loads(folder.task_toml())["metadata"]
    assert metadata == {"task_id": task_id, "key with space": task_id}


def test_task_toml_text_for_plain_values() -> None:
    folder = TaskFolder(
        instruction="q",
        metadata={"benchmark": "a2j", "task_id": "both__abc", "n": 3, "ok": True, "f": 1.5},
        agent_timeout_sec=10800,
    )
    assert folder.task_toml() == (
        'schema_version = "1.3"\n\n[metadata]\nbenchmark = "a2j"\ntask_id = "both__abc"\n'
        "n = 3\nok = true\nf = 1.5\n\n[agent]\ntimeout_sec = 10800.0\n"
    )


@pytest.mark.parametrize("key", ["", "a.b", "é", "1", '"', "bare-key_1"])
def test_task_toml_keeps_each_key_as_one_key(key: str) -> None:
    folder = TaskFolder(instruction="q", metadata={key: "v"})
    assert tomllib.loads(folder.task_toml())["metadata"] == {key: "v"}


def test_task_toml_keeps_number_and_bool_types() -> None:
    metadata: dict[str, str | int | float | bool] = {
        "yes": True,
        "no": False,
        "count": -3,
        "small": 1e-7,
        "big": 1e16,
        "inf": float("inf"),
    }
    parsed = tomllib.loads(TaskFolder(instruction="q", metadata=metadata).task_toml())["metadata"]
    assert parsed == metadata
    assert {key: type(value) for key, value in parsed.items()} == {key: type(value) for key, value in metadata.items()}


def test_task_toml_refuses_a_value_that_does_not_read_back() -> None:
    with pytest.raises(ValueError, match="does not round-trip"):
        TaskFolder(instruction="q", metadata={"score": float("nan")}).task_toml()


async def test_write_task_folder_creates_directories_and_uploads_each_file() -> None:
    sandbox = RecordingSandbox()
    conductor_json = json.dumps({"condition": "search", **CONDUCTOR_OPTIONS}, indent=2) + "\n"
    folder = TaskFolder(
        instruction="Help me apply.",
        metadata={"benchmark": "snap", "task_id": "snap-7"},
        extra_files={
            "agent/conductor/conductor.json": conductor_json,
            "agent/conductor/context.md": b"",
        },
    )

    await write_task_folder(sandbox, folder)

    assert sandbox.commands == ["mkdir -p /task /task/agent/conductor"]
    assert [path for path, _ in sandbox.uploads] == [
        "/task/instruction.md",
        "/task/task.toml",
        "/task/agent/conductor/conductor.json",
        "/task/agent/conductor/context.md",
    ]
    uploads = dict(sandbox.uploads)
    assert uploads["/task/instruction.md"] == b"Help me apply."
    assert uploads["/task/agent/conductor/conductor.json"] == conductor_json.encode()
    assert uploads["/task/agent/conductor/context.md"] == b""


async def test_instruction_is_written_exactly() -> None:
    sandbox = RecordingSandbox()
    instruction = "  Línea uno\r\nline two\n\n¿tres?  \r"
    await write_task_folder(sandbox, TaskFolder(instruction=instruction))
    assert dict(sandbox.uploads)["/task/instruction.md"] == instruction.encode("utf-8")


async def test_task_dir_can_change_and_is_quoted() -> None:
    sandbox = RecordingSandbox()
    folder = TaskFolder(instruction="q", extra_files={"a b/c.txt": b"x"})
    await write_task_folder(sandbox, folder, task_dir="/work/my task")
    assert sandbox.commands == ["mkdir -p '/work/my task' '/work/my task/a b'"]
    assert [path for path, _ in sandbox.uploads][-1] == "/work/my task/a b/c.txt"


async def test_relative_task_dir_is_refused() -> None:
    with pytest.raises(ValueError, match="task_dir"):
        await write_task_folder(RecordingSandbox(), TaskFolder(instruction="q"), task_dir="task")


async def test_mkdir_failure_raises_and_uploads_nothing() -> None:
    sandbox = RecordingSandbox(ExecResult(exit_code=1, output="read-only file system"))
    with pytest.raises(RuntimeError, match=r"Could not create /task in the sandbox: read-only file system"):
        await write_task_folder(sandbox, TaskFolder(instruction="q"))
    assert sandbox.uploads == []


@pytest.mark.parametrize(
    "path",
    [
        "/etc/passwd",
        "..",
        "../x",
        "agent/../../x",
        "instruction.md",
        "task.toml",
        "./task.toml",
        "agent//x",
        "agent\\x",
        "",
        ".",
        "instruction.md/child.txt",
        "task.toml/child.txt",
    ],
)
def test_bad_extra_file_path_is_refused(path: str) -> None:
    with pytest.raises(ValidationError) as caught:
        TaskFolder(instruction="q", extra_files={path: b""})
    assert repr(path) in str(caught.value)


def test_path_below_another_extra_file_is_refused() -> None:
    with pytest.raises(ValidationError, match=r"'a/b/c\.txt' is below 'a/b', which is a file"):
        TaskFolder(instruction="q", extra_files={"a/b/c.txt": b"", "a/b": b""})


@pytest.mark.parametrize("path", ["task.toml", "../x", "a.txt/child.txt"])
async def test_path_added_after_construction_is_refused(path: str) -> None:
    folder = TaskFolder(instruction="q", extra_files={"a.txt": b""})
    folder.extra_files[path] = b""
    sandbox = RecordingSandbox()
    with pytest.raises(ValueError, match="Extra file path"):
        await write_task_folder(sandbox, folder)
    assert sandbox.commands == []
    assert sandbox.uploads == []


def test_timeout_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        TaskFolder(instruction="q", agent_timeout_sec=0)


def test_harbor_loads_task_toml() -> None:
    config_module = pytest.importorskip("harbor.models.task.config")
    folder = TaskFolder(
        instruction="q",
        metadata={"benchmark": "a2j", "task_id": "a2j-0042"},
        agent_timeout_sec=10800,
    )
    config = config_module.TaskConfig.model_validate_toml(folder.task_toml())
    assert config.schema_version == "1.3"
    assert config.metadata == {"benchmark": "a2j", "task_id": "a2j-0042"}
    assert config.agent.timeout_sec == 10800.0

    no_timeout = TaskFolder(instruction="q", metadata={"benchmark": "medicaid", "task_id": "m-1"})
    assert config_module.TaskConfig.model_validate_toml(no_timeout.task_toml()).agent.timeout_sec is None
