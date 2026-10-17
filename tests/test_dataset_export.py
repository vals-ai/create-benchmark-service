"""Tests for local dataset export."""

from pathlib import Path
from stat import S_IMODE
from typing import Any, ClassVar
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from benchmark_service import write_harbor_split
from cli.cli import service
from tests.conftest import StubBenchmark


class ExportBenchmark(StubBenchmark):
    tasks: ClassVar[dict[str, Path]] = {}
    loaded: ClassVar[bool] = False
    exported_dataset: ClassVar[str] = ""

    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        type(self).loaded = True
        return {"Mixed Case/name": dict(self.tasks)}

    async def export_dataset(self, dataset: str, out_dir: Path) -> bool:
        assert self.loaded
        assert dataset in self.datasets
        type(self).exported_dataset = dataset
        write_harbor_split(self.tasks, "test", out_dir)
        return True


def harbor_task(path: Path) -> Path:
    path.mkdir()
    _ = (path / "task.toml").write_text('[metadata]\nname = "A task"\n', encoding="utf-8")
    _ = (path / "instruction.md").write_text("Complete this task.\n", encoding="utf-8")
    return path


async def test_default_export_returns_false(service: StubBenchmark, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    assert await service.export_dataset("alt", out_dir) is False
    assert not out_dir.exists()


def test_cli_skips_service_without_export(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    result = CliRunner().invoke(
        service,
        ["export-dataset", "--service", "tests.conftest:StubBenchmark", "--dataset", "alt", "--out", str(out_dir)],
    )
    assert result.exit_code == 3, result.output
    assert list(out_dir.iterdir()) == []


def test_cli_exports_layout_modes_and_dataset_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first = harbor_task(tmp_path / "first")
    first.chmod(0o750)
    (first / "instruction.md").chmod(0o640)
    assets = first / "assets"
    assets.mkdir(mode=0o750)
    script = assets / "run.sh"
    _ = script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    script.chmod(0o751)
    second = harbor_task(tmp_path / "second")
    monkeypatch.setattr(ExportBenchmark, "tasks", {"Task_1": first, "task-2.0": second})
    monkeypatch.setattr(ExportBenchmark, "loaded", False)
    monkeypatch.setattr(ExportBenchmark, "exported_dataset", "")
    create = AsyncMock(wraps=ExportBenchmark.create)
    monkeypatch.setattr(ExportBenchmark, "create", create)
    out_dir = tmp_path / "out"

    result = CliRunner().invoke(
        service,
        [
            "export-dataset",
            "--service",
            f"{__name__}:ExportBenchmark",
            "--dataset",
            "Mixed Case/name",
            "--out",
            str(out_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    create.assert_awaited_once_with()
    assert ExportBenchmark.loaded
    assert ExportBenchmark.exported_dataset == "Mixed Case/name"
    assert sorted(str(path.relative_to(out_dir)) for path in out_dir.rglob("*")) == [
        "splits",
        "splits/test",
        "splits/test/Task_1",
        "splits/test/Task_1/assets",
        "splits/test/Task_1/assets/run.sh",
        "splits/test/Task_1/instruction.md",
        "splits/test/Task_1/task.toml",
        "splits/test/task-2.0",
        "splits/test/task-2.0/instruction.md",
        "splits/test/task-2.0/task.toml",
    ]
    for task_id, source in ExportBenchmark.tasks.items():
        target = out_dir / "splits" / "test" / task_id
        for source_path in [source, *source.rglob("*")]:
            copied = target / source_path.relative_to(source)
            assert S_IMODE(copied.stat().st_mode) == S_IMODE(source_path.stat().st_mode)
            if source_path.is_file():
                assert copied.read_bytes() == source_path.read_bytes()


@pytest.mark.parametrize("filename", ["task.toml", "instruction.md"])
def test_split_rejects_missing_required_file(tmp_path: Path, filename: str) -> None:
    task = harbor_task(tmp_path / "task")
    (task / filename).unlink()
    with pytest.raises(ValueError):
        write_harbor_split({"task": task}, "test", tmp_path / "out")


@pytest.mark.parametrize(
    "task_id", ["", ".", "..", "../task", "a/b", "/task", "_task", "-task", "a b", "tâsk", "task\n"]
)
def test_split_rejects_bad_task_id(tmp_path: Path, task_id: str) -> None:
    task = harbor_task(tmp_path / "task")
    with pytest.raises(ValueError):
        write_harbor_split({task_id: task}, "test", tmp_path / "out")


@pytest.mark.parametrize("kind", ["root", "file", "directory", "broken", "required"])
def test_split_rejects_symlinks(tmp_path: Path, kind: str) -> None:
    task = harbor_task(tmp_path / "task")
    if kind == "root":
        link = tmp_path / "linked-task"
        link.symlink_to(task, target_is_directory=True)
        task = link
    elif kind == "file":
        (task / "linked-file").symlink_to(task / "task.toml")
    elif kind == "directory":
        assets = task / "assets"
        assets.mkdir()
        (assets / "linked-directory").symlink_to(tmp_path, target_is_directory=True)
    elif kind == "broken":
        assets = task / "assets"
        assets.mkdir()
        (assets / "broken-link").symlink_to(tmp_path / "missing")
    elif kind == "required":
        (task / "instruction.md").unlink()
        (task / "instruction.md").symlink_to(task / "task.toml")
    else:
        raise AssertionError(kind)
    with pytest.raises(ValueError):
        write_harbor_split({"task": task}, "test", tmp_path / "out")


@pytest.mark.parametrize("contents", [b"[invalid", b'name = "\xff"'])
def test_split_rejects_invalid_toml(tmp_path: Path, contents: bytes) -> None:
    task = harbor_task(tmp_path / "task")
    _ = (task / "task.toml").write_bytes(contents)
    with pytest.raises(ValueError):
        write_harbor_split({"task": task}, "test", tmp_path / "out")


def test_split_rejects_empty_instruction(tmp_path: Path) -> None:
    task = harbor_task(tmp_path / "task")
    _ = (task / "instruction.md").write_text("", encoding="utf-8")
    with pytest.raises(ValueError):
        write_harbor_split({"task": task}, "test", tmp_path / "out")


def test_cli_export_error_has_error_exit_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    task = harbor_task(tmp_path / "task")
    (task / "task.toml").unlink()
    monkeypatch.setattr(ExportBenchmark, "tasks", {"task": task})
    result = CliRunner().invoke(
        service,
        [
            "export-dataset",
            "--service",
            f"{__name__}:ExportBenchmark",
            "--dataset",
            "Mixed Case/name",
            "--out",
            str(tmp_path / "out"),
        ],
    )
    assert result.exit_code not in (0, 3)
    assert isinstance(result.exception, ValueError)


@pytest.mark.parametrize("split", ["", "..", "../test", "/test", "a/b"])
def test_split_rejects_invalid_split_path(tmp_path: Path, split: str) -> None:
    task = harbor_task(tmp_path / "task")
    with pytest.raises(ValueError):
        write_harbor_split({"task": task}, split, tmp_path / "out")


def test_split_rejects_task_id_in_another_split(tmp_path: Path) -> None:
    task = harbor_task(tmp_path / "task")
    out_dir = tmp_path / "out"
    write_harbor_split({"task": task}, "train", out_dir)
    with pytest.raises(ValueError, match="Duplicate task ID"):
        write_harbor_split({"task": task}, "test", out_dir)
    assert not (out_dir / "splits" / "test").exists()


def test_split_allows_distinct_task_ids_across_splits(tmp_path: Path) -> None:
    first = harbor_task(tmp_path / "first")
    second = harbor_task(tmp_path / "second")
    out_dir = tmp_path / "out"
    write_harbor_split({"first": first}, "train", out_dir)
    write_harbor_split({"second": second}, "test", out_dir)
    assert (out_dir / "splits" / "train" / "first" / "instruction.md").read_bytes() == (
        first / "instruction.md"
    ).read_bytes()
    assert (out_dir / "splits" / "test" / "second" / "instruction.md").read_bytes() == (
        second / "instruction.md"
    ).read_bytes()
