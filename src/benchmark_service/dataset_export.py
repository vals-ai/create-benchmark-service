"""Copy Harbor tasks into the vals-datasets layout."""

import json
import re
import shutil
import tomllib
from collections.abc import Mapping
from pathlib import Path

SAFE_NAME = r"[A-Za-z0-9][A-Za-z0-9_.-]*"


def task_directory(task_id: str) -> str:
    """Return the storage directory for a task ID. Unsafe IDs map to a deterministic safe name."""
    if re.fullmatch(SAFE_NAME, task_id):
        return task_id
    name = re.sub(r"[^A-Za-z0-9_.-]+", "-", task_id).strip("-_.")
    if not re.fullmatch(SAFE_NAME, name):
        raise ValueError(f"Invalid task ID: {task_id!r}")
    return name


def write_harbor_split(tasks: Mapping[str, Path], split: str, out_dir: Path) -> None:
    """Copy task directories and preserve their file modes. Reject symlinks.

    A task ID that is not a safe directory name keeps its exact ID in
    task.toml as [metadata.vals_datasets] id, as the vals-datasets format requires.
    """
    if not re.fullmatch(SAFE_NAME, split):
        raise ValueError(f"Invalid split: {split!r}")
    directories: dict[str, str] = {}
    for task_id, source in tasks.items():
        directory = task_directory(task_id)
        if directory in directories.values() or any((out_dir / "splits").glob(f"*/{directory}")):
            raise ValueError(f"Duplicate task directory for task ID: {task_id!r}")
        directories[task_id] = directory
        if source.is_symlink() or any(path.is_symlink() for path in source.rglob("*")):
            raise ValueError(f"Task {task_id!r} contains a symlink")
        for name in ("task.toml", "instruction.md"):
            if not (source / name).is_file():
                raise ValueError(f"Task {task_id!r} requires {name}")
        manifest = tomllib.loads((source / "task.toml").read_text(encoding="utf-8"))
        if directory != task_id and "vals_datasets" in manifest.get("metadata", {}):
            raise ValueError(f"Task {task_id!r} already sets metadata.vals_datasets")
        if not (source / "instruction.md").read_text(encoding="utf-8").strip():
            raise ValueError(f"Task {task_id!r} requires a nonempty instruction.md")
    for task_id, source in tasks.items():
        target = out_dir / "splits" / split / directories[task_id]
        shutil.copytree(source, target)
        if directories[task_id] != task_id:
            manifest_path = target / "task.toml"
            text = manifest_path.read_text(encoding="utf-8")
            manifest_path.write_text(
                f"{text.rstrip()}\n\n[metadata.vals_datasets]\nid = {json.dumps(task_id)}\n",
                encoding="utf-8",
            )
            written = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
            assert written["metadata"]["vals_datasets"]["id"] == task_id
