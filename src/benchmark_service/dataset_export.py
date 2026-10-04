"""Copy Harbor tasks into the vals-datasets layout."""

import re
import shutil
import tomllib
from collections.abc import Mapping
from pathlib import Path


def write_harbor_split(tasks: Mapping[str, Path], split: str, out_dir: Path) -> None:
    """Copy task directories and preserve their file modes. Reject symlinks."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", split):
        raise ValueError(f"Invalid split: {split!r}")
    for task_id, source in tasks.items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", task_id):
            raise ValueError(f"Invalid task ID: {task_id!r}")
        if any((out_dir / "splits").glob(f"*/{task_id}")):
            raise ValueError(f"Duplicate task ID: {task_id!r}")
        if source.is_symlink() or any(path.is_symlink() for path in source.rglob("*")):
            raise ValueError(f"Task {task_id!r} contains a symlink")
        for name in ("task.toml", "instruction.md"):
            if not (source / name).is_file():
                raise ValueError(f"Task {task_id!r} requires {name}")
        tomllib.loads((source / "task.toml").read_text(encoding="utf-8"))
        if not (source / "instruction.md").read_text(encoding="utf-8").strip():
            raise ValueError(f"Task {task_id!r} requires a nonempty instruction.md")
    for task_id, source in tasks.items():
        shutil.copytree(source, out_dir / "splits" / split / task_id)
