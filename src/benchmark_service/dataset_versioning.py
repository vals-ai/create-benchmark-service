"""Hash-guarded dataset version tracking.

A dataset_versions.yaml next to the dataset files maps each dataset name to a
human-assigned semver and a sha256 of its content file. Version semantics:
major = scores not comparable, minor = additive, patch = non-scoring fixes.
"""

import hashlib
import sys
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import BaseModel


class DatasetVersionError(Exception):
    """Dataset content does not match its declared version entry."""


class DatasetVersionEntry(BaseModel):
    file: str
    version: str
    sha256: str


def compute_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_versions_mapping(versions_file: Path) -> dict[str, Any]:
    """Load the versions file as a mapping; an empty or non-mapping file is a clear error."""
    data = yaml.safe_load(versions_file.read_text())
    if not isinstance(data, dict):
        raise DatasetVersionError(
            f"{versions_file} must be a YAML mapping of dataset name to entry, got {type(data).__name__}"
        )
    return cast("dict[str, Any]", data)


def load_dataset_versions(versions_file: Path) -> dict[str, DatasetVersionEntry]:
    return {name: DatasetVersionEntry.model_validate(entry) for name, entry in _load_versions_mapping(versions_file).items()}


def load_verified_dataset_versions(versions_file: Path) -> dict[str, DatasetVersionEntry]:
    """Load entries and verify every dataset file matches its declared checksum.

    Raises DatasetVersionError on any mismatch: content that does not match its
    declared version must never be served.
    """
    entries = load_dataset_versions(versions_file)
    data_dir = versions_file.parent
    mismatches: list[str] = []
    for name, entry in entries.items():
        actual = compute_checksum(data_dir / entry.file)
        if actual != entry.sha256:
            mismatches.append(f"{name} ({entry.file}): declared {entry.sha256}, actual {actual}")
    if mismatches:
        raise DatasetVersionError(
            "dataset content does not match dataset_versions.yaml — bump the version, then run "
            "`python -m benchmark_service.dataset_versioning update <file>`:\n  " + "\n  ".join(mismatches)
        )
    return entries


def main(argv: list[str]) -> int:
    if len(argv) != 2 or argv[0] not in ("check", "update"):
        print("usage: python -m benchmark_service.dataset_versioning {check|update} <dataset_versions.yaml>")
        return 2
    command, versions_file = argv[0], Path(argv[1])
    if command == "check":
        try:
            load_verified_dataset_versions(versions_file)
        except DatasetVersionError as exc:
            print(exc)
            return 1
        print("dataset checksums OK")
        return 0
    raw = _load_versions_mapping(versions_file)
    for entry in raw.values():
        entry["sha256"] = compute_checksum(versions_file.parent / entry["file"])
    versions_file.write_text(yaml.safe_dump(raw, sort_keys=False))
    print(f"updated {versions_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
