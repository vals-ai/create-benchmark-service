"""Tests for hash-guarded dataset version tracking."""

from pathlib import Path

import pytest
import yaml

from benchmark_service.dataset_versioning import (
    DatasetVersionError,
    compute_checksum,
    load_verified_dataset_versions,
    main,
)

from tests.conftest import StubBenchmark


def _write_fixture(tmp_path: Path, content: bytes = b'{"tests": []}') -> Path:
    data_file = tmp_path / "validation.json"
    data_file.write_bytes(content)
    versions_file = tmp_path / "dataset_versions.yaml"
    versions_file.write_text(
        yaml.safe_dump(
            {
                "validation": {
                    "file": "validation.json",
                    "version": "1.0.0",
                    "sha256": compute_checksum(data_file),
                }
            }
        )
    )
    return versions_file


def test_load_verified_returns_entries_when_content_matches(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)
    entries = load_verified_dataset_versions(versions_file)
    assert entries["validation"].version == "1.0.0"


def test_load_verified_raises_on_content_mismatch(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)
    (tmp_path / "validation.json").write_bytes(b'{"tests": [1]}')
    with pytest.raises(DatasetVersionError, match="validation"):
        load_verified_dataset_versions(versions_file)


def test_check_command_fails_on_mismatch_and_update_repairs(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)
    (tmp_path / "validation.json").write_bytes(b'{"tests": [1]}')
    assert main(["check", str(versions_file)]) == 1
    assert main(["update", str(versions_file)]) == 0
    assert main(["check", str(versions_file)]) == 0


async def test_service_startup_verifies_and_serves_dataset_versions(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)

    class VersionedBenchmark(StubBenchmark):
        dataset_versions_file = versions_file

    service = await VersionedBenchmark.create()
    assert service.get_dataset_version("validation") == "1.0.0"
    assert service.get_dataset_version("unknown") is None


async def test_service_startup_fails_on_checksum_mismatch(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)
    (tmp_path / "validation.json").write_bytes(b"tampered")

    class VersionedBenchmark(StubBenchmark):
        dataset_versions_file = versions_file

    with pytest.raises(DatasetVersionError):
        await VersionedBenchmark.create()
