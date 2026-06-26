"""Tests for dataset version tracking (version labels)."""

from pathlib import Path

import pytest
import yaml

from benchmark_service.dataset_versioning import DatasetVersionError, load_dataset_versions

from tests.conftest import StubBenchmark


def _write_fixture(tmp_path: Path, version: str = "1.0.0") -> Path:
    versions_file = tmp_path / "dataset_versions.yaml"
    versions_file.write_text(yaml.safe_dump({"validation": {"version": version}}))
    return versions_file


def test_load_dataset_versions_reads_declared_version(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)
    assert load_dataset_versions(versions_file)["validation"].version == "1.0.0"


def test_non_mapping_versions_file_raises_clear_error(tmp_path: Path) -> None:
    versions_file = tmp_path / "dataset_versions.yaml"
    versions_file.write_text("# no entries yet\n")
    with pytest.raises(DatasetVersionError, match="must be a YAML mapping"):
        load_dataset_versions(versions_file)


async def test_service_serves_declared_dataset_version(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)

    class VersionedBenchmark(StubBenchmark):
        dataset_versions_file = versions_file

    service = await VersionedBenchmark.create()
    assert service.get_dataset_version("validation") == "1.0.0"
    assert service.get_dataset_version("unknown") is None
