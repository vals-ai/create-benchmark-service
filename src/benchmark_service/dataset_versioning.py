"""Dataset version tracking.

A dataset_versions.yaml maps each dataset name to a human-assigned semver.
Version semantics: major = scores not comparable, minor = additive,
patch = non-scoring fixes. The version is served by get_dataset_version() and
flows into manifests. This records the declared version only; it does not read
or verify dataset content.
"""

from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import BaseModel


class DatasetVersionError(Exception):
    """dataset_versions.yaml is not a valid mapping of dataset name to entry."""


class DatasetVersionEntry(BaseModel):
    version: str


def load_dataset_versions(versions_file: Path) -> dict[str, DatasetVersionEntry]:
    """Load the declared semver for each dataset name from a dataset_versions.yaml."""
    data = yaml.safe_load(versions_file.read_text())
    if not isinstance(data, dict):
        raise DatasetVersionError(
            f"{versions_file} must be a YAML mapping of dataset name to entry, got {type(data).__name__}"
        )
    return {
        name: DatasetVersionEntry.model_validate(entry)
        for name, entry in cast("dict[str, Any]", data).items()
    }
