"""Tests for framework version exposure."""

import re
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import benchmark_service
from benchmark_service import Sandbox
from benchmark_service.app import BenchmarkServiceApp, _get_service_metadata  # pyright: ignore[reportPrivateUsage]
from benchmark_service.base import BenchmarkService
from benchmark_service.schemas import (
    EvalMode,
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
)

from tests.test_dataset_versioning import _write_fixture  # pyright: ignore[reportPrivateUsage]


def test_version_is_importable_and_well_formed() -> None:
    assert isinstance(benchmark_service.__version__, str)
    # Tagless dev builds, clean semver, and post-tag dev strings all start with digit(s).digit(s).
    assert re.match(r"^\d+\.\d+", benchmark_service.__version__)


class _FakeService(BenchmarkService):
    """Test subclass — abstract methods are not invoked in these tests."""

    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        return {"default": {}}

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        raise NotImplementedError

    def setup_task(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        raise NotImplementedError

    def evaluate_instance(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
        raise NotImplementedError


class _VersionedService(_FakeService):
    def get_service_version(self) -> str | None:
        return "service-hook-1.2.3"


def _service_cls_in_module(module_name: str) -> type[BenchmarkService]:
    """Build a throwaway BenchmarkService subclass whose __module__ is `module_name`."""
    return type(_FakeService.__name__, (_FakeService,), {"__module__": module_name})


def test_service_metadata_resolves_installed_distribution() -> None:
    fake_packages = {"some_pkg": ["create-benchmark-service"]}
    with (
        patch("benchmark_service.app.importlib.metadata.packages_distributions", return_value=fake_packages),
        patch("benchmark_service.app.importlib.metadata.version", return_value="1.2.3"),
    ):
        name, version = _get_service_metadata(_service_cls_in_module("some_pkg"))
    assert name == "create-benchmark-service"
    assert version == "1.2.3"


def test_service_metadata_returns_nulls_when_distribution_unknown() -> None:
    with patch("benchmark_service.app.importlib.metadata.packages_distributions", return_value={}):
        name, version = _get_service_metadata(_service_cls_in_module("nonexistent_pkg"))
    assert name is None
    assert version is None


def test_version_endpoint_returns_framework_version() -> None:
    app = BenchmarkServiceApp(_FakeService)
    with TestClient(app) as client:
        response = client.get("/version")

    assert response.status_code == 200
    body = response.json()
    assert body["framework_version"] == benchmark_service.__version__
    assert "service_name" in body
    assert "service_version" in body


def test_version_endpoint_prefers_service_version_hook() -> None:
    app = BenchmarkServiceApp(_VersionedService)
    with TestClient(app) as client:
        response = client.get("/version")

    assert response.status_code == 200
    assert response.json()["service_version"] == "service-hook-1.2.3"


def test_version_endpoint_reports_dataset_version_key() -> None:
    app = BenchmarkServiceApp(_FakeService)
    with TestClient(app) as client:
        response = client.get("/version", params={"dataset": "default"})

    assert response.status_code == 200
    assert "dataset_version" in response.json()


def test_version_endpoint_reports_tracked_dataset_version(tmp_path: Path) -> None:
    versions_file = _write_fixture(tmp_path)

    class _DatasetVersionedService(_FakeService):
        dataset_versions_file = versions_file

    app = BenchmarkServiceApp(_DatasetVersionedService)
    with TestClient(app) as client:
        response = client.get("/version", params={"dataset": "validation"})

    assert response.status_code == 200
    assert response.json()["dataset_version"] == "1.0.0"


class _SandboxModeService(_FakeService):
    def eval_mode(self) -> EvalMode:
        return EvalMode.SANDBOX


def test_version_reports_text_eval_mode_by_default() -> None:
    app = BenchmarkServiceApp(_FakeService)
    with TestClient(app) as client:
        response = client.get("/version")
    assert response.status_code == 200
    assert response.json()["eval_mode"] == "text"


def test_version_reports_sandbox_eval_mode_when_overridden(monkeypatch: pytest.MonkeyPatch) -> None:
    # SANDBOX mode requires grading + artifact config at boot.
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("DAYTONA_API_KEY", "k")
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example")
    monkeypatch.setenv("DAYTONA_TARGET", "us")
    app = BenchmarkServiceApp(_SandboxModeService)
    with TestClient(app) as client:
        response = client.get("/version")
    assert response.status_code == 200
    assert response.json()["eval_mode"] == "sandbox"
