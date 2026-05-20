"""Tests for framework version exposure."""

import re
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import patch

from daytona import AsyncSandbox
from fastapi.testclient import TestClient

import benchmark_service
from benchmark_service.app import BenchmarkServiceApp, _get_service_metadata  # pyright: ignore[reportPrivateUsage]
from benchmark_service.base import BenchmarkService
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
)


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
        self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        raise NotImplementedError

    def evaluate_instance(
        self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
        raise NotImplementedError


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
