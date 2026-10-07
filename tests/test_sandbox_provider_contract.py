from typing import ClassVar

import pytest
from fastapi.testclient import TestClient

from benchmark_service import SandboxProviderName
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.sandbox import ImageSource
from benchmark_service.schemas import RetrieveTaskResponse
from tests.conftest import StubBenchmark


class ProviderAwareBenchmark(StubBenchmark):
    sandbox_providers: ClassVar[tuple[SandboxProviderName, ...]] = ("modal", "daytona")
    default_sandbox_provider: ClassVar[SandboxProviderName | None] = "modal"

    async def retrieve_task(
        self,
        task_id: str,
        skip_validation: bool = False,
        dataset: str | None = None,
        sandbox_provider: SandboxProviderName | None = None,
    ) -> RetrieveTaskResponse:
        response = await super().retrieve_task(task_id, skip_validation, dataset)
        return response.model_copy(update={"source": ImageSource(image=f"image-for-{sandbox_provider}")})


@pytest.fixture
def provider_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    return TestClient(BenchmarkServiceApp(ProviderAwareBenchmark))


@pytest.mark.parametrize("provider", ["modal", "daytona"])
def test_retrieve_task_passes_requested_provider(provider_client: TestClient, provider: str) -> None:
    with provider_client as client:
        response = client.get("/retrieve-task/", params={"task_id": "task-1", "sandbox_provider": provider})

    assert response.status_code == 200, response.text
    assert response.json()["source"]["image"] == f"image-for-{provider}"


def test_retrieve_task_uses_default_provider_when_missing(provider_client: TestClient) -> None:
    with provider_client as client:
        response = client.get("/retrieve-task/", params={"task_id": "task-1"})

    assert response.status_code == 200, response.text
    assert response.json()["source"]["image"] == "image-for-modal"


def test_retrieve_task_rejects_unsupported_provider(provider_client: TestClient) -> None:
    with provider_client as client:
        unsupported = client.get("/retrieve-task/", params={"task_id": "task-1", "sandbox_provider": "docker"})
        unknown = client.get("/retrieve-task/", params={"task_id": "task-1", "sandbox_provider": "nope"})

    assert unsupported.status_code == 400
    assert "Sandbox provider 'docker' is not supported" in unsupported.json()["detail"]
    assert unknown.status_code == 422


def test_legacy_service_ignores_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        response = client.get("/retrieve-task/", params={"task_id": "task-1", "sandbox_provider": "daytona"})

    assert response.status_code == 200, response.text
    assert response.json()["source"]["image"] == "python:3.12-slim"


def test_app_rejects_default_outside_declared_providers() -> None:
    class MisdeclaredBenchmark(ProviderAwareBenchmark):
        default_sandbox_provider: ClassVar[SandboxProviderName | None] = "docker"

    with pytest.raises(ValueError, match="default_sandbox_provider must be one of"):
        BenchmarkServiceApp(MisdeclaredBenchmark)


def test_version_reports_sandbox_providers(provider_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    with provider_client as client:
        declared = client.get("/version").json()
    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        legacy = client.get("/version").json()

    assert declared["sandbox_providers"] == ["modal", "daytona"]
    assert declared["default_sandbox_provider"] == "modal"
    assert legacy["sandbox_providers"] == []
    assert legacy["default_sandbox_provider"] is None
