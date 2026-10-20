"""Dataset-name discovery across the service, hosted policy, and Python client."""

import json
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from benchmark_service import (
    BenchmarkServiceApp,
    BenchmarkServiceClient,
    BenchmarkServiceError,
    BenchmarkServiceUnauthenticatedError,
    ListDatasetsResponse,
)
from benchmark_service.vals import auth
from benchmark_service.vals.app import ValsBenchmarkServiceApp
from tests.conftest import StubBenchmark, ValsStubBenchmark


def test_lists_names_without_task_content(client: TestClient) -> None:
    response = client.get("/v1/datasets")
    assert response.status_code == 200
    assert response.json() == {"datasets": ["alt", "default"]}


def test_dynamic_discovery_does_not_load_or_resolve_datasets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)

    class DynamicBenchmark(StubBenchmark):
        async def list_datasets(self) -> list[str]:
            return ["remote", "hidden", "remote", "another"]

        async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
            return dataset != "hidden"

    app = BenchmarkServiceApp(DynamicBenchmark)
    with TestClient(app) as client:
        # Startup has finished; discovery must not load task data again.
        load = AsyncMock(side_effect=AssertionError("Discovery loaded tasks"))
        monkeypatch.setattr(app.service, "load_datasets", load)
        monkeypatch.setattr(app.service, "get_dataset", load)
        monkeypatch.setattr(app.service, "open_dataset_version", load)
        response = client.get("/v1/datasets")
        assert response.status_code == 200
        assert response.json() == {"datasets": ["another", "remote"]}
        load.assert_not_called()


@pytest.mark.parametrize("names", [[], ["unavailable"]])
def test_empty_inventory(monkeypatch: pytest.MonkeyPatch, names: list[str]) -> None:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)

    class EmptyBenchmark(StubBenchmark):
        async def list_datasets(self) -> list[str]:
            return names

        async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
            return False

    with TestClient(BenchmarkServiceApp(EmptyBenchmark)) as client:
        response = client.get("/v1/datasets")
        assert response.status_code == 200
        assert response.json() == {"datasets": []}


@pytest.mark.parametrize(
    ("tenant", "grants", "trial", "expected"),
    [
        ("reader", ["default", "nonexistent"], False, ["default"]),
        ("reader", [], False, []),
        ("reader", ["alt", "default"], False, ["alt", "default"]),
        ("reader", ["default"], True, ["default"]),
        ("unknown", ["default"], False, None),
        (None, ["default"], False, None),
    ],
)
def test_hosted_discovery_obeys_authentication_and_dataset_grants(
    monkeypatch: pytest.MonkeyPatch,
    tenant: str | None,
    grants: list[str],
    trial: bool,
    expected: list[str] | None,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "test-project")
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"reader": {"datasets": grants, "trial_mode": trial}}}),
    )
    exchange = AsyncMock(return_value={"tenants": {tenant: {}}})
    monkeypatch.setattr(auth, "_exchange_descope_access_key", exchange)
    app = ValsBenchmarkServiceApp(ValsStubBenchmark)
    with TestClient(app) as client:
        discovery = AsyncMock(wraps=app.service.list_datasets)
        monkeypatch.setattr(app.service, "list_datasets", discovery)
        response = client.get("/v1/datasets", headers={"x-descope-api-key": "test-key"} if tenant else {})
        if expected is None:
            assert response.status_code == 401
            discovery.assert_not_called()
        else:
            assert response.status_code == 200
            assert response.json() == {"datasets": expected}


@pytest.mark.parametrize("version", [None, "v1.0"])
async def test_client_discovers_names_from_app_without_version_acknowledgement(
    monkeypatch: pytest.MonkeyPatch, version: str | None
) -> None:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    app = BenchmarkServiceApp(StubBenchmark)
    async with app.router.lifespan_context(app):
        async with BenchmarkServiceClient("http://test", {}, dataset_version=version) as client:
            original = client._http_client  # pyright: ignore[reportPrivateUsage]
            client._http_client = httpx.AsyncClient(  # pyright: ignore[reportPrivateUsage]
                headers=original.headers,
                event_hooks=original.event_hooks,
                transport=httpx.ASGITransport(app=app),
            )
            await original.aclose()
            result = await client.list_datasets()
            assert isinstance(result, ListDatasetsResponse)
            assert result.datasets == ["alt", "default"]


@pytest.mark.parametrize("status", [401, 403, 404, 503])
async def test_client_preserves_errors_including_older_services(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock], status: int
) -> None:
    client, http = benchmark_client
    http.get.return_value = httpx.Response(status, json={"detail": "Not available"})
    error_type = BenchmarkServiceUnauthenticatedError if status == 401 else BenchmarkServiceError
    with pytest.raises(error_type) as failure:
        await client.list_datasets()
    assert failure.value.status_code == status
    http.get.assert_called_once_with("http://localhost:8000/v1/datasets")


@pytest.mark.parametrize("body", [{}, {"datasets": [123]}, {"datasets": "default"}])
async def test_client_validates_discovery_response(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock], body: dict[str, object]
) -> None:
    client, http = benchmark_client
    http.get.return_value = httpx.Response(200, json=body)
    with pytest.raises(ValidationError):
        await client.list_datasets()


def test_discovery_preserves_service_owned_datasets_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    app = BenchmarkServiceApp(StubBenchmark)

    async def legacy_datasets() -> list[dict[str, str]]:
        return [{"dataset_id": "existing", "dataset_uri": "s3://example/manifest.json"}]

    app.add_api_route("/datasets", legacy_datasets, methods=["GET"])
    with TestClient(app) as client:
        assert client.get("/datasets").json() == [
            {"dataset_id": "existing", "dataset_uri": "s3://example/manifest.json"}
        ]
        assert client.get("/v1/datasets").json() == {"datasets": ["alt", "default"]}
