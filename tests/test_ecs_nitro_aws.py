from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from botocore.credentials import Credentials

from benchmark_service.sandbox.ecs_nitro import aws
from benchmark_service.sandbox.ecs_nitro.aws import AwsApiError, EcsJsonClient


class _Session:
    def get_credentials(self) -> Credentials:
        return Credentials("AKIDEXAMPLE", "secret", "session-token")


def _ecs_client(responses: list[Any], seen: list[httpx.Request]) -> EcsJsonClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        status, body = result
        return httpx.Response(status, json=body)

    client = EcsJsonClient("us-east-1")
    client._session = _Session()  # pyright: ignore[reportPrivateUsage]
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # pyright: ignore[reportPrivateUsage]
    return client


@pytest.fixture
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    from tenacity import wait_none

    def no_wait(**_: float) -> wait_none:
        return wait_none()

    monkeypatch.setattr(aws, "wait_random_exponential", no_wait)


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_backoff")
async def test_ecs_client_retries_throttling_and_transport_errors_with_fresh_signatures() -> None:
    seen: list[httpx.Request] = []
    responses: list[Any] = [
        httpx.ConnectError("reset"),
        (400, {"__type": "ThrottlingException", "message": "Rate exceeded"}),
        (503, {}),
        (200, {"tasks": [{"taskArn": "arn"}]}),
    ]
    client = _ecs_client(responses, seen)
    result = await client.call("RunTask", {"clientToken": "sandbox-1"})
    assert result == {"tasks": [{"taskArn": "arn"}]}
    assert len(seen) == 4
    assert {json.loads(r.content)["clientToken"] for r in seen} == {"sandbox-1"}
    assert all(r.headers["Authorization"].startswith("AWS4-HMAC-SHA256") for r in seen)


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_backoff")
@pytest.mark.parametrize(
    ("status", "code"),
    [(403, "AccessDeniedException"), (401, "UnrecognizedClientException"), (400, "InvalidParameterException")],
)
async def test_ecs_client_does_not_retry_auth_or_validation_errors(status: int, code: str) -> None:
    seen: list[httpx.Request] = []
    client = _ecs_client([(status, {"__type": code, "message": "no"})], seen)
    with pytest.raises(AwsApiError, match=code):
        await client.call("RunTask", {})
    assert len(seen) == 1
