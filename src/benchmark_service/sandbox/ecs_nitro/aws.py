from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import Mapping
from typing import Any, Protocol

import httpx
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_random_exponential

_ECS_TARGET_PREFIX = "AmazonEC2ContainerServiceV20141113"
_MAX_ATTEMPTS = 8
_THROTTLE_CODES = frozenset(
    {
        "ThrottlingException",
        "Throttling",
        "TooManyRequestsException",
        "RequestLimitExceeded",
        "RequestThrottled",
        "RequestThrottledException",
        "ServerException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "InternalError",
        "InternalFailure",
    }
)
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class AwsApiError(Exception):
    def __init__(self, service: str, action: str, code: str, message: str) -> None:
        self.code = code
        self.message = message
        self.retryable = code in _THROTTLE_CODES or code in {str(status) for status in _RETRYABLE_STATUS}
        super().__init__(f"{service} {action} failed: {code}: {message}")


class EcsApi(Protocol):
    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class Ec2Api(Protocol):
    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]: ...


class EcsJsonClient:
    """ECS over its JSON 1.1 wire protocol, signed with SigV4.

    Botocore drops request and response members its bundled model lacks, and the
    botocore releases that model ``trustedExecutionConfiguration`` are newer than
    aiobotocore allows, so ECS calls bypass the model and keep every field.
    """

    def __init__(self, region: str, endpoint_url: str | None = None) -> None:
        self._region = region
        self._endpoint = endpoint_url or f"https://ecs.{region}.amazonaws.com/"
        self._session: Any = importlib.import_module("botocore.session").get_session()
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(60.0))

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        async for attempt in AsyncRetrying(
            retry=retry_if_exception(_transient),
            stop=stop_after_attempt(_MAX_ATTEMPTS),
            wait=wait_random_exponential(multiplier=0.5, max=20),
            reraise=True,
        ):
            with attempt:
                return await self._call_once(action, params)
        raise AssertionError("unreachable")

    async def _call_once(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        credentials: Any = await asyncio.to_thread(self._session.get_credentials)
        if credentials is None:
            raise AwsApiError("ecs", action, "NoCredentials", "no AWS credentials in the default provider chain")
        frozen: Any = await asyncio.to_thread(credentials.get_frozen_credentials)
        body = json.dumps(params)
        request = AWSRequest(
            method="POST",
            url=self._endpoint,
            data=body,
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": f"{_ECS_TARGET_PREFIX}.{action}",
            },
        )
        signer: Any = SigV4Auth(frozen, "ecs", self._region)
        signer.add_auth(request)
        response = await self._http.post(self._endpoint, content=body, headers=dict(request.headers.items()))
        try:
            payload: dict[str, Any] = response.json() if response.content else {}
        except ValueError:
            payload = {}
        if response.status_code >= 400:
            code = str(payload.get("__type") or response.status_code).rsplit("#", 1)[-1]
            if response.status_code in _RETRYABLE_STATUS and code not in _THROTTLE_CODES:
                code = str(response.status_code)
            message = str(payload.get("message") or payload.get("Message") or response.text[:500])
            raise AwsApiError("ecs", action, code, message)
        return payload

    async def close(self) -> None:
        await self._http.aclose()


def _transient(error: BaseException) -> bool:
    if isinstance(error, AwsApiError):
        return error.retryable
    return isinstance(error, httpx.TransportError)


class Boto3Ec2Client:
    def __init__(self, region: str) -> None:
        boto3: Any = importlib.import_module("boto3")
        config: Any = importlib.import_module("botocore.config").Config(
            retries={"mode": "standard", "max_attempts": _MAX_ATTEMPTS}
        )
        self._client: Any = boto3.client("ec2", region_name=region, config=config)

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        method = getattr(self._client, action)
        try:
            return await asyncio.to_thread(method, **params)
        except ClientError as error:
            response: Any = error.response
            details: dict[str, Any] = response.get("Error", {})
            raise AwsApiError("ec2", action, str(details.get("Code", "")), str(details.get("Message", ""))) from None
