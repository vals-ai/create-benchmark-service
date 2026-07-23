"""Fixtures for live sandbox provider integration tests.

Run: AWS_PROFILE=vals uv run pytest tests/integration
"""

import json
import os
from collections.abc import AsyncGenerator
from typing import Any, Literal, cast

import boto3
import pytest

from benchmark_service import (
    SandboxProvider,
    SandboxProviderConfig,
    sandbox_provider_config_from_mapping,
)

ProviderType = Literal["daytona", "modal"]

_PROVIDER_SECRET_ENV_NAMES: dict[ProviderType, str] = {
    "daytona": "TEST_DAYTONA_SECRET_NAME",
    "modal": "TEST_MODAL_SECRET_NAME",
}


@pytest.fixture(params=tuple(_PROVIDER_SECRET_ENV_NAMES), ids=tuple(_PROVIDER_SECRET_ENV_NAMES))
def provider_type(request: pytest.FixtureRequest) -> ProviderType:
    """Select each supported provider as an independent pytest case."""
    return cast(ProviderType, request.param)


@pytest.fixture
def sandbox_provider_config(provider_type: ProviderType) -> SandboxProviderConfig:
    """Load and validate a provider config from its AWS Secrets Manager JSON."""
    aws_region = os.environ.get("TEST_AWS_REGION")
    secret_env_name = _PROVIDER_SECRET_ENV_NAMES[provider_type]
    secret_name = os.environ.get(secret_env_name)
    if not aws_region or not secret_name:
        pytest.fail(f"TEST_AWS_REGION and {secret_env_name} must be set to run provider integration tests.")

    secrets_client = cast(Any, boto3).client("secretsmanager", region_name=aws_region)
    try:
        response = cast(
            dict[str, object],
            secrets_client.get_secret_value(SecretId=secret_name),
        )
    finally:
        secrets_client.close()

    secret_string = response.get("SecretString")
    if not isinstance(secret_string, str):
        pytest.fail(f"Provider secret for {provider_type} must contain SecretString JSON.")

    try:
        secret_data = cast(object, json.loads(secret_string))
    except json.JSONDecodeError as exc:
        pytest.fail(f"Provider secret for {provider_type} must contain valid JSON: {exc}")

    if not isinstance(secret_data, dict):
        pytest.fail(f"Provider secret for {provider_type} must be a JSON object with string keys and values.")

    provider_data = cast(dict[object, object], secret_data)
    if not all(isinstance(key, str) and isinstance(value, str) for key, value in provider_data.items()):
        pytest.fail(f"Provider secret for {provider_type} must be a JSON object with string keys and values.")

    provider_config = cast(dict[str, str], provider_data)
    return sandbox_provider_config_from_mapping({**provider_config, "type": provider_type})


@pytest.fixture
async def sandbox_provider(
    sandbox_provider_config: SandboxProviderConfig,
) -> AsyncGenerator[SandboxProvider, None]:
    """Yield a real provider and always close its client resources."""
    provider = sandbox_provider_config.create_provider()
    try:
        yield provider
    finally:
        await provider.close()
