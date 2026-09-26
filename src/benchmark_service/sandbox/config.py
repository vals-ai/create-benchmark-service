"""Sandbox provider configuration that can be parsed without provider SDKs."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Literal, cast

from pydantic import BaseModel, Field, field_validator

from benchmark_service.sandbox.types import MissingSandboxConfigError, SandboxProvider


def _get_config_header(headers: Mapping[str, str], *names: str) -> str | None:
    normalized_headers = {key.lower(): value for key, value in headers.items()}
    for name in names:
        value = normalized_headers.get(name.lower())
        if value:
            return value
    return None


class DaytonaProviderConfig(BaseModel):
    type: Literal["daytona"] = "daytona"
    DAYTONA_API_KEY: str
    DAYTONA_API_URL: str
    DAYTONA_TARGET: str
    DAYTONA_ORGANIZATION_ID: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @field_validator("DAYTONA_API_URL")
    @classmethod
    def _normalize_api_url(cls, value: str) -> str:
        return value.rstrip("/")

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> "DaytonaProviderConfig":
        api_key = _get_config_header(headers, "x-api-key", "daytona_api_key")
        api_url = _get_config_header(headers, "x-api-url", "daytona_api_url")
        target = _get_config_header(headers, "x-target", "daytona_target")
        organization_id = _get_config_header(headers, "x-organization-id")
        if not api_key or not api_url or not target:
            raise MissingSandboxConfigError("Missing required headers: x-api-key, x-api-url, x-target")
        return cls(
            DAYTONA_API_KEY=api_key,
            DAYTONA_API_URL=api_url,
            DAYTONA_TARGET=target,
            DAYTONA_ORGANIZATION_ID=organization_id,
        )

    @classmethod
    def from_env(cls) -> "DaytonaProviderConfig":
        """Build config from the DAYTONA_* environment variables; callers never supply creds."""
        api_key = os.environ.get("DAYTONA_API_KEY")
        api_url = os.environ.get("DAYTONA_API_URL")
        target = os.environ.get("DAYTONA_TARGET")
        organization_id = os.environ.get("DAYTONA_ORGANIZATION_ID")
        missing = [
            name
            for name, value in (
                ("DAYTONA_API_KEY", api_key),
                ("DAYTONA_API_URL", api_url),
                ("DAYTONA_TARGET", target),
            )
            if not value
        ]
        if missing:
            raise MissingSandboxConfigError(f"Missing required environment variables: {', '.join(missing)}")
        return cls(
            DAYTONA_API_KEY=cast(str, api_key),
            DAYTONA_API_URL=cast(str, api_url),
            DAYTONA_TARGET=cast(str, target),
            DAYTONA_ORGANIZATION_ID=organization_id,
        )

    def create_provider(self) -> SandboxProvider:
        try:
            from benchmark_service.sandbox.daytona import DaytonaSandboxProvider
        except ModuleNotFoundError as exc:
            if exc.name is None or exc.name.split(".", 1)[0] not in {
                "aiohttp",
                "daytona",
                "daytona_api_client_async",
                "daytona_toolbox_api_client_async",
            }:
                raise
            raise ImportError(
                "The Daytona sandbox provider requires the daytona extra; "
                "install it with: uv add 'create-benchmark-service[daytona]'"
            ) from exc
        return DaytonaSandboxProvider(self)


class ModalProviderConfig(BaseModel):
    type: Literal["modal"] = "modal"
    runtime: Literal["gvisor", "vm"] = "gvisor"
    MODAL_TOKEN_ID: str
    MODAL_TOKEN_SECRET: str

    @classmethod
    def from_env(cls) -> "ModalProviderConfig":
        token_id = os.environ.get("MODAL_TOKEN_ID")
        token_secret = os.environ.get("MODAL_TOKEN_SECRET")
        missing = [
            name
            for name, value in (
                ("MODAL_TOKEN_ID", token_id),
                ("MODAL_TOKEN_SECRET", token_secret),
            )
            if not value
        ]
        if missing:
            raise MissingSandboxConfigError(f"Missing required environment variables: {', '.join(missing)}")
        return cls.model_validate(
            {
                "runtime": os.environ.get("MODAL_RUNTIME", "gvisor"),
                "MODAL_TOKEN_ID": token_id,
                "MODAL_TOKEN_SECRET": token_secret,
            }
        )

    def create_provider(self) -> SandboxProvider:
        try:
            from benchmark_service.sandbox.modal import ModalSandboxProvider
        except ModuleNotFoundError as exc:
            if exc.name is None or exc.name.split(".", 1)[0] != "modal":
                raise
            raise ImportError(
                "The Modal sandbox provider requires the modal extra; "
                "install it with: uv add 'create-benchmark-service[modal]'"
            ) from exc
        return ModalSandboxProvider(self)
