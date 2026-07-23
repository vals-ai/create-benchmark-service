"""Validate provider configuration for the Kubernetes control service."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import AnyHttpUrl, BaseModel, Field, model_validator

from benchmark_service.sandbox.kubernetes.client import KubernetesControlClientDriver
from benchmark_service.sandbox.kubernetes.provider import KubernetesSandboxProvider
from benchmark_service.sandbox.types import SandboxProvider


class KubernetesProviderConfig(BaseModel):
    """Connection settings for the private Kubernetes sandbox control API."""

    type: Literal["kubernetes"] = "kubernetes"
    KUBERNETES_API_URL: AnyHttpUrl
    KUBERNETES_API_TOKEN: str = Field(min_length=1, repr=False)
    KUBERNETES_CONNECT_TIMEOUT: float = Field(default=10, gt=0)
    KUBERNETES_REQUEST_TIMEOUT: float = Field(default=60, gt=0)
    KUBERNETES_STREAM_READ_TIMEOUT: float = Field(default=45, gt=0)
    KUBERNETES_MAX_CONNECTIONS: int = Field(default=256, gt=0)
    KUBERNETES_MAX_KEEPALIVE_CONNECTIONS: int = Field(default=64, ge=0)

    @model_validator(mode="after")
    def validate_connection_limits(self) -> Self:
        if self.KUBERNETES_MAX_KEEPALIVE_CONNECTIONS > self.KUBERNETES_MAX_CONNECTIONS:
            raise ValueError("KUBERNETES_MAX_KEEPALIVE_CONNECTIONS cannot exceed KUBERNETES_MAX_CONNECTIONS")
        return self

    def create_provider(self) -> SandboxProvider:
        """Build a provider that owns a dedicated control-service HTTP client."""
        driver = KubernetesControlClientDriver(
            api_url=str(self.KUBERNETES_API_URL),
            api_token=self.KUBERNETES_API_TOKEN,
            connect_timeout=self.KUBERNETES_CONNECT_TIMEOUT,
            request_timeout=self.KUBERNETES_REQUEST_TIMEOUT,
            stream_read_timeout=self.KUBERNETES_STREAM_READ_TIMEOUT,
            max_connections=self.KUBERNETES_MAX_CONNECTIONS,
            max_keepalive_connections=self.KUBERNETES_MAX_KEEPALIVE_CONNECTIONS,
        )
        return KubernetesSandboxProvider(driver)
