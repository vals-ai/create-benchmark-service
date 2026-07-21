from __future__ import annotations

from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, Field

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

    def create_provider(self) -> SandboxProvider:
        driver = KubernetesControlClientDriver(
            api_url=str(self.KUBERNETES_API_URL),
            api_token=self.KUBERNETES_API_TOKEN,
            connect_timeout=self.KUBERNETES_CONNECT_TIMEOUT,
            request_timeout=self.KUBERNETES_REQUEST_TIMEOUT,
        )
        return KubernetesSandboxProvider(driver)
