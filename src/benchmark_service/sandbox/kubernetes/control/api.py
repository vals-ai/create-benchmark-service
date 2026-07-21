from __future__ import annotations

from typing import Any, Protocol, cast

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException
from kubernetes_asyncio.config.config_exception import ConfigException

from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings


class KubernetesApiError(Exception):
    """Stable Kubernetes API failure used by the control backend."""

    def __init__(self, status: int, reason: str) -> None:
        self.status = status
        self.reason = reason
        super().__init__(f"Kubernetes API returned {status}: {reason}")


class KubernetesApi(Protocol):
    async def create_job(self, namespace: str, body: dict[str, object]) -> dict[str, object]: ...

    async def get_job(self, namespace: str, name: str) -> dict[str, object] | None: ...

    async def list_jobs(
        self,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
    ) -> dict[str, object]: ...

    async def patch_job(self, namespace: str, name: str, body: dict[str, object]) -> None: ...

    async def delete_job(self, namespace: str, name: str) -> None: ...

    async def list_pods(self, namespace: str, label_selector: str) -> list[dict[str, object]]: ...

    async def create_network_policy(self, namespace: str, body: dict[str, object]) -> None: ...

    async def delete_network_policy(self, namespace: str, name: str) -> None: ...

    async def replace_custom_object(
        self,
        namespace: str,
        plural: str,
        name: str,
        body: dict[str, object],
    ) -> None: ...

    async def delete_custom_object(self, namespace: str, plural: str, name: str) -> None: ...

    async def close(self) -> None: ...


class KubernetesAsyncioApi:
    """Narrow dictionary-based adapter over kubernetes-asyncio."""

    def __init__(self, api_client: client.ApiClient) -> None:
        self._api_client = api_client
        self._batch = client.BatchV1Api(api_client)
        self._core = client.CoreV1Api(api_client)
        self._networking = client.NetworkingV1Api(api_client)
        self._custom = client.CustomObjectsApi(api_client)

    @classmethod
    async def create(cls, settings: KubernetesControlSettings) -> "KubernetesAsyncioApi":
        try:
            config.load_incluster_config()  # pyright: ignore[reportUnknownMemberType]
        except ConfigException:
            if not settings.allow_local_kubeconfig:
                raise
            await config.load_kube_config()  # pyright: ignore[reportUnknownMemberType]
        return cls(client.ApiClient())

    def _dict(self, value: object) -> dict[str, object]:
        serialized = self._api_client.sanitize_for_serialization(value)
        return cast(dict[str, object], serialized)

    def _raise(self, error: ApiException) -> KubernetesApiError:
        return KubernetesApiError(error.status or 0, error.reason or str(error))

    async def create_job(self, namespace: str, body: dict[str, object]) -> dict[str, object]:
        try:
            result = await self._batch.create_namespaced_job(namespace, cast(Any, body))
        except ApiException as error:
            raise self._raise(error) from error
        return self._dict(result)

    async def get_job(self, namespace: str, name: str) -> dict[str, object] | None:
        try:
            result = await self._batch.read_namespaced_job(name, namespace)
        except ApiException as error:
            if error.status == 404:
                return None
            raise self._raise(error) from error
        return self._dict(result)

    async def list_jobs(
        self,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
    ) -> dict[str, object]:
        try:
            result = await self._batch.list_namespaced_job(
                namespace,
                label_selector=label_selector,
                limit=limit,
                _continue=continue_token or "",
            )
        except ApiException as error:
            raise self._raise(error) from error
        return self._dict(result)

    async def patch_job(self, namespace: str, name: str, body: dict[str, object]) -> None:
        try:
            await self._batch.patch_namespaced_job(name, namespace, body)
        except ApiException as error:
            raise self._raise(error) from error

    async def delete_job(self, namespace: str, name: str) -> None:
        try:
            await self._batch.delete_namespaced_job(name, namespace, propagation_policy="Foreground")
        except ApiException as error:
            if error.status != 404:
                raise self._raise(error) from error

    async def list_pods(self, namespace: str, label_selector: str) -> list[dict[str, object]]:
        try:
            result = await self._core.list_namespaced_pod(namespace, label_selector=label_selector)
        except ApiException as error:
            raise self._raise(error) from error
        serialized = self._dict(result)
        return cast(list[dict[str, object]], serialized.get("items", []))

    async def create_network_policy(self, namespace: str, body: dict[str, object]) -> None:
        try:
            await self._networking.create_namespaced_network_policy(namespace, cast(Any, body))
        except ApiException as error:
            if error.status != 409:
                raise self._raise(error) from error
            name = cast(dict[str, Any], body["metadata"])["name"]
            try:
                await self._networking.replace_namespaced_network_policy(name, namespace, cast(Any, body))
            except ApiException as replace_error:
                raise self._raise(replace_error) from replace_error

    async def delete_network_policy(self, namespace: str, name: str) -> None:
        try:
            await self._networking.delete_namespaced_network_policy(name, namespace)
        except ApiException as error:
            if error.status != 404:
                raise self._raise(error) from error

    async def replace_custom_object(
        self,
        namespace: str,
        plural: str,
        name: str,
        body: dict[str, object],
    ) -> None:
        try:
            await self._custom.replace_namespaced_custom_object("cilium.io", "v2", namespace, plural, name, body)
        except ApiException as error:
            if error.status != 404:
                raise self._raise(error) from error
            try:
                await self._custom.create_namespaced_custom_object("cilium.io", "v2", namespace, plural, body)
            except ApiException as create_error:
                raise self._raise(create_error) from create_error

    async def delete_custom_object(self, namespace: str, plural: str, name: str) -> None:
        try:
            await self._custom.delete_namespaced_custom_object("cilium.io", "v2", namespace, plural, name)
        except ApiException as error:
            if error.status != 404:
                raise self._raise(error) from error

    async def close(self) -> None:
        await self._api_client.close()
