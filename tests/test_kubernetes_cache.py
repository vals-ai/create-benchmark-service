"""Tests for shared Kubernetes Job and Pod watch state.

Run: uv run pytest tests/test_kubernetes_cache.py
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest

from benchmark_service.sandbox.kubernetes.control.api import KubernetesApiError, ResourceWatchEvent
from benchmark_service.sandbox.kubernetes.control.cache import PodEndpoint, SandboxResourceCache, pending_failure
from benchmark_service.sandbox.types import SandboxError


def _job(name: str, *, failed: bool = False) -> dict[str, object]:
    status: dict[str, object] = {"failed": 1} if failed else {}

    return {
        "metadata": {
            "name": name,
            "resourceVersion": "1",
            "labels": {"app.kubernetes.io/managed-by": "benchmark-sandbox-control"},
        },
        "status": status,
    }


def _pod(name: str, *, ready: bool, unschedulable: bool = False) -> dict[str, object]:
    conditions: list[dict[str, str]] = []
    if ready:
        conditions = [{"type": "Ready", "status": "True"}]
    elif unschedulable:
        conditions = [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}]
    return {
        "metadata": {
            "name": f"{name}-pod",
            "resourceVersion": "2",
            "labels": {"sandbox.vals.ai/id": name},
        },
        "status": {
            "phase": "Running" if ready else "Pending",
            "podIP": "10.0.0.8",
            "conditions": conditions,
        },
    }


class MockWatchApi:
    """Provide paginated snapshots and queue-backed watch streams."""

    def __init__(self) -> None:
        self.jobs = [_job("task-1")]
        self.pods: list[dict[str, object]] = []
        self.job_events: asyncio.Queue[ResourceWatchEvent | KubernetesApiError] = asyncio.Queue()
        self.pod_events: asyncio.Queue[ResourceWatchEvent | KubernetesApiError] = asyncio.Queue()
        self.job_list_calls = 0
        self.pod_list_calls = 0
        self.job_watch_calls = 0
        self.pod_watch_calls = 0
        self.job_relisted = asyncio.Event()
        self.job_list_errors: list[KubernetesApiError] = []

    async def list_jobs(
        self,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
    ) -> dict[str, object]:
        del namespace, label_selector, limit, continue_token
        self.job_list_calls += 1
        if self.job_list_errors:
            raise self.job_list_errors.pop(0)
        if self.job_list_calls > 1:
            self.job_relisted.set()

        return {"items": self.jobs, "metadata": {"resourceVersion": str(self.job_list_calls)}}

    async def list_pods_page(
        self,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
    ) -> dict[str, object]:
        del namespace, label_selector, limit, continue_token
        self.pod_list_calls += 1

        return {"items": self.pods, "metadata": {"resourceVersion": str(self.pod_list_calls)}}

    async def watch_jobs(
        self,
        namespace: str,
        label_selector: str,
        resource_version: str,
    ) -> AsyncGenerator[ResourceWatchEvent, None]:
        del namespace, label_selector, resource_version
        self.job_watch_calls += 1
        while True:
            event = await self.job_events.get()
            if isinstance(event, KubernetesApiError):
                raise event
            yield event

    async def watch_pods(
        self,
        namespace: str,
        label_selector: str,
        resource_version: str,
    ) -> AsyncGenerator[ResourceWatchEvent, None]:
        del namespace, label_selector, resource_version
        self.pod_watch_calls += 1
        while True:
            event = await self.pod_events.get()
            if isinstance(event, KubernetesApiError):
                raise event
            yield event


class TestSandboxResourceCache:
    """Shared readiness and lookup behavior for watched resources."""

    async def test_shares_watches_across_two_thousand_readiness_waiters(self) -> None:
        """Wake a large readiness burst from one Job watch and one Pod watch.

        Test cases:
        - Two thousand waiters share the initial lists and watch connections.
        - A temporary unschedulable state waits for autoscaled capacity.
        - One ready Pod event wakes every waiter with the same snapshot.
        - Ready-Pod lookup uses the watched state.
        """
        api = MockWatchApi()
        api.pods = [_pod("task-1", ready=False, unschedulable=True)]
        assert pending_failure(_job("task-1"), api.pods) is None
        transient_pull: dict[str, object] = {
            "status": {
                "containerStatuses": [
                    {"state": {"waiting": {"reason": "ErrImagePull", "message": "pull QPS exceeded"}}}
                ]
            }
        }
        assert pending_failure(_job("task-1"), [transient_pull]) is None
        cache = SandboxResourceCache("benchmark-sandboxes", api)
        await cache.start()
        waiters = [asyncio.create_task(cache.wait_ready("task-1", timeout=1)) for _ in range(2_000)]

        await api.pod_events.put(("ADDED", _pod("task-1", ready=True)))
        snapshots = await asyncio.gather(*waiters)
        ready_pod_name = await cache.ready_pod_name("task-1")
        endpoint = await cache.ready_pod_endpoint("task-1")
        cache_ready = await cache.ready()

        await cache.close()

        assert all(snapshot[0] == _job("task-1") for snapshot in snapshots)
        assert ready_pod_name == "task-1-pod"
        assert endpoint == PodEndpoint(name="task-1-pod", ip="10.0.0.8")
        assert cache_ready is True
        assert api.job_list_calls == api.pod_list_calls == 1
        assert api.job_watch_calls == api.pod_watch_calls == 1

    async def test_reports_failure_and_relists_expired_watch_history(self) -> None:
        """Keep waiters correct when resources fail or watch history expires.

        Test cases:
        - A failed Job event wakes its readiness waiter with SandboxError.
        - HTTP 410 from a watch triggers a fresh list before reconnecting.
        """
        api = MockWatchApi()
        cache = SandboxResourceCache("benchmark-sandboxes", api)
        await cache.start()
        failed = asyncio.create_task(cache.wait_ready("task-1", timeout=1))

        await api.job_events.put(("MODIFIED", _job("task-1", failed=True)))
        with pytest.raises(SandboxError, match="failed state"):
            await failed

        api.jobs = [_job("task-1")]
        await api.job_events.put(KubernetesApiError(410, "resource version expired"))
        await asyncio.wait_for(api.job_relisted.wait(), timeout=1)
        assert await cache.ready() is True

        await api.job_events.put(KubernetesApiError(403, "forbidden"))
        for _ in range(100):
            if not await cache.ready():
                break
            await asyncio.sleep(0)
        assert await cache.ready() is False
        await cache.close()

        assert api.job_list_calls == 2

    async def test_retries_a_transient_failure_while_relisting(self) -> None:
        """Keep the shared watch alive when its repair list briefly fails.

        Test cases:
        - Expired watch history starts a fresh Job list.
        - A connection failure during that list backs off and retries.
        """
        api = MockWatchApi()
        waits: list[float] = []

        async def record_wait(delay: float) -> None:
            waits.append(delay)

        cache = SandboxResourceCache("benchmark-sandboxes", api, wait=record_wait)
        await cache.start()
        api.job_list_errors.append(KubernetesApiError(0, "connection reset"))

        await api.job_events.put(KubernetesApiError(410, "resource version expired"))
        await asyncio.wait_for(api.job_relisted.wait(), timeout=1)
        await cache.close()

        assert api.job_list_calls == 3
        assert waits == [0.25]
