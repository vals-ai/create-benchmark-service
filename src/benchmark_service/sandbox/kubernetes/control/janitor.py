"""Run the one-shot cleanup entry point for expired Kubernetes sandboxes."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from datetime import UTC, datetime

from benchmark_service.sandbox.kubernetes.control.api import KubernetesAsyncioApi
from benchmark_service.sandbox.kubernetes.control.egress import select_egress_driver
from benchmark_service.sandbox.kubernetes.control.kubernetes import KubernetesSandboxBackend
from benchmark_service.sandbox.kubernetes.control.main import load_settings


async def run_janitor_once(environ: Mapping[str, str]) -> int:
    """Delete expired sandboxes once and close the Kubernetes client."""
    settings = load_settings(environ)
    egress_factory = select_egress_driver(settings)
    api = await KubernetesAsyncioApi.create(settings)
    egress = egress_factory(api)
    backend = KubernetesSandboxBackend(settings, api, egress_driver=egress)
    try:
        return await backend.delete_idle_sandboxes(datetime.now(UTC))
    finally:
        await backend.close()


def main() -> None:
    """Run one Kubernetes sandbox cleanup pass."""
    asyncio.run(run_janitor_once(os.environ))
