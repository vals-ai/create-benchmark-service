"""Benchmark hooks for the Vals hosted service template."""

from typing import Any

from benchmark_service.auth import UNAUTHENTICATED_TENANT_SENTINEL
from benchmark_service.base import BenchmarkService as CoreBenchmarkService

from .auth import check_benchmark_service_auth, get_tenant_config, resolve_caller_tenant


class BenchmarkService(CoreBenchmarkService):
    """Apply Vals tenant authentication and dataset access rules."""

    async def check_auth(self, headers: dict[str, str]) -> bool:
        return await check_benchmark_service_auth(headers)

    async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
        if type(self).check_auth is not BenchmarkService.check_auth:
            ok = await self.check_auth(headers)
            return UNAUTHENTICATED_TENANT_SENTINEL if ok else None
        return await resolve_caller_tenant(headers)

    async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
        if tenant == UNAUTHENTICATED_TENANT_SENTINEL:
            return True
        entry = get_tenant_config(tenant)
        return entry is not None and (dataset or "default") in entry.datasets

    def project_trial_result(self, result: Any) -> Any:
        """Trial-safe projection of a per-task eval result.

        For `trial_mode` tenants, /v1/evaluate responses are reduced to what this
        returns, and that projection is all a trial caller can resubmit to
        /v1/score. So it must include both the score fields a prospect may see
        AND any field `calculate_final_score` needs to aggregate -- anything
        dropped here is gone from the final score too.

        Like `list_tasks`, the default raises so trial mode requires an explicit,
        audited projection rather than leaking rubric / judge data by omission.

        Raises:
            NotImplementedError: if the benchmark has not opted into trial mode.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.project_trial_result must be implemented for trial_mode tenants"
        )
