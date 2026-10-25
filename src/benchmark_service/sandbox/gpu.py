"""Names for GPU types, and the way each provider's own names are found among them."""

import re
from collections.abc import Sequence

from daytona import GpuType

from benchmark_service.sandbox.types import SandboxError

_ALIASES = {
    "h100-80gb": "h100",
    "h200-141gb": "h200",
    "rtx-pro6000": "rtx-pro-6000",
    "rtx-6000-pro": "rtx-pro-6000",
    "rtx-pro-6000-blackwell": "rtx-pro-6000",
    "rtx4090": "rtx-4090",
    "4090": "rtx-4090",
    "rtx5090": "rtx-5090",
    "5090": "rtx-5090",
}
_DAYTONA = {t.value.lower(): t for t in GpuType if t is not GpuType.UNKNOWN_DEFAULT_OPEN_API}
_MODAL = frozenset(
    {
        "T4",
        "L4",
        "A10",
        "L40S",
        "A100",
        "A100-40GB",
        "A100-80GB",
        "RTX-PRO-6000",
        "H100",
        "H100!",
        "H200",
        "B200",
        "B200+",
        "B300",
    }
)


def normalize_gpu_name(name: str) -> str:
    key = re.sub(r"^(nvidia-)?(geforce-|tesla-)?", "", re.sub(r"[\s_-]+", "-", name.strip().lower()))
    return _ALIASES.get(key, key)


def daytona_gpu_type(names: Sequence[str]) -> GpuType | list[GpuType] | None:
    found = list(dict.fromkeys(t for name in names if (t := _DAYTONA.get(normalize_gpu_name(name))) is not None))
    if names and not found:
        supported = ", ".join(t.value for t in _DAYTONA.values())
        raise SandboxError(f"Unsupported Daytona GPU type: {', '.join(names)}. Supported types: {supported}")
    if len(found) > 1:
        return found
    return found[0] if found else None


def modal_gpu_type(names: Sequence[str]) -> str | None:
    candidates = [normalize_gpu_name(name).upper() for name in names]
    return next((name for name in candidates if name in _MODAL), candidates[0] if candidates else None)
