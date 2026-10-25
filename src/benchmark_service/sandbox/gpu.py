"""Names for GPU types, and the way each provider's own names are found among them."""

import re
from collections.abc import Mapping, Sequence

from benchmark_service.sandbox.types import SandboxError


def normalize_gpu_name(name: str) -> str:
    key = re.sub(r"[\s_]+", "-", name.strip().lower())
    return re.sub(r"^(nvidia-)?(geforce-)?", "", key)


def _names(native_names: Mapping[str, Sequence[str]]) -> dict[str, str]:
    return {alias: native for native, aliases in native_names.items() for alias in (native.lower(), *aliases)}


DAYTONA_GPU_TYPES = _names(
    {
        "H100": ("h100-80gb",),
        "H200": ("h200-141gb",),
        "RTX-PRO-6000": ("rtx-pro6000", "rtx-6000-pro", "rtx-pro-6000-blackwell"),
        "RTX-4090": ("rtx4090", "4090"),
        "RTX-5090": ("rtx5090", "5090"),
    }
)
MODAL_GPU_TYPES = _names(
    {
        "T4": (),
        "L4": (),
        "L40S": (),
        "H100": ("h100-80gb",),
        "H200": ("h200-141gb",),
        "B200": (),
        "A100-40GB": ("a100",),
        "A100-80GB": (),
    }
)


def daytona_gpu_type(names: Sequence[str]) -> str | None:
    if not names:
        return None
    for name in names:
        if (native := DAYTONA_GPU_TYPES.get(normalize_gpu_name(name))) is not None:
            return native
    supported = ", ".join(sorted(set(DAYTONA_GPU_TYPES.values())))
    plural = "s" if len(names) > 1 else ""
    raise SandboxError(f"Unsupported Daytona GPU type{plural}: {', '.join(names)}. Supported types: {supported}")


def modal_gpu_type(names: Sequence[str]) -> str | None:
    if not names:
        return None
    return MODAL_GPU_TYPES.get(normalize_gpu_name(names[0]), names[0])
