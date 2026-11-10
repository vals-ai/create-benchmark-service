"""Names for GPU types, and the way each provider's own names are found among them."""

import re
from collections.abc import Mapping, Sequence

from daytona import GpuType

from benchmark_service.sandbox.types import SandboxError

# Spellings no uniform rewrite reaches: word order, generation suffixes, and bare GeForce numbers.
_ALIASES = {
    "rtx-6000-pro": "rtx-pro-6000",
    "rtx-pro-6000-blackwell": "rtx-pro-6000",
    "4090": "rtx-4090",
    "5090": "rtx-5090",
}
# Modal's SDK carries no list of its GPU names; these are the ones its docs give.
_MODAL_NAMES = (
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
)
_DAYTONA_TYPES = [t for t in GpuType if t is not GpuType.UNKNOWN_DEFAULT_OPEN_API]


def normalize_gpu_name(name: str) -> str:
    key = re.sub(r"[\s_-]+", "-", name.strip().lower())
    key = re.sub(r"^(nvidia-)?(geforce-|tesla-)?", "", key)
    key = re.sub(r"(rtx|pro)-?(?=\d)", r"\1-", key)
    return _ALIASES.get(key, key)


def _offered[T](names: Sequence[str], catalog: Mapping[str, T]) -> list[T]:
    """The provider's types among the names, in order. A memory size counts only where the provider sells it apart."""
    found: list[T] = []
    for name in names:
        key = normalize_gpu_name(name)
        match = catalog.get(key) or catalog.get(re.sub(r"-\d+gb$", "", key))
        if match is not None and match not in found:
            found.append(match)
    return found


def daytona_gpu_type(names: Sequence[str]) -> GpuType | list[GpuType] | None:
    found = _offered(names, {normalize_gpu_name(t.value): t for t in _DAYTONA_TYPES})
    if names and not found:
        supported = ", ".join(t.value for t in _DAYTONA_TYPES)
        raise SandboxError(f"Unsupported Daytona GPU type: {', '.join(names)}. Supported types: {supported}")
    return found if len(found) > 1 else next(iter(found), None)


def modal_gpu_type(names: Sequence[str]) -> str | None:
    """The first type Modal names, else the first given in capitals so types newer than this list keep working."""
    found = _offered(names, {normalize_gpu_name(name): name for name in _MODAL_NAMES})
    return next(iter(found), None) or next((key.upper() for name in names if (key := normalize_gpu_name(name))), None)
