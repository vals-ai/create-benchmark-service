"""Names for GPU types, and the way each provider's own names are found among them."""

import re
from collections.abc import Sequence
from enum import Enum

from daytona import GpuType

from benchmark_service.sandbox.types import SandboxError


class GpuModel(Enum):
    """A GPU type: its key, other spellings of it, and its name on each provider that offers it."""

    T4 = ("t4", (), None, "T4")
    L4 = ("l4", (), None, "L4")
    A10 = ("a10", (), None, "A10")
    L40S = ("l40s", (), None, "L40S")
    A100 = ("a100", (), None, "A100")
    A100_40GB = ("a100-40gb", (), None, "A100-40GB")
    A100_80GB = ("a100-80gb", (), None, "A100-80GB")
    H100 = ("h100", ("h100-80gb",), GpuType.H100, "H100")
    H100_ONLY = ("h100!", (), None, "H100!")
    H200 = ("h200", ("h200-141gb",), GpuType.H200, "H200")
    B200 = ("b200", (), None, "B200")
    B200_PLUS = ("b200+", (), None, "B200+")
    B300 = ("b300", (), None, "B300")
    RTX_PRO_6000 = (
        "rtx-pro-6000",
        ("rtx-pro6000", "rtx-6000-pro", "rtx-pro-6000-blackwell"),
        GpuType.RTX_PRO_6000,
        "RTX-PRO-6000",
    )
    RTX_4090 = ("rtx-4090", ("rtx4090", "4090"), GpuType.RTX_4090, None)
    RTX_5090 = ("rtx-5090", ("rtx5090", "5090"), GpuType.RTX_5090, None)

    def __init__(self, key: str, aliases: tuple[str, ...], daytona: GpuType | None, modal: str | None) -> None:
        self.key: str = key
        self.aliases: tuple[str, ...] = aliases
        self.daytona: GpuType | None = daytona
        self.modal: str | None = modal


_BY_KEY = {key: model for model in GpuModel for key in (model.key, *model.aliases)}


def normalize_gpu_name(name: str) -> str:
    key = re.sub(r"^(nvidia-)?(geforce-|tesla-)?", "", re.sub(r"[\s_-]+", "-", name.strip().lower()))
    model = _BY_KEY.get(key)
    return model.key if model else key


def gpu_models(names: Sequence[str]) -> list[GpuModel]:
    """The GPU types the names give, in order, ignoring blanks. Unknown names are refused."""
    keys = {name: key for name in names if (key := normalize_gpu_name(name))}
    if unknown := [name for name, key in keys.items() if key not in _BY_KEY]:
        known = ", ".join(model.key for model in GpuModel)
        raise SandboxError(f"Unknown GPU type: {', '.join(unknown)}. Known types: {known}")
    return list(dict.fromkeys(_BY_KEY[key] for key in keys.values()))


def daytona_gpu_type(names: Sequence[str]) -> GpuType | list[GpuType] | None:
    found = [model.daytona for model in gpu_models(names) if model.daytona]
    if names and not found:
        supported = ", ".join(model.daytona.value for model in GpuModel if model.daytona)
        raise SandboxError(f"Unsupported Daytona GPU type: {', '.join(names)}. Supported types: {supported}")
    return found if len(found) > 1 else next(iter(found), None)


def modal_gpu_type(names: Sequence[str]) -> str | None:
    models = gpu_models(names)
    found = next((model.modal for model in models if model.modal), None)
    if models and not found:
        supported = ", ".join(model.modal for model in GpuModel if model.modal)
        raise SandboxError(f"Unsupported Modal GPU type: {', '.join(names)}. Supported types: {supported}")
    return found
