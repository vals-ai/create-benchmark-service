from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class KubernetesControlSettings(BaseModel):
    """Deployment-owned settings for the in-cluster control service."""

    namespace: str = "benchmark-sandboxes"
    api_token: str = Field(min_length=1, repr=False)
    runtime_class_name: str = "kata-qemu"
    sandbox_container_name: str = "sandbox"
    docker_image: str
    docker_enabled: bool = True
    hard_lifetime_seconds: int = Field(default=86400, gt=0)
    finished_ttl_seconds: int = Field(default=300, ge=0)
    exec_output_limit_bytes: int = Field(default=16 * 1024 * 1024, gt=0)
    janitor_interval_seconds: int = Field(default=60, gt=0)
    gpu_resource_name: str = "nvidia.com/gpu"
    gpu_type_label: str = "sandbox.vals.ai/gpu-type"
    allowed_image_prefixes: tuple[str, ...] = ()
    require_image_digest: bool = False
    allow_local_kubeconfig: bool = False

    @field_validator(
        "namespace",
        "runtime_class_name",
        "sandbox_container_name",
        "docker_image",
        "gpu_resource_name",
        "gpu_type_label",
    )
    @classmethod
    def require_nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value
