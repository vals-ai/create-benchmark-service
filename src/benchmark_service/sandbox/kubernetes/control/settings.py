"""Define deployment-owned settings for the Kubernetes control service."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class KubernetesControlSettings(BaseModel):
    """Deployment-owned settings for the in-cluster control service."""

    namespace: str = "benchmark-sandboxes"
    api_token: str = Field(min_length=1, repr=False)
    runtime_class_name: str = "kata-qemu"
    sandbox_container_name: str = "sandbox"
    agent_image: str | None = None
    agent_port: int = Field(default=8787, gt=0, le=65535)
    agent_connect_timeout_seconds: float = Field(default=5, gt=0)
    agent_heartbeat_seconds: float = Field(default=10, gt=0)
    docker_image: str
    docker_enabled: bool = True
    hard_lifetime_seconds: int = Field(default=86400, gt=0)
    finished_ttl_seconds: int = Field(default=300, ge=0)
    exec_output_limit_bytes: int = Field(default=16 * 1024 * 1024, gt=0)
    upload_limit_bytes: int = Field(default=8 * 1024 * 1024 * 1024, gt=0)
    activity_write_interval_seconds: float = Field(default=30, gt=0)
    command_heartbeat_seconds: float = Field(default=15, gt=0)
    exec_connection_pool_size: int = Field(default=512, gt=0)
    max_create_timeout_seconds: int = Field(default=900, gt=0)
    max_vcpu: int = Field(default=64, gt=0)
    max_memory_gib: int = Field(default=256, gt=0)
    max_disk_gib: int = Field(default=1024, gt=0)
    max_gpu: int = Field(default=8, ge=0)
    gpu_resource_name: str = "nvidia.com/gpu"
    gpu_type_label: str = "sandbox.vals.ai/gpu-type"
    sandbox_node_selector: dict[str, str] = Field(default_factory=dict)
    sandbox_pod_annotations: dict[str, str] = Field(default_factory=dict)
    egress_driver: str = "cilium"
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
        "egress_driver",
    )
    @classmethod
    def require_nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("agent_image")
    @classmethod
    def require_nonempty_agent_image(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("must not be empty")
        return value
