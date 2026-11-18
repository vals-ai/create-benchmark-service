"""Build Lambda MicroVM images that run the sandbox agent.

Lambda builds an image by running a Dockerfile from a zip in S3 on top of its Amazon Linux base
image, then snapshots the started agent. Image names hash their inputs, so an unchanged
Dockerfile and context reuse the image already built.
"""

from __future__ import annotations

import hashlib
import importlib
import io
import time
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from benchmark_service.sandbox.types import SandboxBuildError, SandboxError

AGENT_PATH = Path(__file__).with_name("agent.py")
AGENT_FILE = "cbs_microvm_agent.py"
BASE_IMAGE_NAME = "al2023-1"
DEFAULT_BASE_DOCKERFILE = """FROM public.ecr.aws/amazonlinux/amazonlinux:2023
RUN dnf install -y python3 tar gzip git findutils procps-ng && dnf clean all
WORKDIR /workspace
"""
# Baseline memory sizes Lambda offers; each scales to 4x memory and vCPU at peak (2 GiB = 1 vCPU).
BASELINE_MEMORY_MIB = (512, 1024, 2048, 4096, 8192)
PEAK_MULTIPLIER = 4
MIB_PER_VCPU = 2048
IMAGE_TAGS = {"Purpose": "microvm-sandbox-mvp", "ManagedBy": "create-benchmark-service"}
_POLL_SECONDS = 10.0


def network_connector(region: str, kind: str) -> str:
    return f"arn:aws:lambda:{region}:aws:network-connector:aws-network-connector:{kind}"


def baseline_memory_mib(vcpu: float, memory_gib: float) -> int:
    """Smallest baseline whose 4x peak covers the requested vCPU and memory."""
    for baseline in BASELINE_MEMORY_MIB:
        peak = baseline * PEAK_MULTIPLIER
        if peak >= memory_gib * 1024 and peak / MIB_PER_VCPU >= vcpu:
            return baseline
    raise SandboxError(f"Lambda MicroVMs cannot fit {vcpu} vCPU / {memory_gib} GiB (max peak 16 vCPU / 32 GiB)")


# Images without Python get it from whichever package manager they have; the agent needs python3.
_ENSURE_PYTHON = (
    "RUN command -v python3 >/dev/null 2>&1 || "
    "(apt-get update && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*) || "
    "dnf install -y python3 || yum install -y python3 || apk add --no-cache python3 || "
    '(echo "cbs: the MicroVM sandbox agent needs python3 in the image" >&2; exit 1)'
)


def image_dockerfile(dockerfile: str) -> str:
    """The user's Dockerfile with the agent as the only startup process, replacing any inherited ENTRYPOINT."""
    return (
        dockerfile.rstrip("\n")
        + f"\n{_ENSURE_PYTHON}\n"
        + f"COPY {AGENT_FILE} /opt/cbs/{AGENT_FILE}\n"
        + "ENTRYPOINT []\n"
        + f'CMD ["python3", "/opt/cbs/{AGENT_FILE}"]\n'
    )


def _context_files(context_dir: Path) -> dict[str, tuple[bytes, int]]:
    """Regular files under the context with their permission bits; symlinks leaving the context are skipped."""
    root = context_dir.resolve()
    files: dict[str, tuple[bytes, int]] = {}
    for path in sorted(context_dir.rglob("*")):
        if not path.is_file() or not path.resolve().is_relative_to(root):
            continue
        files[path.relative_to(context_dir).as_posix()] = (path.read_bytes(), path.stat().st_mode & 0o777)
    return files


def build_artifact(dockerfile: str, context_dir: Path | None = None) -> bytes:
    """Zip the build context, the wrapped Dockerfile, and the agent, with stable timestamps and file modes."""
    files = _context_files(context_dir) if context_dir is not None else {}
    files["Dockerfile"] = (image_dockerfile(dockerfile).encode(), 0o644)
    files[AGENT_FILE] = (AGENT_PATH.read_bytes(), 0o644)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(files):
            content, mode = files[name]
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.external_attr = (0o100000 | mode) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, content)
    return buffer.getvalue()


def image_name(artifact: bytes, memory_mib: int, architecture: str) -> str:
    digest = hashlib.sha256(artifact + f"{memory_mib}:{architecture}".encode()).hexdigest()[:20]
    return f"cbs-{digest}"


def aws_error_code(error: Exception) -> str | None:
    response: Any = getattr(error, "response", None)
    try:
        return str(response["Error"]["Code"])
    except (KeyError, TypeError):
        return None


def aws_client(service: str, region: str) -> Any:
    return importlib.import_module("boto3").client(service, region_name=region)


def _not_found(error: Exception) -> bool:
    return aws_error_code(error) in ("ResourceNotFoundException", "NotFoundException")


def ensure_image(
    microvms: Any,
    s3: Any,
    *,
    dockerfile: str,
    context_dir: Path | None,
    memory_mib: int,
    bucket: str,
    build_role_arn: str,
    architecture: str = "ARM_64",
    timeout: float = 1800.0,
    log: Callable[[str], None] = lambda _message: None,
) -> str:
    """Return the ARN of a built image for this Dockerfile, building it when absent.

    Blocking; providers call it from a worker thread.
    """
    region = microvms.meta.region_name
    artifact = build_artifact(dockerfile, context_dir)
    name = image_name(artifact, memory_mib, architecture)
    try:
        image = microvms.get_microvm_image(imageIdentifier=name)
    except Exception as error:
        if not _not_found(error):
            raise
        key = f"cbs-microvm-images/{name}.zip"
        s3.put_object(Bucket=bucket, Key=key, Body=artifact)
        log(f"Building MicroVM image {name}")
        image = microvms.create_microvm_image(
            name=name,
            baseImageArn=f"arn:aws:lambda:{region}:aws:microvm-image:{BASE_IMAGE_NAME}",
            buildRoleArn=build_role_arn,
            codeArtifact={"uri": f"s3://{bucket}/{key}"},
            egressNetworkConnectors=[network_connector(region, "INTERNET_EGRESS")],
            cpuConfigurations=[{"architecture": architecture}],
            resources=[{"minimumMemoryInMiB": memory_mib}],
            hooks={
                "port": 9000,
                "microvmHooks": {"run": "ENABLED", "resume": "ENABLED", "suspend": "ENABLED", "terminate": "ENABLED"},
                "microvmImageHooks": {"ready": "ENABLED", "validate": "ENABLED"},
            },
            tags=IMAGE_TAGS,
        )
    deadline = time.monotonic() + timeout
    while True:
        image = microvms.get_microvm_image(imageIdentifier=name)
        state = image["state"]
        if image.get("latestActiveImageVersion") and state in ("CREATED", "UPDATED"):
            return str(image["imageArn"])
        if state in ("CREATE_FAILED", "UPDATE_FAILED"):
            raise SandboxBuildError(f"MicroVM image {name} failed to build: {_failure_reason(microvms, image)}")
        if time.monotonic() > deadline:
            raise SandboxBuildError(f"MicroVM image {name} did not finish building within {timeout:.0f}s")
        time.sleep(_POLL_SECONDS)


def _failure_reason(microvms: Any, image: dict[str, Any]) -> str:
    version = image.get("latestFailedImageVersion")
    if not version:
        return str(image["state"])
    detail = microvms.get_microvm_image_version(imageIdentifier=image["imageArn"], imageVersion=version)
    return str(detail.get("stateReason") or detail["state"])
