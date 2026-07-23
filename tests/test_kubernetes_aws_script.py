"""Tests for AWS Kubernetes wrapper safety guards.

Run: uv run pytest tests/test_kubernetes_aws_script.py -q
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).parents[1]
SCRIPT_PATH = REPOSITORY_ROOT / "infra/kubernetes/aws/kubernetes-aws"
MOCK_COMMAND_PATH = Path(__file__).parent / "fixtures/kubernetes_aws_mock_command.py"
MOCK_COMMAND_NAMES = (
    "aws",
    "curl",
    "docker",
    "git",
    "kubectl",
    "openssl",
    "terraform",
    "uv",
)


def test_aws_wrapper_preserves_identity_and_destroy_guards(tmp_path: Path) -> None:
    """Keep the account boundary and failed-destroy recovery state intact.

    Test cases:
    - A non-vals-dev profile and wrong account stop before Terraform.
    - A failed workload destroy preserves state for a retry.
    """
    executable_dir = tmp_path / "bin"
    executable_dir.mkdir()
    command_log_path = tmp_path / "commands.log"
    command_log_path.touch()

    for command_name in MOCK_COMMAND_NAMES:
        command_path = executable_dir / command_name
        shutil.copy(MOCK_COMMAND_PATH, command_path)
        command_path.chmod(0o755)

    deployment_name = "aws-safety-test"
    state_dir = REPOSITORY_ROOT / f"infra/kubernetes/aws/.state/{deployment_name}"
    runtime_dir = REPOSITORY_ROOT / f"infra/kubernetes/aws/.runtime/{deployment_name}"
    base_environment = {
        **os.environ,
        "AWS_ACCOUNT_ID": "123456789012",
        "AWS_OPERATOR_CIDR": "203.0.113.10/32",
        "AWS_PROFILE": "vals-dev",
        "KUBERNETES_DEPLOYMENT_NAME": deployment_name,
        "MOCK_COMMAND_LOG": str(command_log_path),
        "PATH": f"{executable_dir}{os.pathsep}{os.defpath}",
        "TEST_KUBERNETES_IMAGE": f"123456789012.dkr.ecr.us-east-2.amazonaws.com/benchmark@sha256:{'b' * 64}",
    }

    def run_wrapper(command: str, environment_updates: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        environment = {**base_environment, **(environment_updates or {})}

        return subprocess.run(
            ["/bin/bash", str(SCRIPT_PATH), command],
            cwd=REPOSITORY_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    shutil.rmtree(state_dir, ignore_errors=True)
    shutil.rmtree(runtime_dir, ignore_errors=True)

    try:
        profile_result = run_wrapper("plan", {"AWS_PROFILE": "production"})

        assert profile_result.returncode != 0
        assert "AWS_PROFILE must be vals-dev" in profile_result.stderr
        assert command_log_path.read_text() == ""

        account_result = run_wrapper("plan", {"MOCK_AWS_ACCOUNT": "210987654321"})

        assert account_result.returncode != 0
        assert "AWS account mismatch: expected 123456789012, got 210987654321" in account_result.stderr

        account_commands = command_log_path.read_text().splitlines()
        assert any("aws sts get-caller-identity" in command for command in account_commands)
        assert not any(command.startswith("terraform ") for command in account_commands)

        plan_result = run_wrapper("plan")
        deploy_result = run_wrapper("deploy")
        destroy_result = run_wrapper("destroy", {"MOCK_WORKLOAD_DESTROY_FAIL": "1"})

        assert plan_result.returncode == 0, plan_result.stderr
        assert deploy_result.returncode == 0, deploy_result.stderr
        assert destroy_result.returncode != 0
        assert "workload destroy failed" in destroy_result.stderr

        destroy_commands = command_log_path.read_text().splitlines()
        assert any(
            command.startswith("terraform ") and "destroy" in command and "workload.tfstate" in command
            for command in destroy_commands
        )
        assert not any(
            command.startswith("terraform ") and "destroy" in command and "foundation.tfstate" in command
            for command in destroy_commands
        )
        assert state_dir.exists()
        assert runtime_dir.exists()
    finally:
        shutil.rmtree(state_dir, ignore_errors=True)
        shutil.rmtree(runtime_dir, ignore_errors=True)
