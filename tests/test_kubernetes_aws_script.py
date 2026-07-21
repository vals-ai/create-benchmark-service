"""Tests for the disposable AWS Kubernetes orchestration script.

Run: uv run pytest tests/test_kubernetes_aws_script.py -q
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).parents[1]
SCRIPT_PATH = REPOSITORY_ROOT / "infra/kubernetes/aws/kubernetes-aws"
FOUNDATION_MAIN_PATH = REPOSITORY_ROOT / "infra/kubernetes/aws/foundation/main.tf"
FOUNDATION_VARIABLES_PATH = REPOSITORY_ROOT / "infra/kubernetes/aws/foundation/variables.tf"


def test_kubernetes_aws_orchestration(tmp_path: Path) -> None:
    """Verify preflight rejection, operation ordering, and failure preservation.

    Test cases:
    - Missing or invalid AWS identity inputs stop before Terraform changes.
    - Deploy applies foundation before publishing images and workload readiness.
    - A failed live test preserves infrastructure, while destroy cleans up last.
    """
    executable_dir = tmp_path / "bin"
    executable_dir.mkdir()
    record_path = tmp_path / "commands.log"
    shim = """#!/bin/bash
set -eu
command_name="${0##*/}"
printf '%s %s\\n' "$command_name" "$*" >> "$RECORD_PATH"

case "$command_name" in
  aws)
    case "$*" in
      *"sts get-caller-identity"*)
        printf '%s\\t%s\\n' "${SHIM_ACCOUNT:-123456789012}" "${SHIM_ARN:-arn:aws:iam::123456789012:user/tester}"
        ;;
      *"ecr get-login-password"*)
        if [[ "${SHIM_ECR_LOGIN_FAIL:-0}" == "1" ]]; then
          exit 1
        fi
        printf 'password\\n'
        ;;
      *"ecr describe-images"*)
        image_tag=""
        for argument in "$@"; do
          case "$argument" in
            imageTag=*) image_tag="${argument#imageTag=}" ;;
          esac
        done
        if ! grep -F "docker push 123456789012.dkr.ecr.us-east-2.amazonaws.com/test/sandbox-images:${image_tag}" "$RECORD_PATH" >/dev/null; then
          exit 255
        fi
        case "$image_tag" in
          control-*) printf 'sha256:%064d\\n' 1 ;;
          dind-28.3.3) printf 'sha256:%064d\\n' 2 ;;
        esac
        ;;
      *"resourcegroupstaggingapi get-resources"*)
        printf '%s\\n' "${SHIM_TAGGED_ARNS:-}"
        ;;
    esac
    ;;
  terraform)
    state_path=""
    for argument in "$@"; do
      case "$argument" in
        -out=*) : > "${argument#-out=}" ;;
        -state=*) state_path="${argument#-state=}" ;;
      esac
    done
    if [[ "$*" == *" apply "* && -n "$state_path" ]]; then
      : > "$state_path"
    fi
    if [[ "${SHIM_FOUNDATION_APPLY_FAIL:-0}" == "1" && "$*" == *" apply "*"foundation.tfstate"* ]]; then
      exit 1
    fi
    if [[ "$*" == *"workload.tfplan"* && "$*" == *" plan "* ]]; then
      [[ "${TF_VAR_api_token:-}" == "0000000000000000000000000000000000000000000000000000000000000007" ]]
      printf 'terraform-token-env plan\\n' >> "$RECORD_PATH"
    fi
    if [[ "$*" == *"workload.tfplan"* && "$*" == *" apply "* ]]; then
      [[ "${TF_VAR_api_token:-}" == "0000000000000000000000000000000000000000000000000000000000000007" ]]
      printf 'terraform-token-env apply\\n' >> "$RECORD_PATH"
    fi
    case "$*" in
      *"output"*"cluster_name"*) printf 'test-eks\\n' ;;
      *"output"*"image_repository_url"*) printf '123456789012.dkr.ecr.us-east-2.amazonaws.com/test/sandbox-images\\n' ;;
    esac
    if [[ "${SHIM_WORKLOAD_DESTROY_FAIL:-0}" == "1" && "$*" == *"destroy"*"workload.tfstate"* ]]; then
      exit 1
    fi
    if [[ "$*" == *" destroy "*"foundation.tfstate"* ]]; then
      [[ "${TF_VAR_aws_account_id:-}" == "123456789012" ]]
      printf 'terraform-account-env destroy\\n' >> "$RECORD_PATH"
    fi
    ;;
  openssl)
    printf '%064d\\n' 7
    ;;
  git)
    printf '0123456789ab\\n'
    ;;
  kubectl)
    if [[ "$*" == *"port-forward service/kubernetes-sandbox-control"* ]]; then
      if [[ "${SHIM_PORT_FORWARD_DEAD:-0}" == "1" ]]; then
        exit 1
      fi
      while true; do sleep 1; done
    fi
    if [[ "$*" == *"get namespace benchmark-sandboxes"* ]]; then
      if [[ "${SHIM_NAMESPACE_FAILURE:-0}" == "1" ]]; then
        exit 1
      fi
      printf '%s\\n' "${SHIM_NAMESPACE_RESULT:-}"
    fi
    ;;
  uv)
    if [[ "${1:-}" == "run" && "${2:-}" == "python" ]]; then
      shift 2
      exec "$REAL_PYTHON" "$@"
    fi
    if [[ "${SHIM_UV_FAIL:-0}" == "1" && "$*" == *"pytest"* ]]; then
      exit 1
    fi
    ;;
  curl)
    if [[ "${SHIM_PORT_FORWARD_DEAD:-0}" == "1" ]]; then
      sleep 0.1
    fi
    printf '{"status":"ok"}\\n'
    ;;
  rm)
    /bin/rm "$@"
    ;;
esac
"""
    for command_name in ("aws", "curl", "docker", "git", "kubectl", "openssl", "rm", "terraform", "uv"):
        command_path = executable_dir / command_name
        command_path.write_text(shim)
        command_path.chmod(0o755)

    base_env = {
        **os.environ,
        "AWS_ACCOUNT_ID": "123456789012",
        "AWS_OPERATOR_CIDR": "203.0.113.10/32",
        "AWS_PROFILE": "vals-dev",
        "KUBERNETES_DEPLOYMENT_NAME": "test-orchestration",
        "PATH": f"{executable_dir}{os.pathsep}{os.defpath}",
        "RECORD_PATH": str(record_path),
        "REAL_PYTHON": sys.executable,
        "TEST_KUBERNETES_IMAGE": (
            "123456789012.dkr.ecr.us-east-2.amazonaws.com/benchmark@"
            f"sha256:{'b' * 64}"
        ),
    }

    def run(command: str, env_updates: Mapping[str, str | None] | None = None) -> subprocess.CompletedProcess[str]:
        environment = base_env.copy()
        for name, value in (env_updates or {}).items():
            if value is None:
                environment.pop(name, None)
            else:
                environment[name] = value

        return subprocess.run(
            ["/bin/bash", str(SCRIPT_PATH), command],
            cwd=REPOSITORY_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    preflight_cases = (
        ({"AWS_ACCOUNT_ID": None}, "AWS_ACCOUNT_ID"),
        ({"AWS_OPERATOR_CIDR": None}, "AWS_OPERATOR_CIDR"),
        ({"AWS_PROFILE": None}, "AWS_PROFILE"),
        ({"AWS_PROFILE": "production"}, "vals-dev"),
        ({"SHIM_ARN": "arn:aws-us-gov:iam::123456789012:user/tester"}, "commercial aws partition"),
        ({"SHIM_ACCOUNT": "210987654321"}, "expected 123456789012, got 210987654321"),
        ({"AWS_OPERATOR_CIDR": "10.0.0.0/16"}, "no broader than /24"),
        ({"KUBERNETES_DEPLOYMENT_NAME": "../escape"}, "lowercase DNS label"),
    )

    state_dir = REPOSITORY_ROOT / "infra/kubernetes/aws/.state/test-orchestration"
    runtime_dir = REPOSITORY_ROOT / "infra/kubernetes/aws/.runtime/test-orchestration"
    try:
        foundation_variables = FOUNDATION_VARIABLES_PATH.read_text()
        foundation_main = FOUNDATION_MAIN_PATH.read_text()
        assert 'variable "aws_account_id"' in foundation_variables
        assert "allowed_account_ids = [var.aws_account_id]" in foundation_main

        for env_updates, expected_error in preflight_cases:
            record_path.write_text("")

            result = run("plan", env_updates)

            assert result.returncode != 0
            assert expected_error in result.stderr
            commands = record_path.read_text()
            if "SHIM_ARN" not in env_updates and "SHIM_ACCOUNT" not in env_updates:
                assert "aws " not in commands
            assert "terraform " not in commands

        record_path.write_text("")

        plan_result = run("plan")
        plan_commands = record_path.read_text().splitlines()
        assert any("-var=aws_account_id=123456789012" in command for command in plan_commands)

        interrupted_apply_result = run("deploy", {"SHIM_FOUNDATION_APPLY_FAIL": "1"})

        assert plan_result.returncode == 0, plan_result.stderr
        assert interrupted_apply_result.returncode != 0
        foundation_runtime = (runtime_dir / "deployment.env").read_text()
        assert "runtime_phase=foundation" in foundation_runtime
        assert "cluster_name=test-orchestration-eks" in foundation_runtime
        assert (
            "image_repository_url=123456789012.dkr.ecr.us-east-2.amazonaws.com/"
            "test-orchestration/sandbox-images"
        ) in foundation_runtime
        assert (runtime_dir / "deployment.env").stat().st_mode & 0o777 == 0o600

        before_apply_recovery = len(record_path.read_text().splitlines())

        apply_recovery_result = run("destroy")

        assert apply_recovery_result.returncode == 0, apply_recovery_result.stderr
        apply_recovery_commands = record_path.read_text().splitlines()[before_apply_recovery:]
        assert any("destroy" in command and "foundation.tfstate" in command for command in apply_recovery_commands)
        assert "terraform-account-env destroy" in apply_recovery_commands
        assert not runtime_dir.exists()
        assert not state_dir.exists()

        record_path.write_text("")

        plan_result = run("plan")
        interrupted_deploy_result = run("deploy", {"SHIM_ECR_LOGIN_FAIL": "1"})

        assert plan_result.returncode == 0, plan_result.stderr
        assert interrupted_deploy_result.returncode != 0
        assert runtime_dir.exists()
        assert (runtime_dir / "deployment.env").stat().st_mode & 0o777 == 0o600
        assert "api_token=" not in (runtime_dir / "deployment.env").read_text()

        before_recovery_destroy = len(record_path.read_text().splitlines())

        recovery_destroy_result = run("destroy")

        assert recovery_destroy_result.returncode == 0, recovery_destroy_result.stderr
        recovery_destroy_commands = record_path.read_text().splitlines()[before_recovery_destroy:]
        assert not any(command.startswith("kubectl ") for command in recovery_destroy_commands)
        assert not any("destroy" in command and "workload.tfstate" in command for command in recovery_destroy_commands)
        assert any("destroy" in command and "foundation.tfstate" in command for command in recovery_destroy_commands)
        assert not runtime_dir.exists()
        assert not state_dir.exists()

        record_path.write_text("")

        plan_result = run("plan")
        before_deploy = len(record_path.read_text().splitlines())
        deploy_result = run("deploy")

        assert plan_result.returncode == 0, plan_result.stderr
        assert deploy_result.returncode == 0, deploy_result.stderr

        commands = record_path.read_text().splitlines()
        deploy_commands = commands[before_deploy:]
        workload_validation = next(
            index
            for index, command in enumerate(deploy_commands)
            if command.startswith("terraform ") and "/workload validate" in command
        )
        foundation_apply = next(
            index
            for index, command in enumerate(commands)
            if command.startswith("terraform ") and "apply" in command and "foundation.tfstate" in command
        )
        image_publish = next(index for index, command in enumerate(commands) if command.startswith("docker push "))
        workload_apply = next(
            index
            for index, command in enumerate(commands)
            if command.startswith("terraform ") and "apply" in command and "workload.tfstate" in command
        )
        readiness = next(
            index for index, command in enumerate(commands) if command.startswith("kubectl ") and "rollout status" in command
        )
        deploy_foundation_apply = next(
            index
            for index, command in enumerate(deploy_commands)
            if command.startswith("terraform ") and "apply" in command and "foundation.tfstate" in command
        )
        assert workload_validation < deploy_foundation_apply
        assert foundation_apply < image_publish < workload_apply < readiness
        assert all("--profile vals-dev" in command for command in commands if command.startswith("aws "))
        assert all("0000000000000000000000000000000000000000000000000000000000000007" not in command for command in commands)
        assert "terraform-token-env plan" in commands
        assert "terraform-token-env apply" in commands
        assert "0000000000000000000000000000000000000000000000000000000000000007" not in deploy_result.stdout
        assert "0000000000000000000000000000000000000000000000000000000000000007" not in deploy_result.stderr
        assert (runtime_dir / "deployment.env").stat().st_mode & 0o777 == 0o600
        assert "runtime_phase=workload" in (runtime_dir / "deployment.env").read_text()

        preserved_runtime = (runtime_dir / "deployment.env").read_text()

        interrupted_redeploy_result = run("deploy", {"SHIM_ECR_LOGIN_FAIL": "1"})

        assert interrupted_redeploy_result.returncode != 0
        assert (runtime_dir / "deployment.env").read_text() == preserved_runtime

        before_dead_port_forward = len(record_path.read_text().splitlines())

        dead_port_forward_result = run("test", {"SHIM_PORT_FORWARD_DEAD": "1"})

        assert dead_port_forward_result.returncode != 0
        dead_port_forward_commands = record_path.read_text().splitlines()[before_dead_port_forward:]
        assert not any(command.startswith("uv run pytest") for command in dead_port_forward_commands)
        assert not any(" destroy " in f" {command} " for command in dead_port_forward_commands)

        before_test = len(record_path.read_text().splitlines())

        test_result = run("test", {"SHIM_UV_FAIL": "1"})

        assert test_result.returncode != 0
        failed_test_commands = record_path.read_text().splitlines()[before_test:]
        assert not any(" destroy " in f" {command} " for command in failed_test_commands)

        before_destroy = len(record_path.read_text().splitlines())

        failed_destroy_result = run("destroy", {"SHIM_WORKLOAD_DESTROY_FAIL": "1"})

        assert failed_destroy_result.returncode != 0
        failed_destroy_commands = record_path.read_text().splitlines()[before_destroy:]
        assert any("destroy" in command and "workload.tfstate" in command for command in failed_destroy_commands)
        assert not any("destroy" in command and "foundation.tfstate" in command for command in failed_destroy_commands)
        assert not any("resourcegroupstaggingapi get-resources" in command for command in failed_destroy_commands)
        assert runtime_dir.exists()
        assert state_dir.exists()

        before_destroy = len(record_path.read_text().splitlines())

        namespace_failure_result = run("destroy", {"SHIM_NAMESPACE_FAILURE": "1"})

        assert namespace_failure_result.returncode != 0
        namespace_failure_commands = record_path.read_text().splitlines()[before_destroy:]
        assert any("destroy" in command and "workload.tfstate" in command for command in namespace_failure_commands)
        assert any("--ignore-not-found -o name" in command for command in namespace_failure_commands)
        assert not any("destroy" in command and "foundation.tfstate" in command for command in namespace_failure_commands)
        assert runtime_dir.exists()
        assert state_dir.exists()

        before_destroy = len(record_path.read_text().splitlines())

        residue_result = run("destroy", {"SHIM_TAGGED_ARNS": "arn:aws:ec2:us-east-2:123456789012:vpc/test"})

        assert residue_result.returncode != 0
        residue_commands = record_path.read_text().splitlines()[before_destroy:]
        assert any("destroy" in command and "workload.tfstate" in command for command in residue_commands)
        assert any("get namespace benchmark-sandboxes --ignore-not-found -o name" in command for command in residue_commands)
        assert any("destroy" in command and "foundation.tfstate" in command for command in residue_commands)
        assert any("resourcegroupstaggingapi get-resources" in command for command in residue_commands)
        assert not any(command.startswith("rm -rf -- ") for command in residue_commands)
        foundation_runtime = (runtime_dir / "deployment.env").read_text()
        assert "runtime_phase=foundation" in foundation_runtime
        assert "api_token=" not in foundation_runtime

        before_destroy = len(record_path.read_text().splitlines())

        destroy_result = run("destroy")

        assert destroy_result.returncode == 0, destroy_result.stderr

        destroy_commands = record_path.read_text().splitlines()[before_destroy:]
        foundation_destroy = next(
            index
            for index, command in enumerate(destroy_commands)
            if command.startswith("terraform ") and "destroy" in command and "foundation.tfstate" in command
        )
        tag_check = next(
            index
            for index, command in enumerate(destroy_commands)
            if command.startswith("aws ") and "resourcegroupstaggingapi get-resources" in command
        )
        cleanup = next(index for index, command in enumerate(destroy_commands) if command.startswith("rm -rf -- "))
        assert not any(command.startswith("kubectl ") for command in destroy_commands)
        assert not any("destroy" in command and "workload.tfstate" in command for command in destroy_commands)
        assert foundation_destroy < tag_check < cleanup
        assert not runtime_dir.exists()
        assert not state_dir.exists()
    finally:
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(state_dir, ignore_errors=True)
