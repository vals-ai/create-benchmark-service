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
WORKLOAD_MAIN_PATH = REPOSITORY_ROOT / "infra/kubernetes/workload/main.tf"
KARPENTER_CAPACITY_PATH = REPOSITORY_ROOT / "infra/kubernetes/charts/karpenter-sandbox/templates/capacity.yaml"


def test_kubernetes_aws_orchestration(tmp_path: Path) -> None:
    """Verify preflight rejection, operation ordering, and failure preservation.

    Test cases:
    - Missing or invalid AWS identity inputs stop before Terraform changes.
    - Spot profiles check their sized Spot quota and never fall back to On-Demand.
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
    if [[ "${SHIM_EC2_VERIFY_FAIL:-0}" == "1" && "$*" == *"ec2 describe-"* ]]; then
      exit 1
    fi
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
      *"service-quotas get-service-quota"*"L-1216C47A"*)
        printf '%s\n' "${SHIM_STANDARD_VCPU_QUOTA:-5000}"
        ;;
      *"service-quotas get-service-quota"*"L-34B43A08"*)
        printf '%s\n' "${SHIM_STANDARD_SPOT_VCPU_QUOTA:-5000}"
        ;;
      *"resourcegroupstaggingapi get-resources"*)
        printf '%s\\n' "${SHIM_TAGGED_ARNS:-}"
        ;;
      *"ec2 describe-vpcs"*)
        printf '%s\\n' "${SHIM_VPC_ID:-}"
        ;;
      *"ec2 describe-instances"*)
        printf '%s\\n' "${SHIM_INSTANCE_STATE:-}"
        ;;
      *"ec2 describe-nat-gateways"*)
        printf '%s\\n' "${SHIM_NAT_GATEWAY_STATE:-}"
        ;;
      *"ec2 describe-vpc-endpoints"*)
        printf '%s\\n' "${SHIM_VPC_ENDPOINT_ID:-}"
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
    if [[ "$*" == *"foundation.tfplan"* && "$*" == *" plan "* && -n "${SHIM_EXPECTED_SYSTEM_NODE_SIZE:-}" ]]; then
      [[ "$*" == *"-var=system_node_min_size=$SHIM_EXPECTED_SYSTEM_NODE_SIZE"* ]]
      [[ "$*" == *"-var=system_node_max_size=$SHIM_EXPECTED_SYSTEM_NODE_SIZE"* ]]
      [[ "$*" == *"-var=system_node_desired_size=$SHIM_EXPECTED_SYSTEM_NODE_SIZE"* ]]
    fi
    if [[ "${SHIM_FOUNDATION_APPLY_FAIL:-0}" == "1" && "$*" == *" apply "*"foundation.tfstate"* ]]; then
      exit 1
    fi
    if [[ "$*" == *"workload.tfplan"* && "$*" == *" plan "* ]]; then
      [[ "${TF_VAR_api_token:-}" == "0000000000000000000000000000000000000000000000000000000000000007" ]]
      [[ "${TF_VAR_activity_write_interval_seconds:-}" == "${SHIM_EXPECTED_ACTIVITY_WRITE_INTERVAL_SECONDS:-30}" ]]
      [[ "${TF_VAR_karpenter_capacity_types:-}" == "${SHIM_EXPECTED_KARPENTER_CAPACITY_TYPES:-[\"on-demand\"]}" ]]
      if [[ -n "${SHIM_EXPECTED_NAMESPACE_POD_QUOTA:-}" ]]; then
        [[ "${TF_VAR_namespace_pod_quota:-}" == "$SHIM_EXPECTED_NAMESPACE_POD_QUOTA" ]]
        [[ "${TF_VAR_namespace_cpu_quota:-}" == "$SHIM_EXPECTED_NAMESPACE_CPU_QUOTA" ]]
        [[ "${TF_VAR_namespace_cpu_limit_quota:-}" == "$SHIM_EXPECTED_NAMESPACE_CPU_LIMIT_QUOTA" ]]
        [[ "${TF_VAR_namespace_memory_quota:-}" == "$SHIM_EXPECTED_NAMESPACE_MEMORY_QUOTA" ]]
        [[ "${TF_VAR_namespace_memory_limit_quota:-}" == "$SHIM_EXPECTED_NAMESPACE_MEMORY_LIMIT_QUOTA" ]]
        [[ "${TF_VAR_namespace_storage_quota:-}" == "$SHIM_EXPECTED_NAMESPACE_STORAGE_QUOTA" ]]
        [[ "${TF_VAR_namespace_storage_limit_quota:-}" == "$SHIM_EXPECTED_NAMESPACE_STORAGE_LIMIT_QUOTA" ]]
        [[ "${TF_VAR_karpenter_cpu_limit:-}" == "$SHIM_EXPECTED_KARPENTER_CPU_LIMIT" ]]
        [[ "${TF_VAR_karpenter_memory_limit:-}" == "$SHIM_EXPECTED_KARPENTER_MEMORY_LIMIT" ]]
        [[ "${TF_VAR_karpenter_root_volume_size:-}" == "$SHIM_EXPECTED_KARPENTER_ROOT_VOLUME_SIZE" ]]
      fi
      printf 'terraform-token-env plan\\n' >> "$RECORD_PATH"
      printf 'terraform-capacity-env plan %s\\n' "$TF_VAR_karpenter_capacity_types" >> "$RECORD_PATH"
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
    if [[ "$*" == *" destroy "*"workload.tfstate"* ]]; then
      [[ "${TF_VAR_karpenter_capacity_types:-}" == "${SHIM_EXPECTED_KARPENTER_CAPACITY_TYPES:-[\"on-demand\"]}" ]]
      printf 'terraform-capacity-env destroy %s\\n' "$TF_VAR_karpenter_capacity_types" >> "$RECORD_PATH"
    fi
    if [[ "$*" == *" destroy "*"foundation.tfstate"* ]]; then
      [[ "${TF_VAR_aws_account_id:-}" == "123456789012" ]]
      [[ "${TF_VAR_coredns_replica_count:-}" == "${SHIM_EXPECTED_COREDNS_REPLICAS:-2}" ]]
      printf 'terraform-account-env destroy\\n' >> "$RECORD_PATH"
      if [[ "${SHIM_FOUNDATION_DESTROY_TRANSIENT:-0}" == "1" ]]; then
        foundation_destroy_attempts="$(grep -c 'terraform .* destroy .*foundation.tfstate' "$RECORD_PATH")"
        if [[ "$foundation_destroy_attempts" == "1" ]]; then
          exit 1
        fi
      fi
    fi
    ;;
  openssl)
    printf '%064d\\n' 7
    ;;
  git)
    if [[ "$*" == *"hash-object --stdin"* ]]; then
      while IFS= read -r _line; do :; done
    fi
    printf '0123456789ab\\n'
    ;;
  docker)
    if [[ "$*" == login* ]]; then
      read -r _password || true
    fi
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
    if [[ "$*" == *"exec daemonset/cilium"* ]]; then
      printf 'Encryption:       Wireguard [cilium_wg0 (Peers: 0)]\\n'
    fi
    ;;
  uv)
    if [[ "${1:-}" == "run" && "${2:-}" == "python" ]]; then
      shift 2
      exec "$REAL_PYTHON" "$@"
    fi
    if [[ "$*" == *"test_kubernetes_control_service.py"* ]]; then
      [[ "${TEST_KUBERNETES_COMPOSE_IMAGE:-}" == 123456789012.dkr.ecr.us-east-2.amazonaws.com/test/sandbox-images@sha256:*2 ]]
      printf 'compose-image-env pytest\n' >> "$RECORD_PATH"
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
        "TEST_KUBERNETES_IMAGE": (f"123456789012.dkr.ecr.us-east-2.amazonaws.com/benchmark@sha256:{'b' * 64}"),
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
        ({"KUBERNETES_DEPLOYMENT_NAME": "a" * 26}, "3-25 character"),
        ({"KUBERNETES_SCALE_PROFILE": "scale-100-spot"}, "KARPENTER_AMI_ALIAS"),
        ({"KUBERNETES_SCALE_PROFILE": "scale-250-spot"}, "KARPENTER_AMI_ALIAS"),
        ({"KUBERNETES_SCALE_PROFILE": "scale-500-spot"}, "KARPENTER_AMI_ALIAS"),
        ({"KUBERNETES_SCALE_PROFILE": "scale-2000"}, "KARPENTER_AMI_ALIAS"),
        ({"KUBERNETES_SCALE_PROFILE": "scale-2000-spot"}, "KARPENTER_AMI_ALIAS"),
    )

    state_dir = REPOSITORY_ROOT / "infra/kubernetes/aws/.state/test-orchestration"
    runtime_dir = REPOSITORY_ROOT / "infra/kubernetes/aws/.runtime/test-orchestration"
    try:
        foundation_variables = FOUNDATION_VARIABLES_PATH.read_text()
        foundation_main = FOUNDATION_MAIN_PATH.read_text()
        workload_main = WORKLOAD_MAIN_PATH.read_text()
        karpenter_capacity = KARPENTER_CAPACITY_PATH.read_text()
        orchestration_script = SCRIPT_PATH.read_text()
        assert 'variable "aws_account_id"' in foundation_variables
        assert 'variable "coredns_replica_count"' in foundation_variables
        assert "allowed_account_ids = [var.aws_account_id]" in foundation_main
        assert "replicaCount = var.coredns_replica_count" in foundation_main
        assert "ingress_nodes_wireguard" in foundation_main
        assert "from_port   = 51871" in foundation_main
        assert "node.cilium.io/agent-not-ready" not in foundation_main
        assert "min_size     = var.system_node_min_size" in foundation_main
        assert "max_size     = var.system_node_max_size" in foundation_main
        assert "desired_size = var.system_node_desired_size" in foundation_main
        assert "enable_inline_policy            = true" in foundation_main
        assert 'resource "aws_iam_service_linked_role" "ec2_spot"' in foundation_main
        assert 'aws_service_name = "spot.amazonaws.com"' in foundation_main
        assert 'export TEST_KUBERNETES_COMPOSE_IMAGE="$docker_image"' in orchestration_script
        assert "git hash-object --stdin-paths" in orchestration_script
        assert 'control_tag="control-$commit_sha-${control_source_hash:0:12}"' in orchestration_script
        assert "exec daemonset/cilium -c cilium-agent -- cilium-dbg status" in orchestration_script
        assert "trap - EXIT INT TERM" in orchestration_script
        assert "namespace_pod_quota=150" in orchestration_script
        assert "namespace_pod_quota=2200" in orchestration_script
        assert "system_node_desired_size=3" in orchestration_script
        scale_500_profile = orchestration_script.split("    scale-500-spot)", 1)[1].split(
            "    scale-2000|scale-2000-spot)", 1
        )[0]
        assert "system_node_min_size=3" in scale_500_profile
        assert "system_node_max_size=3" in scale_500_profile
        assert "system_node_desired_size=3" in scale_500_profile
        assert orchestration_script.count('TF_VAR_system_node_desired_size="$system_node_desired_size"') == 1
        assert "coredns_replica_count=2" in orchestration_script
        assert "coredns_replica_count=6" in orchestration_script
        assert orchestration_script.count('-var="coredns_replica_count=$coredns_replica_count"') == 1
        assert (
            orchestration_script.count('-var="create_spot_service_linked_role=$create_spot_service_linked_role"') == 1
        )
        assert (
            orchestration_script.count('TF_VAR_create_spot_service_linked_role="$create_spot_service_linked_role"') == 1
        )
        assert orchestration_script.count('TF_VAR_coredns_replica_count="$coredns_replica_count"') == 1
        assert "activity_write_interval_seconds=30" in orchestration_script
        assert "activity_write_interval_seconds=300" in orchestration_script
        assert (
            orchestration_script.count('TF_VAR_activity_write_interval_seconds="$activity_write_interval_seconds"') == 2
        )
        assert "namespace_cpu_limit_quota=8500" in orchestration_script
        assert "namespace_memory_limit_quota=17000Gi" in orchestration_script
        assert "namespace_storage_limit_quota=82000Gi" in orchestration_script
        assert "L-1216C47A" in orchestration_script
        assert "L-34B43A08" in orchestration_script
        assert "karpenter_root_volume_size=100Gi" in orchestration_script
        assert "karpenter_root_volume_size=250Gi" in orchestration_script
        assert "karpenter_root_volume_size=500Gi" in orchestration_script
        assert orchestration_script.count('TF_VAR_karpenter_root_volume_size="$karpenter_root_volume_size"') == 2
        assert 'resources  = ["pods/exec"]' not in workload_main
        assert 'verbs      = ["get", "list", "watch", "create", "patch", "delete"]' in workload_main
        assert 'verbs      = ["get", "list", "watch"]' in workload_main
        assert '"app.kubernetes.io/managed-by" = "benchmark-sandbox-control"' in workload_main
        assert 'name  = "KUBERNETES_SANDBOX_AGENT_IMAGE"' in workload_main
        assert 'type    = "wireguard"' in workload_main
        assert "enableRouteMTUForCNIChaining = true" in workload_main
        assert 'path   = "/ready"' in workload_main
        assert "startupTaints:" in karpenter_capacity
        assert "taints:" in karpenter_capacity
        assert "key: sandbox.vals.ai/dedicated" in karpenter_capacity
        assert "key: node.cilium.io/agent-not-ready" in karpenter_capacity
        assert "effect: NoExecute" in karpenter_capacity

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
        insufficient_quota = run(
            "plan",
            {
                "KUBERNETES_SCALE_PROFILE": "scale-2000",
                "KARPENTER_AMI_ALIAS": "al2023@v20260715",
                "SHIM_STANDARD_VCPU_QUOTA": "4999",
            },
        )

        assert insufficient_quota.returncode != 0
        assert "5,000 Standard On-Demand vCPUs" in insufficient_quota.stderr
        assert "service-quotas get-service-quota" in record_path.read_text()
        assert "terraform " not in record_path.read_text()

        record_path.write_text("")

        insufficient_small_spot_quota = run(
            "plan",
            {
                "KUBERNETES_SCALE_PROFILE": "scale-100-spot",
                "KARPENTER_AMI_ALIAS": "al2023@v20260715",
                "SHIM_STANDARD_SPOT_VCPU_QUOTA": "255",
            },
        )

        assert insufficient_small_spot_quota.returncode != 0
        assert "256 Standard Spot vCPUs" in insufficient_small_spot_quota.stderr

        small_spot_quota_commands = record_path.read_text()
        assert "L-34B43A08" in small_spot_quota_commands
        assert "L-1216C47A" not in small_spot_quota_commands
        assert "terraform " not in small_spot_quota_commands

        record_path.write_text("")

        insufficient_250_spot_quota = run(
            "plan",
            {
                "KUBERNETES_SCALE_PROFILE": "scale-250-spot",
                "KARPENTER_AMI_ALIAS": "al2023@v20260715",
                "SHIM_STANDARD_SPOT_VCPU_QUOTA": "511",
            },
        )

        assert insufficient_250_spot_quota.returncode != 0
        assert "512 Standard Spot vCPUs" in insufficient_250_spot_quota.stderr

        insufficient_250_spot_commands = record_path.read_text()
        assert "L-34B43A08" in insufficient_250_spot_commands
        assert "terraform " not in insufficient_250_spot_commands

        record_path.write_text("")

        insufficient_500_spot_quota = run(
            "plan",
            {
                "KUBERNETES_SCALE_PROFILE": "scale-500-spot",
                "KARPENTER_AMI_ALIAS": "al2023@v20260715",
                "SHIM_STANDARD_SPOT_VCPU_QUOTA": "1023",
            },
        )

        assert insufficient_500_spot_quota.returncode != 0
        assert "1,024 Standard Spot vCPUs" in insufficient_500_spot_quota.stderr

        insufficient_500_spot_commands = record_path.read_text()
        assert "L-34B43A08" in insufficient_500_spot_commands
        assert "terraform " not in insufficient_500_spot_commands

        record_path.write_text("")

        insufficient_spot_quota = run(
            "plan",
            {
                "KUBERNETES_SCALE_PROFILE": "scale-2000-spot",
                "KARPENTER_AMI_ALIAS": "al2023@v20260715",
                "SHIM_STANDARD_SPOT_VCPU_QUOTA": "4999",
            },
        )

        assert insufficient_spot_quota.returncode != 0
        assert "5,000 Standard Spot vCPUs" in insufficient_spot_quota.stderr

        spot_quota_commands = record_path.read_text()
        assert "L-34B43A08" in spot_quota_commands
        assert "L-1216C47A" not in spot_quota_commands
        assert "terraform " not in spot_quota_commands

        record_path.write_text("")

        spot_environment = {
            "KUBERNETES_SCALE_PROFILE": "scale-100-spot",
            "KARPENTER_AMI_ALIAS": "al2023@v20260715",
            "SHIM_EXPECTED_ACTIVITY_WRITE_INTERVAL_SECONDS": "30",
            "SHIM_EXPECTED_COREDNS_REPLICAS": "2",
            "SHIM_EXPECTED_KARPENTER_CAPACITY_TYPES": '["spot"]',
            "SHIM_STANDARD_SPOT_VCPU_QUOTA": "1152",
        }
        spot_plan_result = run("plan", spot_environment)
        spot_deploy_result = run("deploy", spot_environment)

        assert spot_plan_result.returncode == 0, spot_plan_result.stderr
        assert spot_deploy_result.returncode == 0, spot_deploy_result.stderr

        spot_commands = record_path.read_text().splitlines()
        assert 'terraform-capacity-env plan ["spot"]' in spot_commands

        spot_plan_inputs = state_dir / "foundation-plan.env"
        assert "planned_karpenter_capacity_types=" in spot_plan_inputs.read_text()
        assert "spot" in spot_plan_inputs.read_text()

        spot_destroy_result = run("destroy", spot_environment)

        assert spot_destroy_result.returncode == 0, spot_destroy_result.stderr
        assert 'terraform-capacity-env destroy ["spot"]' in record_path.read_text().splitlines()
        assert not runtime_dir.exists()
        assert not state_dir.exists()

        record_path.write_text("")

        spot_250_environment = {
            "KUBERNETES_SCALE_PROFILE": "scale-250-spot",
            "KARPENTER_AMI_ALIAS": "al2023@v20260715",
            "SHIM_EXPECTED_ACTIVITY_WRITE_INTERVAL_SECONDS": "30",
            "SHIM_EXPECTED_COREDNS_REPLICAS": "2",
            "SHIM_EXPECTED_KARPENTER_CAPACITY_TYPES": '["spot"]',
            "SHIM_EXPECTED_NAMESPACE_POD_QUOTA": "300",
            "SHIM_EXPECTED_NAMESPACE_CPU_QUOTA": "400",
            "SHIM_EXPECTED_NAMESPACE_CPU_LIMIT_QUOTA": "900",
            "SHIM_EXPECTED_NAMESPACE_MEMORY_QUOTA": "400Gi",
            "SHIM_EXPECTED_NAMESPACE_MEMORY_LIMIT_QUOTA": "1500Gi",
            "SHIM_EXPECTED_NAMESPACE_STORAGE_QUOTA": "2000Gi",
            "SHIM_EXPECTED_NAMESPACE_STORAGE_LIMIT_QUOTA": "7500Gi",
            "SHIM_EXPECTED_KARPENTER_CPU_LIMIT": "512",
            "SHIM_EXPECTED_KARPENTER_MEMORY_LIMIT": "1024Gi",
            "SHIM_EXPECTED_KARPENTER_ROOT_VOLUME_SIZE": "250Gi",
            "SHIM_STANDARD_SPOT_VCPU_QUOTA": "1152",
        }
        spot_250_plan_result = run("plan", spot_250_environment)
        spot_250_deploy_result = run("deploy", spot_250_environment)

        assert spot_250_plan_result.returncode == 0, spot_250_plan_result.stderr
        assert spot_250_deploy_result.returncode == 0, spot_250_deploy_result.stderr

        spot_250_destroy_result = run(
            "destroy",
            {
                **spot_250_environment,
                "KUBERNETES_DESTROY_RETRY_DELAY_SECONDS": "0",
                "SHIM_FOUNDATION_DESTROY_TRANSIENT": "1",
            },
        )

        assert spot_250_destroy_result.returncode == 0, spot_250_destroy_result.stderr
        spot_250_commands = record_path.read_text().splitlines()
        assert (
            sum(
                "terraform " in command and " destroy " in f" {command} " and "foundation.tfstate" in command
                for command in spot_250_commands
            )
            == 2
        )
        assert not runtime_dir.exists()
        assert not state_dir.exists()

        record_path.write_text("")

        spot_500_environment = {
            "KUBERNETES_SCALE_PROFILE": "scale-500-spot",
            "KARPENTER_AMI_ALIAS": "al2023@v20260715",
            "SHIM_EXPECTED_ACTIVITY_WRITE_INTERVAL_SECONDS": "30",
            "SHIM_EXPECTED_COREDNS_REPLICAS": "2",
            "SHIM_EXPECTED_KARPENTER_CAPACITY_TYPES": '["spot"]',
            "SHIM_EXPECTED_NAMESPACE_POD_QUOTA": "600",
            "SHIM_EXPECTED_NAMESPACE_CPU_QUOTA": "800",
            "SHIM_EXPECTED_NAMESPACE_CPU_LIMIT_QUOTA": "1800",
            "SHIM_EXPECTED_NAMESPACE_MEMORY_QUOTA": "800Gi",
            "SHIM_EXPECTED_NAMESPACE_MEMORY_LIMIT_QUOTA": "3000Gi",
            "SHIM_EXPECTED_NAMESPACE_STORAGE_QUOTA": "4000Gi",
            "SHIM_EXPECTED_NAMESPACE_STORAGE_LIMIT_QUOTA": "15000Gi",
            "SHIM_EXPECTED_KARPENTER_CPU_LIMIT": "1024",
            "SHIM_EXPECTED_KARPENTER_MEMORY_LIMIT": "2048Gi",
            "SHIM_EXPECTED_KARPENTER_ROOT_VOLUME_SIZE": "250Gi",
            "SHIM_EXPECTED_SYSTEM_NODE_SIZE": "3",
            "SHIM_STANDARD_SPOT_VCPU_QUOTA": "1152",
        }
        spot_500_plan_result = run("plan", spot_500_environment)
        spot_500_deploy_result = run("deploy", spot_500_environment)

        assert spot_500_plan_result.returncode == 0, spot_500_plan_result.stderr
        assert spot_500_deploy_result.returncode == 0, spot_500_deploy_result.stderr

        spot_500_destroy_result = run("destroy", spot_500_environment)

        assert spot_500_destroy_result.returncode == 0, spot_500_destroy_result.stderr
        assert not runtime_dir.exists()
        assert not state_dir.exists()

        record_path.write_text("")

        plan_result = run("plan")
        plan_commands = record_path.read_text().splitlines()
        assert any("-var=aws_account_id=123456789012" in command for command in plan_commands)
        assert any("-var=coredns_replica_count=2" in command for command in plan_commands)
        plan_inputs = state_dir / "foundation-plan.env"
        assert plan_inputs.exists()
        assert plan_inputs.stat().st_mode & 0o777 == 0o600
        assert "planned_aws_region=us-east-2" in plan_inputs.read_text()

        before_drifted_deploy = len(record_path.read_text().splitlines())
        drifted_deploy_result = run("deploy", {"AWS_REGION": "us-west-2"})
        drifted_deploy_commands = record_path.read_text().splitlines()[before_drifted_deploy:]

        assert drifted_deploy_result.returncode != 0
        assert "changed since plan" in drifted_deploy_result.stderr
        assert not any(" apply " in f" {command} " for command in drifted_deploy_commands)

        interrupted_apply_result = run("deploy", {"SHIM_FOUNDATION_APPLY_FAIL": "1"})

        assert plan_result.returncode == 0, plan_result.stderr
        assert interrupted_apply_result.returncode != 0
        foundation_runtime = (runtime_dir / "deployment.env").read_text()
        assert "runtime_phase=foundation" in foundation_runtime
        assert "cluster_name=test-orchestration-eks" in foundation_runtime
        assert (
            "image_repository_url=123456789012.dkr.ecr.us-east-2.amazonaws.com/test-orchestration/sandbox-images"
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
            index
            for index, command in enumerate(commands)
            if command.startswith("kubectl ") and "rollout status" in command
        )
        deploy_foundation_apply = next(
            index
            for index, command in enumerate(deploy_commands)
            if command.startswith("terraform ") and "apply" in command and "foundation.tfstate" in command
        )
        assert workload_validation < deploy_foundation_apply
        assert foundation_apply < image_publish < workload_apply < readiness
        control_build = next(command for command in commands if command.startswith("docker build "))
        dind_pull = next(command for command in commands if command.startswith("docker pull "))
        assert "--platform linux/amd64" in control_build
        assert "--platform linux/amd64" in dind_pull
        assert all("--profile vals-dev" in command for command in commands if command.startswith("aws "))
        assert all(
            "0000000000000000000000000000000000000000000000000000000000000007" not in command for command in commands
        )
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
        assert "compose-image-env pytest" in failed_test_commands
        assert not any(" destroy " in f" {command} " for command in failed_test_commands)

        before_successful_test = len(record_path.read_text().splitlines())

        successful_test_result = run("test")

        assert successful_test_result.returncode == 0, successful_test_result.stderr
        successful_test_commands = record_path.read_text().splitlines()[before_successful_test:]
        assert "compose-image-env pytest" in successful_test_commands
        assert not any(" destroy " in f" {command} " for command in successful_test_commands)

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
        assert not any(
            "destroy" in command and "foundation.tfstate" in command for command in namespace_failure_commands
        )
        assert runtime_dir.exists()
        assert state_dir.exists()

        before_destroy = len(record_path.read_text().splitlines())

        verification_failure_result = run(
            "destroy",
            {
                "SHIM_TAGGED_ARNS": "arn:aws:ec2:us-east-2:123456789012:subnet/subnet-unknown",
                "SHIM_EC2_VERIFY_FAIL": "1",
            },
        )

        assert verification_failure_result.returncode != 0
        verification_failure_commands = record_path.read_text().splitlines()[before_destroy:]
        assert any("destroy" in command and "workload.tfstate" in command for command in verification_failure_commands)
        assert any(
            "get namespace benchmark-sandboxes --ignore-not-found -o name" in command
            for command in verification_failure_commands
        )
        assert any(
            "destroy" in command and "foundation.tfstate" in command for command in verification_failure_commands
        )
        assert any("ec2 describe-subnets" in command for command in verification_failure_commands)
        assert not any(command.startswith("rm -rf -- ") for command in verification_failure_commands)
        assert runtime_dir.exists()
        assert state_dir.exists()

        before_destroy = len(record_path.read_text().splitlines())

        residue_result = run(
            "destroy",
            {
                "SHIM_TAGGED_ARNS": "arn:aws:ec2:us-east-2:123456789012:vpc/test",
                "SHIM_VPC_ID": "test",
            },
        )

        assert residue_result.returncode != 0
        residue_commands = record_path.read_text().splitlines()[before_destroy:]
        assert not any("destroy" in command and "workload.tfstate" in command for command in residue_commands)
        assert not any(command.startswith("kubectl ") for command in residue_commands)
        assert any("destroy" in command and "foundation.tfstate" in command for command in residue_commands)
        assert any("resourcegroupstaggingapi get-resources" in command for command in residue_commands)
        assert not any(command.startswith("rm -rf -- ") for command in residue_commands)
        foundation_runtime = (runtime_dir / "deployment.env").read_text()
        assert "runtime_phase=foundation" in foundation_runtime
        assert "api_token=" not in foundation_runtime

        before_destroy = len(record_path.read_text().splitlines())

        destroy_result = run(
            "destroy",
            {
                "SHIM_TAGGED_ARNS": " ".join(
                    [
                        "arn:aws:ec2:us-east-2:123456789012:instance/i-deleted",
                        "arn:aws:ec2:us-east-2:123456789012:natgateway/nat-deleted",
                        "arn:aws:ec2:us-east-2:123456789012:vpc-endpoint/vpce-deleted",
                        "arn:aws:ec2:us-east-2:123456789012:subnet/subnet-deleted",
                    ]
                ),
                "SHIM_INSTANCE_STATE": "terminated",
                "SHIM_NAT_GATEWAY_STATE": "deleted",
            },
        )

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
        assert any("ec2 describe-instances" in command for command in destroy_commands)
        assert any("ec2 describe-nat-gateways" in command for command in destroy_commands)
        assert any("ec2 describe-vpc-endpoints" in command for command in destroy_commands)
        assert any("ec2 describe-subnets" in command for command in destroy_commands)
        assert foundation_destroy < tag_check < cleanup
        assert not runtime_dir.exists()
        assert not state_dir.exists()
    finally:
        shutil.rmtree(runtime_dir, ignore_errors=True)
        shutil.rmtree(state_dir, ignore_errors=True)
