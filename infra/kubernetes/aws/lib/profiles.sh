configure_scale_profile() {
  local configured_ami_alias="${KARPENTER_AMI_ALIAS:-${karpenter_ami_alias:-}}"

  case "$scale_profile" in
    smoke)
      karpenter_ami_alias="${configured_ami_alias:-al2023@latest}"
      karpenter_capacity_types='["on-demand"]'
      create_spot_service_linked_role=false
      nat_gateway_per_az=false
      system_node_min_size=1
      system_node_max_size=2
      system_node_desired_size=1
      coredns_replica_count=2
      control_replicas=3
      activity_write_interval_seconds=30
      exec_connection_pool_size=1024
      namespace_pod_quota=20
      namespace_cpu_quota=8
      namespace_cpu_limit_quota=80
      namespace_memory_quota=16Gi
      namespace_memory_limit_quota=160Gi
      namespace_storage_quota=100Gi
      namespace_storage_limit_quota=800Gi
      karpenter_cpu_limit=64
      karpenter_memory_limit=128Gi
      karpenter_root_volume_size=100Gi
      ;;
    scale-100-spot)
      [[ "$configured_ami_alias" =~ ^al2023@v[0-9]{8}$ ]] || fail \
        "KARPENTER_AMI_ALIAS must pin al2023@vYYYYMMDD for $scale_profile."
      karpenter_ami_alias="$configured_ami_alias"
      karpenter_capacity_types='["spot"]'
      create_spot_service_linked_role=true
      nat_gateway_per_az=false
      system_node_min_size=2
      system_node_max_size=3
      system_node_desired_size=2
      coredns_replica_count=2
      control_replicas=3
      activity_write_interval_seconds=30
      exec_connection_pool_size=1024
      namespace_pod_quota=150
      namespace_cpu_quota=160
      namespace_cpu_limit_quota=350
      namespace_memory_quota=160Gi
      namespace_memory_limit_quota=600Gi
      namespace_storage_quota=700Gi
      namespace_storage_limit_quota=3000Gi
      karpenter_cpu_limit=256
      karpenter_memory_limit=512Gi
      karpenter_root_volume_size=250Gi
      required_vcpu_quota=256
      required_vcpu_quota_label=256
      ;;
    scale-250-spot)
      [[ "$configured_ami_alias" =~ ^al2023@v[0-9]{8}$ ]] || fail \
        "KARPENTER_AMI_ALIAS must pin al2023@vYYYYMMDD for $scale_profile."
      karpenter_ami_alias="$configured_ami_alias"
      karpenter_capacity_types='["spot"]'
      create_spot_service_linked_role=true
      nat_gateway_per_az=false
      system_node_min_size=2
      system_node_max_size=3
      system_node_desired_size=2
      coredns_replica_count=2
      control_replicas=3
      activity_write_interval_seconds=30
      exec_connection_pool_size=1024
      namespace_pod_quota=300
      namespace_cpu_quota=400
      namespace_cpu_limit_quota=900
      namespace_memory_quota=400Gi
      namespace_memory_limit_quota=1500Gi
      namespace_storage_quota=2000Gi
      namespace_storage_limit_quota=7500Gi
      karpenter_cpu_limit=512
      karpenter_memory_limit=1024Gi
      karpenter_root_volume_size=250Gi
      required_vcpu_quota=512
      required_vcpu_quota_label=512
      ;;
    scale-500-spot)
      [[ "$configured_ami_alias" =~ ^al2023@v[0-9]{8}$ ]] || fail \
        "KARPENTER_AMI_ALIAS must pin al2023@vYYYYMMDD for $scale_profile."
      karpenter_ami_alias="$configured_ami_alias"
      karpenter_capacity_types='["spot"]'
      create_spot_service_linked_role=true
      nat_gateway_per_az=false
      system_node_min_size=3
      system_node_max_size=3
      system_node_desired_size=3
      coredns_replica_count=2
      control_replicas=3
      activity_write_interval_seconds=30
      exec_connection_pool_size=1024
      namespace_pod_quota=600
      namespace_cpu_quota=800
      namespace_cpu_limit_quota=1800
      namespace_memory_quota=800Gi
      namespace_memory_limit_quota=3000Gi
      namespace_storage_quota=4000Gi
      namespace_storage_limit_quota=15000Gi
      karpenter_cpu_limit=1024
      karpenter_memory_limit=2048Gi
      karpenter_root_volume_size=250Gi
      required_vcpu_quota=1024
      required_vcpu_quota_label=1,024
      ;;
    scale-2000|scale-2000-spot)
      [[ "$configured_ami_alias" =~ ^al2023@v[0-9]{8}$ ]] || fail \
        "KARPENTER_AMI_ALIAS must pin al2023@vYYYYMMDD for $scale_profile."
      karpenter_ami_alias="$configured_ami_alias"
      if [[ "$scale_profile" == "scale-2000-spot" ]]; then
        karpenter_capacity_types='["spot"]'
        create_spot_service_linked_role=true
      else
        karpenter_capacity_types='["on-demand"]'
        create_spot_service_linked_role=false
      fi
      nat_gateway_per_az=true
      system_node_min_size=3
      system_node_max_size=6
      system_node_desired_size=3
      coredns_replica_count=6
      control_replicas=6
      activity_write_interval_seconds=300
      exec_connection_pool_size=1024
      namespace_pod_quota=2200
      namespace_cpu_quota=5000
      namespace_cpu_limit_quota=8500
      namespace_memory_quota=9000Gi
      namespace_memory_limit_quota=17000Gi
      namespace_storage_quota=44000Gi
      namespace_storage_limit_quota=82000Gi
      karpenter_cpu_limit=5000
      karpenter_memory_limit=10000Gi
      karpenter_root_volume_size=500Gi
      required_vcpu_quota=5000
      required_vcpu_quota_label=5,000
      ;;
    *)
      fail "KUBERNETES_SCALE_PROFILE must be smoke, scale-100-spot, scale-250-spot, scale-500-spot, scale-2000, or scale-2000-spot."
      ;;
  esac
}

check_scale_quotas() {
  local quota_code
  local quota_name
  local vcpu_quota

  case "$scale_profile" in
    scale-2000)
      quota_code="L-1216C47A"
      quota_name="Standard On-Demand"
      ;;
    scale-100-spot|scale-250-spot|scale-500-spot|scale-2000-spot)
      quota_code="L-34B43A08"
      quota_name="Standard Spot"
      ;;
    *) return 0 ;;
  esac

  vcpu_quota="$(aws service-quotas get-service-quota \
    --profile vals-dev \
    --region "$aws_region" \
    --service-code ec2 \
    --quota-code "$quota_code" \
    --query Quota.Value \
    --output text)" || fail "Unable to read the EC2 $quota_name vCPU quota."
  if ! uv run python - "$vcpu_quota" "$karpenter_cpu_limit" >/dev/null <<'PY'
from decimal import Decimal, InvalidOperation
import sys

try:
    sufficient = Decimal(sys.argv[1]) >= Decimal(sys.argv[2])
except InvalidOperation:
    sufficient = False
raise SystemExit(0 if sufficient else 1)
PY
  then
    fail "$scale_profile requires at least $required_vcpu_quota_label $quota_name vCPUs in $aws_region."
  fi
}
