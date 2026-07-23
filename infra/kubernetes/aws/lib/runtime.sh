configure_paths() {
  state_dir="$script_dir/.state/$deployment_name"
  runtime_dir="$script_dir/.runtime/$deployment_name"
  foundation_state="$state_dir/foundation.tfstate"
  workload_state="$state_dir/workload.tfstate"
  foundation_plan="$state_dir/foundation.tfplan"
  workload_plan="$state_dir/workload.tfplan"
  plan_input_file="$state_dir/foundation-plan.env"
  runtime_file="$runtime_dir/deployment.env"
}

write_plan_inputs() {
  local temporary_plan_input_file="$plan_input_file.tmp"

  umask 077
  {
    printf 'planned_aws_account_id=%q\n' "$AWS_ACCOUNT_ID"
    printf 'planned_aws_profile=%q\n' "$AWS_PROFILE"
    printf 'planned_aws_region=%q\n' "$aws_region"
    printf 'planned_deployment_name=%q\n' "$deployment_name"
    printf 'planned_operator_cidr=%q\n' "$AWS_OPERATOR_CIDR"
    printf 'planned_scale_profile=%q\n' "$scale_profile"
    printf 'planned_karpenter_ami_alias=%q\n' "$karpenter_ami_alias"
    printf 'planned_karpenter_capacity_types=%q\n' "$karpenter_capacity_types"
    printf 'planned_nat_gateway_per_az=%q\n' "$nat_gateway_per_az"
  } > "$temporary_plan_input_file"
  chmod 600 "$temporary_plan_input_file"
  mv -f -- "$temporary_plan_input_file" "$plan_input_file"
}

require_unchanged_plan_inputs() {
  [[ -f "$plan_input_file" ]] || fail "Foundation plan inputs not found. Run plan again."
  # shellcheck disable=SC1090
  source "$plan_input_file"

  [[ "${planned_aws_account_id:-}" == "$AWS_ACCOUNT_ID" ]] || fail "AWS_ACCOUNT_ID changed since plan."
  [[ "${planned_aws_profile:-}" == "$AWS_PROFILE" ]] || fail "AWS_PROFILE changed since plan."
  [[ "${planned_aws_region:-}" == "$aws_region" ]] || fail "AWS_REGION changed since plan."
  [[ "${planned_deployment_name:-}" == "$deployment_name" ]] || fail \
    "KUBERNETES_DEPLOYMENT_NAME changed since plan."
  [[ "${planned_operator_cidr:-}" == "$AWS_OPERATOR_CIDR" ]] || fail \
    "AWS_OPERATOR_CIDR changed since plan."
  [[ "${planned_scale_profile:-}" == "$scale_profile" ]] || fail \
    "KUBERNETES_SCALE_PROFILE changed since plan."
  [[ "${planned_karpenter_ami_alias:-}" == "$karpenter_ami_alias" ]] || fail \
    "KARPENTER_AMI_ALIAS changed since plan."
  [[ "${planned_karpenter_capacity_types:-}" == "$karpenter_capacity_types" ]] || fail \
    "Karpenter capacity types changed since plan."
  [[ "${planned_nat_gateway_per_az:-}" == "$nat_gateway_per_az" ]] || fail \
    "NAT topology changed since plan."
}

write_foundation_runtime_contents() {
  printf 'runtime_phase=foundation\n'
  printf 'scale_profile=%q\n' "$scale_profile"
  printf 'karpenter_ami_alias=%q\n' "$karpenter_ami_alias"
  printf 'aws_region=%q\n' "$aws_region"
  printf 'operator_cidr=%q\n' "$operator_cidr"
  printf 'cluster_name=%q\n' "$cluster_name"
  printf 'image_repository_url=%q\n' "$image_repository_url"
  printf 'kubeconfig_path=%q\n' "$kubeconfig_path"
  printf 'kube_context=%q\n' "$kube_context"
}

write_foundation_runtime_file() {
  local current_aws_region="$aws_region"
  local current_cluster_name="$cluster_name"
  local current_image_repository_url="$image_repository_url"
  local existing_runtime_phase=""

  if [[ -f "$runtime_file" ]]; then
    # shellcheck disable=SC1090
    source "$runtime_file"
    existing_runtime_phase="${runtime_phase:-}"
  fi

  aws_region="$current_aws_region"
  operator_cidr="$AWS_OPERATOR_CIDR"
  cluster_name="$current_cluster_name"
  image_repository_url="$current_image_repository_url"
  kubeconfig_path="$runtime_dir/kubeconfig"
  kube_context="$deployment_name"
  if [[ "$existing_runtime_phase" == "workload" ]]; then
    return
  fi

  mkdir -p "$runtime_dir"
  umask 077
  write_foundation_runtime_contents > "$runtime_file"
  chmod 600 "$runtime_file"
  runtime_phase="foundation"
}

transition_to_foundation_runtime() {
  local temporary_runtime_file="$runtime_file.tmp"

  umask 077
  write_foundation_runtime_contents > "$temporary_runtime_file"
  chmod 600 "$temporary_runtime_file"
  mv -f -- "$temporary_runtime_file" "$runtime_file"
  runtime_phase="foundation"
  unset api_token control_image docker_image allowed_image_prefixes
}

write_runtime_file() {
  local saved_api_token=""
  local saved_runtime_phase="foundation"
  local current_cluster_name="$cluster_name"
  local current_image_repository_url="$image_repository_url"
  local current_control_image="$control_image"
  local current_docker_image="$docker_image"

  mkdir -p "$runtime_dir"
  if [[ -f "$runtime_file" ]]; then
    # shellcheck disable=SC1090
    source "$runtime_file"
    saved_api_token="${api_token:-}"
    saved_runtime_phase="${runtime_phase:-foundation}"
  fi
  if [[ -z "$saved_api_token" ]]; then
    saved_api_token="$(openssl rand -hex 32)"
  fi
  [[ "$saved_api_token" =~ ^[0-9a-f]{64}$ ]] || fail "The runtime API token is invalid."

  api_token="$saved_api_token"
  cluster_name="$current_cluster_name"
  image_repository_url="$current_image_repository_url"
  control_image="$current_control_image"
  docker_image="$current_docker_image"
  kubeconfig_path="$runtime_dir/kubeconfig"
  kube_context="$deployment_name"
  benchmark_image_prefix="${TEST_KUBERNETES_IMAGE%@*}@sha256:"
  stack_image_prefix="$image_repository_url@sha256:"
  allowed_image_prefixes="[\"$benchmark_image_prefix\",\"$stack_image_prefix\"]"

  umask 077
  {
    printf 'runtime_phase=%q\n' "$saved_runtime_phase"
    printf 'scale_profile=%q\n' "$scale_profile"
    printf 'karpenter_ami_alias=%q\n' "$karpenter_ami_alias"
    printf 'api_token=%q\n' "$api_token"
    printf 'aws_region=%q\n' "$aws_region"
    printf 'operator_cidr=%q\n' "$AWS_OPERATOR_CIDR"
    printf 'cluster_name=%q\n' "$cluster_name"
    printf 'image_repository_url=%q\n' "$image_repository_url"
    printf 'control_image=%q\n' "$control_image"
    printf 'docker_image=%q\n' "$docker_image"
    printf 'allowed_image_prefixes=%q\n' "$allowed_image_prefixes"
    printf 'kubeconfig_path=%q\n' "$kubeconfig_path"
    printf 'kube_context=%q\n' "$kube_context"
  } > "$runtime_file"
  chmod 600 "$runtime_file"
}

transition_to_workload_runtime() {
  local temporary_runtime_file="$runtime_file.tmp"

  umask 077
  {
    printf 'runtime_phase=workload\n'
    printf 'scale_profile=%q\n' "$scale_profile"
    printf 'karpenter_ami_alias=%q\n' "$karpenter_ami_alias"
    printf 'api_token=%q\n' "$api_token"
    printf 'aws_region=%q\n' "$aws_region"
    printf 'operator_cidr=%q\n' "$AWS_OPERATOR_CIDR"
    printf 'cluster_name=%q\n' "$cluster_name"
    printf 'image_repository_url=%q\n' "$image_repository_url"
    printf 'control_image=%q\n' "$control_image"
    printf 'docker_image=%q\n' "$docker_image"
    printf 'allowed_image_prefixes=%q\n' "$allowed_image_prefixes"
    printf 'kubeconfig_path=%q\n' "$kubeconfig_path"
    printf 'kube_context=%q\n' "$kube_context"
  } > "$temporary_runtime_file"
  chmod 600 "$temporary_runtime_file"
  mv -f -- "$temporary_runtime_file" "$runtime_file"
  runtime_phase="workload"
}

load_runtime_file() {
  [[ -f "$runtime_file" ]] || fail "Runtime file not found: $runtime_file. Run deploy first."
  # shellcheck disable=SC1090
  source "$runtime_file"
  configure_scale_profile
  [[ "${runtime_phase:-}" == "foundation" || "${runtime_phase:-}" == "workload" ]] || fail \
    "Runtime deployment phase is invalid."
  [[ -n "${cluster_name:-}" ]] || fail "Runtime cluster name is invalid."
  [[ -n "${image_repository_url:-}" ]] || fail "Runtime image repository is invalid."
  [[ "${kubeconfig_path:-}" == "$runtime_dir/kubeconfig" ]] || fail "Runtime kubeconfig path is invalid."
  [[ "${kube_context:-}" == "$deployment_name" ]] || fail "Runtime kubeconfig context is invalid."
  export KUBECONFIG="$kubeconfig_path"
}

require_workload_runtime() {
  [[ "$runtime_phase" == "workload" ]] || fail "Workload runtime metadata is not available."
  [[ "${api_token:-}" =~ ^[0-9a-f]{64}$ ]] || fail "The runtime API token is invalid."
  [[ -n "${control_image:-}" ]] || fail "Runtime control image is invalid."
  [[ -n "${docker_image:-}" ]] || fail "Runtime Docker image is invalid."
  [[ -n "${allowed_image_prefixes:-}" ]] || fail "Runtime image prefixes are invalid."
}
