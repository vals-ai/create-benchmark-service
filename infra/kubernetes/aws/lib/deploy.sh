initialize_and_validate_roots() {
  terraform -chdir="$foundation_root" init -backend=false -input=false
  terraform -chdir="$foundation_root" validate
  terraform -chdir="$workload_root" init -backend=false -input=false
  terraform -chdir="$workload_root" validate
}

run_workload_terraform() {
  local operation="$1"
  shift

  TF_VAR_kubeconfig_path="$kubeconfig_path" \
    TF_VAR_kubeconfig_context="$kube_context" \
    TF_VAR_cluster_name="$cluster_name" \
    TF_VAR_karpenter_queue_name="$cluster_name-karpenter" \
    TF_VAR_karpenter_node_iam_role_name="$cluster_name-karpenter" \
    TF_VAR_karpenter_ami_alias="$karpenter_ami_alias" \
    TF_VAR_karpenter_capacity_types="$karpenter_capacity_types" \
    TF_VAR_control_image="$control_image" \
    TF_VAR_docker_image="$docker_image" \
    TF_VAR_allowed_image_prefixes="$allowed_image_prefixes" \
    TF_VAR_control_replicas="$control_replicas" \
    TF_VAR_activity_write_interval_seconds="$activity_write_interval_seconds" \
    TF_VAR_exec_connection_pool_size="$exec_connection_pool_size" \
    TF_VAR_namespace_pod_quota="$namespace_pod_quota" \
    TF_VAR_namespace_cpu_quota="$namespace_cpu_quota" \
    TF_VAR_namespace_cpu_limit_quota="$namespace_cpu_limit_quota" \
    TF_VAR_namespace_memory_quota="$namespace_memory_quota" \
    TF_VAR_namespace_memory_limit_quota="$namespace_memory_limit_quota" \
    TF_VAR_namespace_storage_quota="$namespace_storage_quota" \
    TF_VAR_namespace_storage_limit_quota="$namespace_storage_limit_quota" \
    TF_VAR_karpenter_cpu_limit="$karpenter_cpu_limit" \
    TF_VAR_karpenter_memory_limit="$karpenter_memory_limit" \
    TF_VAR_karpenter_root_volume_size="$karpenter_root_volume_size" \
    TF_VAR_api_token="$api_token" \
    terraform -chdir="$workload_root" "$operation" "$@"
}

plan_foundation() {
  check_scale_quotas
  mkdir -p "$state_dir"
  initialize_and_validate_roots
  terraform -chdir="$foundation_root" plan \
    -state="$foundation_state" \
    -out="$foundation_plan" \
    -var="aws_account_id=$AWS_ACCOUNT_ID" \
    -var="aws_profile=vals-dev" \
    -var="aws_region=$aws_region" \
    -var="deployment_name=$deployment_name" \
    -var="operator_cidr=$AWS_OPERATOR_CIDR" \
    -var="system_node_min_size=$system_node_min_size" \
    -var="system_node_max_size=$system_node_max_size" \
    -var="system_node_desired_size=$system_node_desired_size" \
    -var="coredns_replica_count=$coredns_replica_count" \
    -var="create_spot_service_linked_role=$create_spot_service_linked_role" \
    -var="nat_gateway_per_az=$nat_gateway_per_az"
  write_plan_inputs
  printf '%s\n' \
    "Foundation plan saved. The workload plan is created after foundation apply because its Kubernetes provider needs the live API endpoint."
}

ecr_image_exists() {
  local repository_name="$1"
  local image_tag="$2"

  aws ecr describe-images \
    --profile vals-dev \
    --region "$aws_region" \
    --repository-name "$repository_name" \
    --image-ids "imageTag=$image_tag" >/dev/null 2>&1
}

resolve_ecr_digest() {
  local repository_name="$1"
  local image_tag="$2"
  local digest

  digest="$(aws ecr describe-images \
    --profile vals-dev \
    --region "$aws_region" \
    --repository-name "$repository_name" \
    --image-ids "imageTag=$image_tag" \
    --query 'imageDetails[0].imageDigest' \
    --output text)"
  [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail \
    "ECR returned an invalid digest for $image_tag: $digest."

  printf '%s\n' "$digest"
}

deploy_stack() {
  local repository_name
  local registry_host
  local commit_sha
  local control_source_hash
  local control_tag
  local dind_tag="dind-28.3.3"
  local control_tagged_image
  local dind_tagged_image
  local control_digest
  local dind_digest
  local cilium_status

  [[ -f "$foundation_plan" ]] || fail "Foundation plan not found. Run plan first."
  require_value TEST_KUBERNETES_IMAGE
  [[ "$TEST_KUBERNETES_IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$ ]] || fail \
    "TEST_KUBERNETES_IMAGE must be a sha256 digest reference."
  require_unchanged_plan_inputs
  initialize_and_validate_roots

  cluster_name="$deployment_name-eks"
  image_repository_url="$AWS_ACCOUNT_ID.dkr.ecr.$aws_region.amazonaws.com/$deployment_name/sandbox-images"
  write_foundation_runtime_file

  terraform -chdir="$foundation_root" apply \
    -state="$foundation_state" \
    "$foundation_plan"

  cluster_name="$(terraform -chdir="$foundation_root" output -state="$foundation_state" -raw cluster_name)"
  image_repository_url="$(terraform -chdir="$foundation_root" output -state="$foundation_state" -raw image_repository_url)"
  if [[ "$runtime_phase" == "foundation" ]]; then
    write_foundation_runtime_file
  fi
  repository_name="${image_repository_url#*/}"
  registry_host="${image_repository_url%%/*}"

  aws ecr get-login-password --profile vals-dev --region "$aws_region" \
    | docker login --username AWS --password-stdin "$registry_host"

  commit_sha="$(git -C "$repository_root" rev-parse --short=12 HEAD)"
  control_source_hash="$(
    cd "$repository_root"
    {
      printf '%s\n' infra/kubernetes/Dockerfile.control pyproject.toml uv.lock README.md .python-version Makefile
      find src cli templates infra/kubernetes/agent -type f
    } | LC_ALL=C sort | git hash-object --stdin-paths | git hash-object --stdin
  )"
  control_tag="control-$commit_sha-${control_source_hash:0:12}"
  control_tagged_image="$image_repository_url:$control_tag"
  if ! ecr_image_exists "$repository_name" "$control_tag"; then
    docker build \
      --platform linux/amd64 \
      -f "$repository_root/infra/kubernetes/Dockerfile.control" \
      --build-arg "PACKAGE_VERSION=0.0.0+$commit_sha" \
      -t "$control_tagged_image" \
      "$repository_root"
    docker push "$control_tagged_image"
  fi

  dind_tagged_image="$image_repository_url:$dind_tag"
  if ! ecr_image_exists "$repository_name" "$dind_tag"; then
    docker pull --platform linux/amd64 "$dind_source_image"
    docker tag "$dind_source_image" "$dind_tagged_image"
    docker push "$dind_tagged_image"
  fi

  control_digest="$(resolve_ecr_digest "$repository_name" "$control_tag")"
  dind_digest="$(resolve_ecr_digest "$repository_name" "$dind_tag")"
  control_image="$image_repository_url@$control_digest"
  docker_image="$image_repository_url@$dind_digest"

  write_runtime_file
  aws eks update-kubeconfig \
    --profile vals-dev \
    --region "$aws_region" \
    --name "$cluster_name" \
    --kubeconfig "$kubeconfig_path" \
    --alias "$kube_context"
  export KUBECONFIG="$kubeconfig_path"

  run_workload_terraform plan \
    -state="$workload_state" \
    -out="$workload_plan"
  transition_to_workload_runtime
  run_workload_terraform apply \
    -state="$workload_state" \
    "$workload_plan"

  kubectl --context "$kube_context" -n kube-system \
    rollout status daemonset/cilium --timeout=15m
  cilium_status="$(kubectl --context "$kube_context" -n kube-system \
    exec daemonset/cilium -c cilium-agent -- cilium-dbg status)"
  [[ "$cilium_status" =~ Encryption:[[:space:]]+Wireguard ]] || fail \
    "Cilium did not report WireGuard encryption as enabled."
  kubectl --context "$kube_context" -n benchmark-sandboxes \
    rollout status deployment/kubernetes-sandbox-control --timeout=15m
}

port_forward() {
  load_runtime_file
  require_workload_runtime
  kubectl --context "$kube_context" -n benchmark-sandboxes \
    port-forward service/kubernetes-sandbox-control 8080:8080
}

run_live_test() {
  local port_forward_pid=""
  local attempt
  local test_status=0

  require_value TEST_KUBERNETES_IMAGE
  load_runtime_file
  require_workload_runtime
  kubectl --context "$kube_context" -n benchmark-sandboxes \
    port-forward service/kubernetes-sandbox-control 8080:8080 &
  port_forward_pid=$!

  cleanup_port_forward() {
    kill "$port_forward_pid" 2>/dev/null || true
    wait "$port_forward_pid" 2>/dev/null || true
  }
  trap cleanup_port_forward EXIT INT TERM

  for attempt in $(seq 1 30); do
    if curl --fail --silent --show-error http://127.0.0.1:8080/ready >/dev/null; then
      kill -0 "$port_forward_pid" 2>/dev/null || fail "The kubectl port-forward process exited."
      break
    fi
    if [[ "$attempt" == "30" ]]; then
      fail "The Kubernetes control service did not become ready."
    fi
    sleep 2
  done

  export TEST_KUBERNETES_CONTROL_URL="http://127.0.0.1:8080"
  export TEST_KUBERNETES_CONTROL_TOKEN="$api_token"
  export TEST_KUBERNETES_IMAGE
  export TEST_KUBERNETES_COMPOSE_IMAGE="$docker_image"
  uv run pytest "$repository_root/tests/integration/test_kubernetes_control_service.py" -q || test_status=$?
  cleanup_port_forward
  trap - EXIT INT TERM
  return "$test_status"
}
