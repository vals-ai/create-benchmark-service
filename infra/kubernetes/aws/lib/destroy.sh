tagged_resource_is_live() {
  local tagged_arn="$1"
  local resource_path
  local resource_type
  local resource_id
  local resource_value

  [[ "$tagged_arn" == arn:aws:ec2:* ]] || return 0
  resource_path="${tagged_arn##*:}"
  [[ "$resource_path" == */* ]] || return 0
  resource_type="${resource_path%%/*}"
  resource_id="${resource_path#*/}"

  case "$resource_type" in
    instance)
      resource_value="$(aws ec2 describe-instances \
        --profile vals-dev \
        --region "$aws_region" \
        --filters "Name=instance-id,Values=$resource_id" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" && "$resource_value" != "terminated" ]]
      ;;
    natgateway)
      resource_value="$(aws ec2 describe-nat-gateways \
        --profile vals-dev \
        --region "$aws_region" \
        --filter "Name=nat-gateway-id,Values=$resource_id" \
        --query 'NatGateways[0].State' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" && "$resource_value" != "deleted" ]]
      ;;
    vpc-endpoint)
      resource_value="$(aws ec2 describe-vpc-endpoints \
        --profile vals-dev \
        --region "$aws_region" \
        --filters "Name=vpc-endpoint-id,Values=$resource_id" \
        --query 'VpcEndpoints[0].VpcEndpointId' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" ]]
      ;;
    vpc)
      resource_value="$(aws ec2 describe-vpcs \
        --profile vals-dev \
        --region "$aws_region" \
        --filters "Name=vpc-id,Values=$resource_id" \
        --query 'Vpcs[0].VpcId' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" ]]
      ;;
    subnet)
      resource_value="$(aws ec2 describe-subnets \
        --profile vals-dev \
        --region "$aws_region" \
        --filters "Name=subnet-id,Values=$resource_id" \
        --query 'Subnets[0].SubnetId' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" ]]
      ;;
    volume)
      resource_value="$(aws ec2 describe-volumes \
        --profile vals-dev \
        --region "$aws_region" \
        --filters "Name=volume-id,Values=$resource_id" \
        --query 'Volumes[0].VolumeId' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" ]]
      ;;
    security-group)
      resource_value="$(aws ec2 describe-security-groups \
        --profile vals-dev \
        --region "$aws_region" \
        --filters "Name=group-id,Values=$resource_id" \
        --query 'SecurityGroups[0].GroupId' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" ]]
      ;;
    network-interface)
      resource_value="$(aws ec2 describe-network-interfaces \
        --profile vals-dev \
        --region "$aws_region" \
        --filters "Name=network-interface-id,Values=$resource_id" \
        --query 'NetworkInterfaces[0].NetworkInterfaceId' \
        --output text)" || fail "Unable to verify tagged AWS resource $tagged_arn."
      [[ -n "$resource_value" && "$resource_value" != "None" ]]
      ;;
    *) return 0 ;;
  esac
}

destroy_foundation() {
  local attempt

  for ((attempt = 1; attempt <= foundation_destroy_max_attempts; attempt++)); do
    if TF_VAR_aws_account_id="$AWS_ACCOUNT_ID" \
      TF_VAR_aws_profile="vals-dev" \
      TF_VAR_aws_region="$aws_region" \
      TF_VAR_deployment_name="$deployment_name" \
      TF_VAR_operator_cidr="$operator_cidr" \
      TF_VAR_system_node_min_size="$system_node_min_size" \
      TF_VAR_system_node_max_size="$system_node_max_size" \
      TF_VAR_system_node_desired_size="$system_node_desired_size" \
      TF_VAR_coredns_replica_count="$coredns_replica_count" \
      TF_VAR_create_spot_service_linked_role="$create_spot_service_linked_role" \
      TF_VAR_nat_gateway_per_az="$nat_gateway_per_az" \
      terraform -chdir="$foundation_root" destroy \
        -auto-approve \
        -state="$foundation_state"; then
      return 0
    fi

    if [[ "$attempt" == "$foundation_destroy_max_attempts" ]]; then
      return 1
    fi

    printf 'Foundation destroy attempt %s failed; retrying in %s seconds.\n' \
      "$attempt" "$destroy_retry_delay_seconds" >&2
    sleep "$destroy_retry_delay_seconds"
  done
}

destroy_stack() {
  local namespace_name
  local tagged_arns
  local tagged_arn
  local -a live_tagged_arns=()
  local expected_state_dir="$script_dir/.state/$deployment_name"
  local expected_runtime_dir="$script_dir/.runtime/$deployment_name"

  load_runtime_file
  initialize_and_validate_roots

  if [[ "$runtime_phase" == "workload" ]]; then
    require_workload_runtime
    run_workload_terraform destroy \
      -auto-approve \
      -state="$workload_state"

    if ! namespace_name="$(kubectl --context "$kube_context" get namespace benchmark-sandboxes \
      --ignore-not-found -o name)"; then
      fail "Unable to verify that namespace benchmark-sandboxes was removed."
    fi
    [[ -z "$namespace_name" ]] || fail "Namespace benchmark-sandboxes still exists after workload destroy."
    transition_to_foundation_runtime
  fi

  destroy_foundation

  tagged_arns="$(aws resourcegroupstaggingapi get-resources \
    --profile vals-dev \
    --region "$aws_region" \
    --tag-filters \
      'Key=Project,Values=create-benchmark-service-kubernetes' \
      "Key=Deployment,Values=$deployment_name" \
    --query 'ResourceTagMappingList[].ResourceARN' \
    --output text)"
  if [[ -n "$tagged_arns" && "$tagged_arns" != "None" ]]; then
    for tagged_arn in $tagged_arns; do
      if tagged_resource_is_live "$tagged_arn"; then
        live_tagged_arns+=("$tagged_arn")
      fi
    done
  fi
  [[ "${#live_tagged_arns[@]}" == "0" ]] || fail \
    "Live tagged AWS resources remain after destroy: ${live_tagged_arns[*]}."

  [[ "$state_dir" == "$expected_state_dir" ]] || fail "Refusing to clean an unexpected state directory."
  [[ "$runtime_dir" == "$expected_runtime_dir" ]] || fail "Refusing to clean an unexpected runtime directory."
  rm -rf -- "$runtime_dir" "$state_dir"
}
