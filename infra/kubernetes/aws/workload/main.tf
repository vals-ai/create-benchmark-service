provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.kubeconfig_context
}

provider "helm" {
  kubernetes = {
    config_path    = var.kubeconfig_path
    config_context = var.kubeconfig_context
  }
}

module "sandbox_workload" {
  source = "../../modules/sandbox-workload"

  sandbox_node_selector = {
    "karpenter.sh/nodepool" = "sandbox"
  }
  sandbox_pod_annotations = {
    "karpenter.sh/do-not-disrupt" = "true"
  }
  runtime_class_name    = "runc"
  runtime_class_handler = "runc"
  egress_driver         = "cilium"
  egress_rbac_rules = [{
    api_groups = ["cilium.io"]
    resources  = ["ciliumnetworkpolicies"]
  }]

  control_image                   = var.control_image
  docker_image                    = var.docker_image
  allowed_image_prefixes          = var.allowed_image_prefixes
  require_image_digest            = var.require_image_digest
  control_replicas                = var.control_replicas
  control_cpu_request             = var.control_cpu_request
  control_memory_request          = var.control_memory_request
  control_cpu_limit               = var.control_cpu_limit
  control_memory_limit            = var.control_memory_limit
  namespace_pod_quota             = var.namespace_pod_quota
  namespace_cpu_quota             = var.namespace_cpu_quota
  namespace_cpu_limit_quota       = var.namespace_cpu_limit_quota
  namespace_memory_quota          = var.namespace_memory_quota
  namespace_memory_limit_quota    = var.namespace_memory_limit_quota
  namespace_storage_quota         = var.namespace_storage_quota
  namespace_storage_limit_quota   = var.namespace_storage_limit_quota
  command_heartbeat_seconds       = var.command_heartbeat_seconds
  activity_write_interval_seconds = var.activity_write_interval_seconds
  exec_connection_pool_size       = var.exec_connection_pool_size
  agent_port                      = var.agent_port
  api_token                       = var.api_token

  depends_on = [helm_release.cilium, helm_release.karpenter_capacity]
}
