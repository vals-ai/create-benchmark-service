moved {
  from = kubernetes_namespace_v1.sandboxes
  to   = module.sandbox_workload.kubernetes_namespace_v1.sandboxes
}

moved {
  from = kubernetes_runtime_class_v1.runc
  to   = module.sandbox_workload.kubernetes_runtime_class_v1.runc
}

moved {
  from = kubernetes_resource_quota_v1.sandboxes
  to   = module.sandbox_workload.kubernetes_resource_quota_v1.sandboxes
}

moved {
  from = kubernetes_limit_range_v1.sandboxes
  to   = module.sandbox_workload.kubernetes_limit_range_v1.sandboxes
}

moved {
  from = kubernetes_service_account_v1.control
  to   = module.sandbox_workload.kubernetes_service_account_v1.control
}

moved {
  from = kubernetes_role_v1.control
  to   = module.sandbox_workload.kubernetes_role_v1.control
}

moved {
  from = kubernetes_role_binding_v1.control
  to   = module.sandbox_workload.kubernetes_role_binding_v1.control
}

moved {
  from = kubernetes_network_policy_v1.sandbox_ingress
  to   = module.sandbox_workload.kubernetes_network_policy_v1.sandbox_ingress
}

moved {
  from = kubernetes_secret_v1.control
  to   = module.sandbox_workload.kubernetes_secret_v1.control
}

moved {
  from = kubernetes_deployment_v1.control
  to   = module.sandbox_workload.kubernetes_deployment_v1.control
}

moved {
  from = kubernetes_pod_disruption_budget_v1.control
  to   = module.sandbox_workload.kubernetes_pod_disruption_budget_v1.control
}

moved {
  from = kubernetes_service_v1.control
  to   = module.sandbox_workload.kubernetes_service_v1.control
}

moved {
  from = kubernetes_cron_job_v1.sandbox_janitor
  to   = module.sandbox_workload.kubernetes_cron_job_v1.sandbox_janitor
}
