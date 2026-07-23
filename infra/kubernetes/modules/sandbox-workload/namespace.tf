locals {
  control_labels = {
    "app.kubernetes.io/name" = "kubernetes-sandbox-control"
  }
}

resource "kubernetes_namespace_v1" "sandboxes" {
  metadata {
    name = "benchmark-sandboxes"
    labels = {
      "pod-security.kubernetes.io/enforce" = "privileged"
      "pod-security.kubernetes.io/audit"   = "restricted"
      "pod-security.kubernetes.io/warn"    = "restricted"
    }
  }
}

resource "kubernetes_runtime_class_v1" "runc" {
  metadata {
    name = var.runtime_class_name
  }

  handler = var.runtime_class_handler
}

resource "kubernetes_resource_quota_v1" "sandboxes" {
  metadata {
    name      = "sandbox-quota"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  spec {
    hard = {
      pods                         = tostring(var.namespace_pod_quota)
      "requests.cpu"               = var.namespace_cpu_quota
      "limits.cpu"                 = var.namespace_cpu_limit_quota
      "requests.memory"            = var.namespace_memory_quota
      "limits.memory"              = var.namespace_memory_limit_quota
      "requests.ephemeral-storage" = var.namespace_storage_quota
      "limits.ephemeral-storage"   = var.namespace_storage_limit_quota
    }
  }
}

resource "kubernetes_limit_range_v1" "sandboxes" {
  metadata {
    name      = "sandbox-defaults"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  spec {
    limit {
      type = "Container"
      default = {
        cpu                 = "2"
        memory              = "4Gi"
        "ephemeral-storage" = "20Gi"
      }
      default_request = {
        cpu                 = "250m"
        memory              = "256Mi"
        "ephemeral-storage" = "1Gi"
      }
    }
  }
}

resource "kubernetes_service_account_v1" "control" {
  metadata {
    name      = "kubernetes-sandbox-control"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }
}

resource "kubernetes_role_v1" "control" {
  metadata {
    name      = "kubernetes-sandbox-control"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  rule {
    api_groups = ["batch"]
    resources  = ["jobs"]
    verbs      = ["get", "list", "watch", "create", "patch", "delete"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "list", "watch"]
  }

  dynamic "rule" {
    for_each = var.egress_rbac_rules

    content {
      api_groups = rule.value.api_groups
      resources  = rule.value.resources
      verbs      = ["get", "create", "update", "delete"]
    }
  }
}

resource "kubernetes_role_binding_v1" "control" {
  metadata {
    name      = "kubernetes-sandbox-control"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.control.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.control.metadata[0].name
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }
}

resource "kubernetes_network_policy_v1" "sandbox_ingress" {
  metadata {
    name      = "sandbox-ingress"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/managed-by" = "benchmark-sandbox-control"
      }
    }

    policy_types = ["Ingress"]

    ingress {
      from {
        pod_selector {
          match_labels = local.control_labels
        }
      }

      ports {
        port     = tostring(var.agent_port)
        protocol = "TCP"
      }
    }
  }
}
