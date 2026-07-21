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

locals {
  control_labels = {
    "app.kubernetes.io/name" = "kubernetes-sandbox-control"
  }
}

resource "helm_release" "cilium" {
  name       = "cilium"
  repository = "https://helm.cilium.io/"
  chart      = "cilium"
  version    = "1.19.6"
  namespace  = "kube-system"

  wait            = true
  atomic          = true
  timeout         = 900
  cleanup_on_fail = true

  values = [yamlencode({
    cni = {
      chainingMode = "aws-cni"
      exclusive    = false
    }
    enableIPv4Masquerade = false
    routingMode          = "native"
    operator = {
      replicas = 1
    }
  })]
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
    name = "runc"
  }

  handler = "runc"
}

resource "kubernetes_resource_quota_v1" "sandboxes" {
  metadata {
    name      = "sandbox-quota"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  spec {
    hard = {
      pods                         = "20"
      "requests.cpu"               = "8"
      "limits.cpu"                 = "8"
      "requests.memory"            = "16Gi"
      "limits.memory"              = "16Gi"
      "requests.ephemeral-storage" = "100Gi"
      "limits.ephemeral-storage"   = "100Gi"
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
    verbs      = ["get", "list", "create", "patch", "delete"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "list"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods/exec"]
    verbs      = ["get", "create"]
  }

  rule {
    api_groups = ["networking.k8s.io"]
    resources  = ["networkpolicies"]
    verbs      = ["get", "create", "update", "delete"]
  }

  rule {
    api_groups = ["cilium.io"]
    resources  = ["ciliumnetworkpolicies"]
    verbs      = ["get", "create", "update", "delete"]
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

resource "kubernetes_secret_v1" "control" {
  metadata {
    name      = "kubernetes-sandbox-control"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  data_wo = {
    KUBERNETES_SANDBOX_API_TOKEN = var.api_token
  }
  data_wo_revision = 1
  type             = "Opaque"
}

resource "kubernetes_deployment_v1" "control" {
  metadata {
    name      = "kubernetes-sandbox-control"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
    labels    = local.control_labels
  }

  spec {
    replicas = 1

    strategy {
      type = "RollingUpdate"
      rolling_update {
        max_unavailable = "0"
        max_surge       = "1"
      }
    }

    selector {
      match_labels = local.control_labels
    }

    template {
      metadata {
        labels = local.control_labels
      }

      spec {
        service_account_name = kubernetes_service_account_v1.control.metadata[0].name
        runtime_class_name   = kubernetes_runtime_class_v1.runc.metadata[0].name

        security_context {
          run_as_non_root = true
          run_as_user     = 10001
          run_as_group    = 10001
          fs_group        = 10001

          seccomp_profile {
            type = "RuntimeDefault"
          }
        }

        container {
          name  = "control"
          image = var.control_image

          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = true

            capabilities {
              drop = ["ALL"]
            }
          }

          port {
            name           = "http"
            container_port = 8080
            protocol       = "TCP"
          }

          env {
            name = "KUBERNETES_SANDBOX_API_TOKEN"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.control.metadata[0].name
                key  = "KUBERNETES_SANDBOX_API_TOKEN"
              }
            }
          }

          env {
            name  = "KUBERNETES_SANDBOX_NAMESPACE"
            value = kubernetes_namespace_v1.sandboxes.metadata[0].name
          }

          env {
            name  = "KUBERNETES_SANDBOX_RUNTIME_CLASS"
            value = kubernetes_runtime_class_v1.runc.metadata[0].name
          }

          env {
            name  = "KUBERNETES_SANDBOX_DOCKER_IMAGE"
            value = var.docker_image
          }

          env {
            name  = "KUBERNETES_SANDBOX_DOCKER_ENABLED"
            value = "true"
          }

          env {
            name  = "KUBERNETES_SANDBOX_ALLOWED_IMAGE_PREFIXES"
            value = join(",", var.allowed_image_prefixes)
          }

          env {
            name  = "KUBERNETES_SANDBOX_REQUIRE_IMAGE_DIGEST"
            value = "true"
          }

          env {
            name  = "KUBERNETES_SANDBOX_MAX_VCPU"
            value = "2"
          }

          env {
            name  = "KUBERNETES_SANDBOX_MAX_MEMORY_GIB"
            value = "4"
          }

          env {
            name  = "KUBERNETES_SANDBOX_MAX_DISK_GIB"
            value = "20"
          }

          env {
            name  = "KUBERNETES_SANDBOX_MAX_GPU"
            value = "0"
          }

          env {
            name  = "KUBERNETES_SANDBOX_MAX_CREATE_TIMEOUT_SECONDS"
            value = "600"
          }

          env {
            name  = "KUBERNETES_SANDBOX_JANITOR_INTERVAL_SECONDS"
            value = "60"
          }

          env {
            name  = "KUBERNETES_SANDBOX_PORT"
            value = "8080"
          }

          env {
            name  = "XDG_CACHE_HOME"
            value = "/home/sandbox-control/.cache"
          }

          volume_mount {
            name       = "tmp"
            mount_path = "/tmp"
          }

          volume_mount {
            name       = "python-cache"
            mount_path = "/home/sandbox-control/.cache"
          }

          readiness_probe {
            http_get {
              path   = "/health"
              port   = "http"
              scheme = "HTTP"
            }

            initial_delay_seconds = 5
            period_seconds        = 10
            timeout_seconds       = 2
            failure_threshold     = 3
          }

          liveness_probe {
            http_get {
              path   = "/health"
              port   = "http"
              scheme = "HTTP"
            }

            initial_delay_seconds = 10
            period_seconds        = 10
            timeout_seconds       = 2
            failure_threshold     = 3
          }
        }

        volume {
          name = "tmp"
          empty_dir {}
        }

        volume {
          name = "python-cache"
          empty_dir {}
        }
      }
    }
  }

  depends_on = [helm_release.cilium]
}

resource "kubernetes_service_v1" "control" {
  metadata {
    name      = "kubernetes-sandbox-control"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  spec {
    selector = local.control_labels
    type     = "ClusterIP"

    port {
      name        = "http"
      port        = 8080
      target_port = "http"
      protocol    = "TCP"
    }
  }
}
