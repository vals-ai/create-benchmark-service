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
    replicas = var.control_replicas

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

        topology_spread_constraint {
          max_skew           = 1
          topology_key       = "topology.kubernetes.io/zone"
          when_unsatisfiable = "ScheduleAnyway"

          label_selector {
            match_labels = local.control_labels
          }
        }

        topology_spread_constraint {
          max_skew           = 1
          topology_key       = "kubernetes.io/hostname"
          when_unsatisfiable = "ScheduleAnyway"

          label_selector {
            match_labels = local.control_labels
          }
        }

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

          resources {
            requests = {
              cpu    = var.control_cpu_request
              memory = var.control_memory_request
            }
            limits = {
              cpu    = var.control_cpu_limit
              memory = var.control_memory_limit
            }
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
            name  = "KUBERNETES_SANDBOX_AGENT_IMAGE"
            value = var.control_image
          }

          env {
            name  = "KUBERNETES_SANDBOX_AGENT_PORT"
            value = tostring(var.agent_port)
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
            value = tostring(var.require_image_digest)
          }

          env {
            name  = "KUBERNETES_SANDBOX_NODE_SELECTOR"
            value = join(",", [for key, value in var.sandbox_node_selector : "${key}=${value}"])
          }

          env {
            name  = "KUBERNETES_SANDBOX_POD_ANNOTATIONS"
            value = join(",", [for key, value in var.sandbox_pod_annotations : "${key}=${value}"])
          }

          env {
            name  = "KUBERNETES_SANDBOX_EGRESS_DRIVER"
            value = var.egress_driver
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
            name  = "KUBERNETES_SANDBOX_COMMAND_HEARTBEAT_SECONDS"
            value = tostring(var.command_heartbeat_seconds)
          }

          env {
            name  = "KUBERNETES_SANDBOX_ACTIVITY_WRITE_INTERVAL_SECONDS"
            value = tostring(var.activity_write_interval_seconds)
          }

          env {
            name  = "KUBERNETES_SANDBOX_EXEC_CONNECTION_POOL_SIZE"
            value = tostring(var.exec_connection_pool_size)
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
              path   = "/ready"
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
}

resource "kubernetes_pod_disruption_budget_v1" "control" {
  metadata {
    name      = "kubernetes-sandbox-control"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  spec {
    max_unavailable = "1"

    selector {
      match_labels = local.control_labels
    }
  }
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
