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
      chainingMode                 = "aws-cni"
      exclusive                    = false
      enableRouteMTUForCNIChaining = true
    }
    enableIPv4Masquerade = false
    routingMode          = "native"
    encryption = {
      enabled = true
      type    = "wireguard"
    }
    operator = {
      replicas = 2
    }
  })]
}

resource "helm_release" "karpenter" {
  name       = "karpenter"
  repository = "oci://public.ecr.aws/karpenter"
  chart      = "karpenter"
  version    = var.karpenter_version
  namespace  = "kube-system"

  wait            = true
  atomic          = true
  timeout         = 900
  cleanup_on_fail = true

  values = [yamlencode({
    dnsPolicy = "Default"
    nodeSelector = {
      "karpenter.sh/controller" = "true"
    }
    controller = {
      resources = {
        requests = {
          cpu    = "500m"
          memory = "512Mi"
        }
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
      }
    }
    settings = {
      clusterName       = var.cluster_name
      eksControlPlane   = true
      interruptionQueue = var.karpenter_queue_name
    }
  })]

  depends_on = [helm_release.cilium]
}

resource "helm_release" "karpenter_capacity" {
  name      = "sandbox-capacity"
  chart     = "${path.module}/../charts/karpenter-sandbox"
  namespace = "kube-system"

  wait            = true
  atomic          = true
  timeout         = 600
  cleanup_on_fail = true

  values = [yamlencode({
    clusterName    = var.cluster_name
    nodeRole       = var.karpenter_node_iam_role_name
    amiAlias       = var.karpenter_ami_alias
    capacityTypes  = var.karpenter_capacity_types
    categories     = var.karpenter_instance_categories
    rootVolumeSize = var.karpenter_root_volume_size
    limits = {
      cpu    = var.karpenter_cpu_limit
      memory = var.karpenter_memory_limit
    }
  })]

  depends_on = [helm_release.karpenter]
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
            value = "karpenter.sh/nodepool=sandbox"
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

  depends_on = [helm_release.cilium, helm_release.karpenter_capacity]
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

resource "kubernetes_cron_job_v1" "sandbox_janitor" {
  metadata {
    name      = "kubernetes-sandbox-janitor"
    namespace = kubernetes_namespace_v1.sandboxes.metadata[0].name
  }

  spec {
    schedule                      = "* * * * *"
    concurrency_policy            = "Forbid"
    starting_deadline_seconds     = 30
    successful_jobs_history_limit = 1
    failed_jobs_history_limit     = 2

    job_template {
      metadata {}

      spec {
        backoff_limit              = 2
        ttl_seconds_after_finished = 300

        template {
          metadata {
            labels = {
              "app.kubernetes.io/name" = "kubernetes-sandbox-janitor"
            }
          }

          spec {
            service_account_name = kubernetes_service_account_v1.control.metadata[0].name
            runtime_class_name   = kubernetes_runtime_class_v1.runc.metadata[0].name
            restart_policy       = "Never"

            security_context {
              run_as_non_root = true
              run_as_user     = 10001
              run_as_group    = 10001

              seccomp_profile {
                type = "RuntimeDefault"
              }
            }

            container {
              name    = "janitor"
              image   = var.control_image
              command = ["kubernetes-sandbox-janitor"]

              security_context {
                allow_privilege_escalation = false
                read_only_root_filesystem  = true

                capabilities {
                  drop = ["ALL"]
                }
              }

              resources {
                requests = {
                  cpu    = "50m"
                  memory = "64Mi"
                }
                limits = {
                  cpu    = "250m"
                  memory = "256Mi"
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
                name  = "KUBERNETES_SANDBOX_DOCKER_IMAGE"
                value = var.docker_image
              }

              env {
                name  = "KUBERNETES_SANDBOX_ACTIVITY_WRITE_INTERVAL_SECONDS"
                value = tostring(var.activity_write_interval_seconds)
              }

              env {
                name  = "XDG_CACHE_HOME"
                value = "/tmp/.cache"
              }

              volume_mount {
                name       = "tmp"
                mount_path = "/tmp"
              }
            }

            volume {
              name = "tmp"
              empty_dir {}
            }
          }
        }
      }
    }
  }
}
