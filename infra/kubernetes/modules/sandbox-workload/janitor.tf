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
                name  = "KUBERNETES_SANDBOX_EGRESS_DRIVER"
                value = var.egress_driver
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
