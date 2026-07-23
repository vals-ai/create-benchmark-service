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
  chart     = "${path.module}/../../charts/karpenter-sandbox"
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
