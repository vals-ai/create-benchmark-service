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
