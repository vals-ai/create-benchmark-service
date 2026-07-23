module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "21.24.0"

  name               = local.cluster_name
  kubernetes_version = var.kubernetes_version

  endpoint_private_access                  = true
  endpoint_public_access                   = true
  endpoint_public_access_cidrs             = [var.operator_cidr]
  deletion_protection                      = false
  enable_cluster_creator_admin_permissions = true
  encryption_config                        = null

  addons = {
    coredns = {
      configuration_values = jsonencode({
        replicaCount = var.coredns_replica_count
      })
    }
    eks-pod-identity-agent = {
      before_compute = true
    }
    kube-proxy = {}
    vpc-cni = {
      before_compute = true
      configuration_values = jsonencode({
        env = {
          ENABLE_PREFIX_DELEGATION = "true"
          WARM_PREFIX_TARGET       = "1"
        }
      })
    }
  }

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    system = {
      ami_type       = "AL2023_x86_64_STANDARD"
      capacity_type  = "ON_DEMAND"
      instance_types = var.node_instance_types

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            delete_on_termination = true
            encrypted             = true
            volume_size           = 100
            volume_type           = "gp3"
          }
        }
      }

      min_size     = var.system_node_min_size
      max_size     = var.system_node_max_size
      desired_size = var.system_node_desired_size

      labels = {
        "karpenter.sh/controller" = "true"
        "vals.ai/node-pool"       = "system"
      }
    }
  }

  node_security_group_tags = {
    "karpenter.sh/discovery" = local.cluster_name
  }

  node_security_group_additional_rules = {
    ingress_nodes_wireguard = {
      description = "Cilium WireGuard between cluster nodes"
      protocol    = "udp"
      from_port   = 51871
      to_port     = 51871
      type        = "ingress"
      self        = true
    }
  }

  tags = local.tags
}
