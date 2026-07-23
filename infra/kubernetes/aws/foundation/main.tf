provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = local.tags
  }
}

check "system_node_sizes" {
  assert {
    condition = (
      var.system_node_min_size <= var.system_node_desired_size
      && var.system_node_desired_size <= var.system_node_max_size
    )
    error_message = "System node sizes must satisfy min <= desired <= max."
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  cluster_name = "${var.deployment_name}-eks"
  availability_zones = slice(
    data.aws_availability_zones.available.names,
    0,
    3,
  )
  tags = {
    Project    = "create-benchmark-service-kubernetes"
    Deployment = var.deployment_name
    ManagedBy  = "Terraform"
  }
}

resource "aws_iam_service_linked_role" "ec2_spot" {
  count = var.create_spot_service_linked_role ? 1 : 0

  aws_service_name = "spot.amazonaws.com"
  description      = "Allows EC2 to launch and manage Spot Instances for the disposable EKS sandbox stack"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "6.6.1"

  name = "${var.deployment_name}-vpc"
  cidr = "10.42.0.0/16"

  azs             = local.availability_zones
  private_subnets = ["10.42.0.0/19", "10.42.32.0/19", "10.42.64.0/19"]
  public_subnets  = ["10.42.96.0/24", "10.42.97.0/24", "10.42.98.0/24"]

  enable_nat_gateway     = true
  single_nat_gateway     = !var.nat_gateway_per_az
  one_nat_gateway_per_az = var.nat_gateway_per_az
  enable_dns_support     = true
  enable_dns_hostnames   = true

  private_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"             = "1"
    "karpenter.sh/discovery"                      = local.cluster_name
  }
  public_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                      = "1"
  }

  tags = local.tags
}

resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "${var.deployment_name}-endpoints-"
  description = "HTTPS access to private AWS service endpoints"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "HTTPS from the sandbox VPC"
    protocol    = "tcp"
    from_port   = 443
    to_port     = 443
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }

  tags = local.tags
}

module "vpc_endpoints" {
  source  = "terraform-aws-modules/vpc/aws//modules/vpc-endpoints"
  version = "6.6.1"

  vpc_id = module.vpc.vpc_id
  endpoints = {
    s3 = {
      service         = "s3"
      service_type    = "Gateway"
      route_table_ids = module.vpc.private_route_table_ids
    }
    ecr_api = {
      service             = "ecr.api"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
      security_group_ids  = [aws_security_group.vpc_endpoints.id]
    }
    ecr_dkr = {
      service             = "ecr.dkr"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
      security_group_ids  = [aws_security_group.vpc_endpoints.id]
    }
    logs = {
      service             = "logs"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
      security_group_ids  = [aws_security_group.vpc_endpoints.id]
    }
    sts = {
      service             = "sts"
      private_dns_enabled = true
      subnet_ids          = module.vpc.private_subnets
      security_group_ids  = [aws_security_group.vpc_endpoints.id]
    }
  }

  tags = local.tags
}

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

module "karpenter" {
  source  = "terraform-aws-modules/eks/aws//modules/karpenter"
  version = "21.24.0"

  cluster_name = module.eks.cluster_name

  create_pod_identity_association = true
  enable_inline_policy            = true
  iam_role_use_name_prefix        = false
  iam_role_name                   = "${local.cluster_name}-karpenter-controller"
  node_iam_role_use_name_prefix   = false
  node_iam_role_name              = "${local.cluster_name}-karpenter"
  queue_name                      = "${local.cluster_name}-karpenter"

  node_iam_role_additional_policies = {
    AmazonSSMManagedInstanceCore = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  }

  tags = local.tags
}

resource "aws_ecr_repository" "sandbox" {
  name                 = "${var.deployment_name}/sandbox-images"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}
