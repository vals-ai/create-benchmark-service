provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = local.tags
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
    2,
  )
  tags = {
    Project    = "create-benchmark-service-kubernetes"
    Deployment = var.deployment_name
    ManagedBy  = "Terraform"
  }
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "6.6.1"

  name = "${var.deployment_name}-vpc"
  cidr = "10.42.0.0/16"

  azs             = local.availability_zones
  private_subnets = ["10.42.0.0/20", "10.42.16.0/20"]
  public_subnets  = ["10.42.32.0/24", "10.42.33.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_support   = true
  enable_dns_hostnames = true

  private_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/internal-elb"             = "1"
  }
  public_subnet_tags = {
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
    "kubernetes.io/role/elb"                      = "1"
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
    coredns    = {}
    kube-proxy = {}
    vpc-cni = {
      before_compute = true
    }
  }

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    sandbox = {
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

      min_size     = 1
      max_size     = 2
      desired_size = 1

      taints = {
        cilium = {
          key    = "node.cilium.io/agent-not-ready"
          value  = "true"
          effect = "NO_EXECUTE"
        }
      }
    }
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
