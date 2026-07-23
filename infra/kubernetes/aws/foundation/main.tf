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
    3,
  )
  tags = {
    Project    = "create-benchmark-service-kubernetes"
    Deployment = var.deployment_name
    ManagedBy  = "Terraform"
  }
}
