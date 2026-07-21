output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  value     = module.eks.cluster_certificate_authority_data
  sensitive = true
}

output "image_repository_url" {
  value = aws_ecr_repository.sandbox.repository_url
}

output "aws_region" {
  value = var.aws_region
}

output "deployment_tags" {
  value = local.tags
}
