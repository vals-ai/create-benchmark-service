resource "aws_ecr_repository" "sandbox" {
  name                 = "${var.deployment_name}/sandbox-images"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}
