check "system_node_sizes" {
  assert {
    condition = (
      var.system_node_min_size <= var.system_node_desired_size
      && var.system_node_desired_size <= var.system_node_max_size
    )
    error_message = "System node sizes must satisfy min <= desired <= max."
  }
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
