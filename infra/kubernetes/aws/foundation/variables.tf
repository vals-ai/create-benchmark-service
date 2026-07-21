variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "aws_profile" {
  type    = string
  default = "vals-dev"

  validation {
    condition     = var.aws_profile == "vals-dev"
    error_message = "aws_profile must be vals-dev for this disposable smoke."
  }
}

variable "aws_account_id" {
  type = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be a 12-digit account ID."
  }
}

variable "deployment_name" {
  type    = string
  default = "cbs-kubernetes-smoke"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,31}$", var.deployment_name))
    error_message = "deployment_name must be a 3-32 character lowercase DNS label."
  }
}

variable "operator_cidr" {
  type = string

  validation {
    condition     = can(cidrhost(var.operator_cidr, 0)) && tonumber(split("/", var.operator_cidr)[1]) >= 24
    error_message = "operator_cidr must be an IPv4 CIDR no broader than /24."
  }
}

variable "kubernetes_version" {
  type    = string
  default = "1.35"
}

variable "node_instance_types" {
  type    = list(string)
  default = ["m6i.xlarge"]
}
