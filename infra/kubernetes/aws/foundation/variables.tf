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
    condition     = can(regex("^[a-z][a-z0-9-]{2,24}$", var.deployment_name))
    error_message = "deployment_name must be a 3-25 character lowercase DNS label."
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

variable "system_node_min_size" {
  type = number

  validation {
    condition     = var.system_node_min_size >= 1 && floor(var.system_node_min_size) == var.system_node_min_size
    error_message = "system_node_min_size must be a positive integer."
  }
}

variable "system_node_max_size" {
  type = number

  validation {
    condition     = var.system_node_max_size >= 1 && floor(var.system_node_max_size) == var.system_node_max_size
    error_message = "system_node_max_size must be a positive integer."
  }
}

variable "system_node_desired_size" {
  type = number

  validation {
    condition     = var.system_node_desired_size >= 1 && floor(var.system_node_desired_size) == var.system_node_desired_size
    error_message = "system_node_desired_size must be a positive integer."
  }
}

variable "coredns_replica_count" {
  type    = number
  default = 2

  validation {
    condition     = var.coredns_replica_count >= 2 && floor(var.coredns_replica_count) == var.coredns_replica_count
    error_message = "coredns_replica_count must be an integer of at least 2."
  }
}

variable "nat_gateway_per_az" {
  type    = bool
  default = false
}

variable "create_spot_service_linked_role" {
  type    = bool
  default = false
}
