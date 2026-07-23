variable "kubeconfig_path" {
  type = string

  validation {
    condition     = startswith(trimspace(var.kubeconfig_path), "/") && length(trimspace(var.kubeconfig_path)) > 1
    error_message = "kubeconfig_path must be a non-empty absolute path."
  }
}

variable "kubeconfig_context" {
  type = string

  validation {
    condition     = length(trimspace(var.kubeconfig_context)) > 0
    error_message = "kubeconfig_context must not be empty."
  }
}

variable "cluster_name" {
  type = string

  validation {
    condition     = length(trimspace(var.cluster_name)) > 0
    error_message = "cluster_name must not be empty."
  }
}

variable "karpenter_queue_name" {
  type = string

  validation {
    condition     = length(trimspace(var.karpenter_queue_name)) > 0
    error_message = "karpenter_queue_name must not be empty."
  }
}

variable "karpenter_node_iam_role_name" {
  type = string

  validation {
    condition     = length(trimspace(var.karpenter_node_iam_role_name)) > 0
    error_message = "karpenter_node_iam_role_name must not be empty."
  }
}

variable "karpenter_version" {
  type    = string
  default = "1.12.1"
}

variable "karpenter_ami_alias" {
  type    = string
  default = "al2023@latest"

  validation {
    condition     = can(regex("^al2023@(latest|v[0-9]{8})$", var.karpenter_ami_alias))
    error_message = "karpenter_ami_alias must be al2023@latest or al2023@vYYYYMMDD."
  }
}

variable "karpenter_capacity_types" {
  type    = list(string)
  default = ["on-demand"]

  validation {
    condition = length(var.karpenter_capacity_types) > 0 && alltrue([
      for capacity_type in var.karpenter_capacity_types :
      contains(["on-demand", "spot"], capacity_type)
    ])
    error_message = "karpenter_capacity_types must contain on-demand, spot, or both."
  }
}

variable "karpenter_instance_categories" {
  type    = list(string)
  default = ["c", "m", "r"]
}

variable "karpenter_cpu_limit" {
  type    = string
  default = "2500"
}

variable "karpenter_memory_limit" {
  type    = string
  default = "5000Gi"
}

variable "karpenter_root_volume_size" {
  type    = string
  default = "100Gi"
}

variable "control_image" {
  type = string

  validation {
    condition     = can(regex("^[^[:space:]@]+@sha256:[0-9a-f]{64}$", var.control_image))
    error_message = "control_image must be a non-empty sha256 digest reference."
  }
}

variable "docker_image" {
  type = string

  validation {
    condition     = can(regex("^[^[:space:]@]+@sha256:[0-9a-f]{64}$", var.docker_image))
    error_message = "docker_image must be a non-empty sha256 digest reference."
  }
}

variable "allowed_image_prefixes" {
  type = list(string)

  validation {
    condition = length(var.allowed_image_prefixes) > 0 && alltrue([
      for prefix in var.allowed_image_prefixes :
      length(prefix) > 0 && prefix == trimspace(prefix) && !strcontains(prefix, ",")
    ])
    error_message = "allowed_image_prefixes must contain trimmed, non-empty, comma-free prefixes."
  }
}

variable "require_image_digest" {
  type    = bool
  default = true
}

variable "control_replicas" {
  type    = number
  default = 3

  validation {
    condition     = var.control_replicas >= 2 && floor(var.control_replicas) == var.control_replicas
    error_message = "control_replicas must be an integer of at least 2."
  }
}

variable "control_cpu_request" {
  type    = string
  default = "250m"
}

variable "control_memory_request" {
  type    = string
  default = "256Mi"
}

variable "control_cpu_limit" {
  type    = string
  default = "2"
}

variable "control_memory_limit" {
  type    = string
  default = "2Gi"
}

variable "namespace_pod_quota" {
  type    = number
  default = 20

  validation {
    condition     = var.namespace_pod_quota >= 5 && floor(var.namespace_pod_quota) == var.namespace_pod_quota
    error_message = "namespace_pod_quota must be an integer of at least 5."
  }
}

variable "namespace_cpu_quota" {
  type    = string
  default = "8"
}

variable "namespace_cpu_limit_quota" {
  type    = string
  default = "80"
}

variable "namespace_memory_quota" {
  type    = string
  default = "16Gi"
}

variable "namespace_memory_limit_quota" {
  type    = string
  default = "160Gi"
}

variable "namespace_storage_quota" {
  type    = string
  default = "100Gi"
}

variable "namespace_storage_limit_quota" {
  type    = string
  default = "800Gi"
}

variable "command_heartbeat_seconds" {
  type    = number
  default = 15

  validation {
    condition     = var.command_heartbeat_seconds > 0
    error_message = "command_heartbeat_seconds must be positive."
  }
}

variable "activity_write_interval_seconds" {
  type    = number
  default = 30

  validation {
    condition     = var.activity_write_interval_seconds > 0
    error_message = "activity_write_interval_seconds must be positive."
  }
}

variable "exec_connection_pool_size" {
  type    = number
  default = 1024

  validation {
    condition     = var.exec_connection_pool_size > 0 && floor(var.exec_connection_pool_size) == var.exec_connection_pool_size
    error_message = "exec_connection_pool_size must be a positive integer."
  }
}

variable "agent_port" {
  type    = number
  default = 8787

  validation {
    condition     = var.agent_port > 0 && var.agent_port <= 65535 && floor(var.agent_port) == var.agent_port
    error_message = "agent_port must be an integer between 1 and 65535."
  }
}

variable "api_token" {
  type      = string
  sensitive = true
  ephemeral = true

  validation {
    condition     = length(trimspace(var.api_token)) >= 32
    error_message = "api_token must contain at least 32 characters."
  }
}
