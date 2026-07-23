variable "sandbox_node_selector" {
  type = map(string)
}

variable "sandbox_pod_annotations" {
  type = map(string)
}

variable "runtime_class_name" {
  type = string
}

variable "runtime_class_handler" {
  type = string
}

variable "egress_driver" {
  type = string
}

variable "egress_rbac_rules" {
  type = list(object({
    api_groups = list(string)
    resources  = list(string)
  }))
}

variable "control_image" {
  type = string
}

variable "docker_image" {
  type = string
}

variable "allowed_image_prefixes" {
  type = list(string)
}

variable "require_image_digest" {
  type = bool
}

variable "control_replicas" {
  type = number
}

variable "control_cpu_request" {
  type = string
}

variable "control_memory_request" {
  type = string
}

variable "control_cpu_limit" {
  type = string
}

variable "control_memory_limit" {
  type = string
}

variable "namespace_pod_quota" {
  type = number
}

variable "namespace_cpu_quota" {
  type = string
}

variable "namespace_cpu_limit_quota" {
  type = string
}

variable "namespace_memory_quota" {
  type = string
}

variable "namespace_memory_limit_quota" {
  type = string
}

variable "namespace_storage_quota" {
  type = string
}

variable "namespace_storage_limit_quota" {
  type = string
}

variable "command_heartbeat_seconds" {
  type = number
}

variable "activity_write_interval_seconds" {
  type = number
}

variable "exec_connection_pool_size" {
  type = number
}

variable "agent_port" {
  type = number
}

variable "api_token" {
  type      = string
  sensitive = true
  ephemeral = true
}
