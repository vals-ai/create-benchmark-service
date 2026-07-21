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

variable "api_token" {
  type      = string
  sensitive = true
  ephemeral = true

  validation {
    condition     = length(trimspace(var.api_token)) >= 32
    error_message = "api_token must contain at least 32 characters."
  }
}
