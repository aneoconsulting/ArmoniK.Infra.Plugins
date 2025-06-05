variable "gcp_windows_lifecycle" {
  description = "Configuration for ArmoniK Windows Lifecycle service deployment"
  type = object({
    # Project and Location
    project_id  = string
    region      = string

    # Service Configuration
    environment = optional(string, "production")

    # Instance Configuration
    base_instance_name = string
    machine_type       = optional(string, "e2-standard-4")
    source_image       = optional(string, "projects/windows-cloud/global/images/family/windows-2022")
    disk_size_gb       = optional(number, 100)
    disk_type          = optional(string, "pd-standard")

    # Network Configuration
    network          = string
    subnetwork       = string
    assign_public_ip = optional(bool, false)
    instance_tags    = optional(list(string), ["armonik-lifecycle"])

    # Instance Template and Group
    instance_template_name_prefix = string
    instance_group_name           = string
    initial_instance_count        = optional(number, 1)

    # Auto-healing Configuration
    auto_healing_delay_sec = optional(number, 1200)  # 20 minutes for Windows + Python installation

    # Health Check Configuration
    health_check_name = string
    health_check_port = optional(number, 8080)
    health_check_path = optional(string, "/healthz")
  })

  # Basic validation rules
  validation {
    condition     = length(var.gcp_windows_lifecycle.project_id) > 0
    error_message = "Project ID must not be empty."
  }

  validation {
    condition     = length(var.gcp_windows_lifecycle.region) > 0
    error_message = "Region must not be empty."
  }

  validation {
    condition     = var.gcp_windows_lifecycle.initial_instance_count >= 1
    error_message = "Initial instance count must be at least 1."
  }

  validation {
    condition     = var.gcp_windows_lifecycle.health_check_port > 0 && var.gcp_windows_lifecycle.health_check_port < 65536
    error_message = "Health check port must be between 1 and 65535."
  }
}
