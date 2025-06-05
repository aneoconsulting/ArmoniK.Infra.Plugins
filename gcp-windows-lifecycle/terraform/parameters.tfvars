# ArmoniK Windows Lifecycle Service - MVP Example Configuration
# This is a minimal example for testing the MVP deployment

gcp_windows_lifecycle = {
  # Project and Location - Configured for current environment
  project_id  = "current-project-id"  # Replace with your actual project ID
  region      = "region"

  # Service Configuration
  environment = "mvp"

  # Instance Configuration
  base_instance_name = "armonik-lifecycle"
  machine_type       = "e2-medium" # Smaller for MVP testing
  disk_size_gb       = 50          # Smaller disk for MVP
  disk_type          = "pd-standard"

  # Network Configuration - UPDATE WITH YOUR PROJECT
  network          = "projects/armonik-gcp-13469/global/networks/default"
  subnetwork       = "projects/armonik-gcp-13469/regions/europe-west1/subnetworks/default"
  assign_public_ip = true # Set to true for MVP testing
  instance_tags    = ["armonik-lifecycle"]

  # Instance Template and Group
  instance_template_name_prefix = "armonik-windows-template"
  instance_group_name           = "armonik-windows-mig"
  initial_instance_count        = 3
  # Auto-healing Configuration - Increased for Windows
  auto_healing_delay_sec = 600
  # Health Check Configuration
  health_check_name = "armonik-windows-health-check"
  health_check_port = 8080
  health_check_path = "/healthz"
}
