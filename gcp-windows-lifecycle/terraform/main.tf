# Data source to get available zones in the region
data "google_compute_zones" "available" {
  region = var.gcp_windows_lifecycle.region
  status = "UP"
}

# Locals for naming consistency
locals {
  name_prefix = "${var.gcp_windows_lifecycle.base_instance_name}-${var.gcp_windows_lifecycle.environment}"

  common_labels = {
    project     = var.gcp_windows_lifecycle.project_id
    environment = var.gcp_windows_lifecycle.environment
    component   = "windows-lifecycle"
    managed-by  = "terraform"
  }

  sanitized_prefix = substr(replace(var.gcp_windows_lifecycle.instance_template_name_prefix, "-", ""), 0, 25)
}

# Service Account for Windows instances
resource "google_service_account" "windows_service_account" {
  account_id   = "${local.sanitized_prefix}-sa"
  display_name = "ArmoniK Windows Lifecycle Service Account"
  description  = "Service account for Windows lifecycle management instances"
}

# IAM bindings for the service account
resource "google_project_iam_member" "windows_logging_writer" {
  project = var.gcp_windows_lifecycle.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.windows_service_account.email}"
}

resource "google_project_iam_member" "windows_monitoring_writer" {
  project = var.gcp_windows_lifecycle.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.windows_service_account.email}"
}

resource "google_project_iam_member" "windows_compute_viewer" {
  project = var.gcp_windows_lifecycle.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.windows_service_account.email}"
}

# Allow health check traffic from GCP load balancer ranges
resource "google_compute_firewall" "allow_health_check" {
  name    = "${local.name_prefix}-allow-health-check"
  network = var.gcp_windows_lifecycle.network

  allow {
    protocol = "tcp"
    ports    = [tostring(var.gcp_windows_lifecycle.health_check_port)]
  }

  # GCP health check source ranges
  source_ranges = [
    "35.191.0.0/16",  # Google Cloud Load Balancer health check ranges
    "130.211.0.0/22"  # Google Cloud Load Balancer health check ranges
  ]

  target_tags = var.gcp_windows_lifecycle.instance_tags

  description = "Allow health check traffic for ArmoniK Windows Lifecycle instances"
}

# Allow external access to health endpoints for testing
resource "google_compute_firewall" "allow_external_health_check" {
  name    = "${local.name_prefix}-allow-external-health"
  network = var.gcp_windows_lifecycle.network

  allow {
    protocol = "tcp"
    ports    = [tostring(var.gcp_windows_lifecycle.health_check_port)]
  }

  # Allow from anywhere for testing (you can restrict this to specific IPs)
  source_ranges = ["0.0.0.0/0"]

  target_tags = var.gcp_windows_lifecycle.instance_tags

  description = "Allow external access to health endpoints for testing and monitoring"
}

# Health Check for Windows instances
resource "google_compute_health_check" "windows_lifecycle" {
  name = var.gcp_windows_lifecycle.health_check_name

  timeout_sec         = 15  # Increased timeout for Windows
  check_interval_sec  = 60  # Increased interval for Windows boot time and Python setup
  healthy_threshold   = 2
  unhealthy_threshold = 10  # Very tolerant for Windows startup and Python installation

  http_health_check {
    port         = var.gcp_windows_lifecycle.health_check_port
    request_path = var.gcp_windows_lifecycle.health_check_path
  }

  description = "Health check for ArmoniK Windows Lifecycle instances"
}

# Instance Template for Windows instances
resource "google_compute_instance_template" "windows_lifecycle" {
  name_prefix = "${local.name_prefix}-template-"
  description = "Template for ArmoniK Windows Lifecycle instances"

  machine_type = var.gcp_windows_lifecycle.machine_type

  disk {
    source_image = var.gcp_windows_lifecycle.source_image
    auto_delete  = true
    boot         = true
    disk_size_gb = var.gcp_windows_lifecycle.disk_size_gb
    disk_type    = var.gcp_windows_lifecycle.disk_type
  }

  network_interface {
    network    = var.gcp_windows_lifecycle.network
    subnetwork = var.gcp_windows_lifecycle.subnetwork

    dynamic "access_config" {
      for_each = var.gcp_windows_lifecycle.assign_public_ip ? [1] : []
      content {}
    }
  }

  service_account {
    email  = google_service_account.windows_service_account.email
    scopes = ["cloud-platform"]
  }

  # Import complete startup script from file with variable substitution
  metadata = {
    "windows-startup-script-ps1" = templatefile("${path.module}/../scripts/startup_script.ps1", {
      health_port = var.gcp_windows_lifecycle.health_check_port
      health_path = var.gcp_windows_lifecycle.health_check_path
    })
  }

  tags = var.gcp_windows_lifecycle.instance_tags

  labels = local.common_labels

  lifecycle {
    create_before_destroy = true
  }
}

# Regional Managed Instance Group (spans multiple zones automatically)
resource "google_compute_region_instance_group_manager" "windows_lifecycle" {
  name   = var.gcp_windows_lifecycle.instance_group_name
  region = var.gcp_windows_lifecycle.region

  description = "Regional Managed Instance Group for ArmoniK Windows Lifecycle instances"

  base_instance_name = var.gcp_windows_lifecycle.base_instance_name
  target_size        = var.gcp_windows_lifecycle.initial_instance_count

  version {
    instance_template = google_compute_instance_template.windows_lifecycle.id
  }

  # Auto-healing policy
  auto_healing_policies {
    health_check      = google_compute_health_check.windows_lifecycle.id
    initial_delay_sec = var.gcp_windows_lifecycle.auto_healing_delay_sec
  }

  # Update policy for rolling updates
  update_policy {
    type                         = "PROACTIVE"
    minimal_action               = "REPLACE"
    max_surge_fixed              = 0  # Set to 0 to avoid zone constraints
    max_unavailable_fixed        = 3  # Must be at least equal to number of zones (europe-west1 has 3 zones)
  }

  timeouts {
    create = "15m"
    update = "15m"
    delete = "15m"
  }
}
