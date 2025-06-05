# Service Account outputs
output "service_account_email" {
  description = "Email of the service account used by Windows instances"
  value       = google_service_account.windows_service_account.email
}

output "service_account_unique_id" {
  description = "Unique ID of the service account"
  value       = google_service_account.windows_service_account.unique_id
}

# Health Check outputs
output "health_check_id" {
  description = "ID of the health check"
  value       = google_compute_health_check.windows_lifecycle.id
}

output "health_check_self_link" {
  description = "Self link of the health check"
  value       = google_compute_health_check.windows_lifecycle.self_link
}

# Instance Template outputs
output "instance_template_id" {
  description = "ID of the instance template"
  value       = google_compute_instance_template.windows_lifecycle.id
}

output "instance_template_self_link" {
  description = "Self link of the instance template"
  value       = google_compute_instance_template.windows_lifecycle.self_link
}

# Regional Managed Instance Group outputs
output "instance_group_id" {
  description = "ID of the regional managed instance group"
  value       = google_compute_region_instance_group_manager.windows_lifecycle.id
}

output "instance_group_self_link" {
  description = "Self link of the regional managed instance group"
  value       = google_compute_region_instance_group_manager.windows_lifecycle.self_link
}

output "instance_group_instance_group" {
  description = "Instance group URL"
  value       = google_compute_region_instance_group_manager.windows_lifecycle.instance_group
}

# Firewall outputs
output "firewall_health_check_id" {
  description = "ID of the health check firewall rule"
  value       = google_compute_firewall.allow_health_check.id
}

output "health_check_url" {
  description = "Health check endpoint URL pattern"
  value       = "http://INSTANCE_IP:${var.gcp_windows_lifecycle.health_check_port}${var.gcp_windows_lifecycle.health_check_path}"
}

# Instance monitoring commands
output "monitoring_commands" {
  description = "Useful commands for monitoring the regional deployment"
  value = {
    list_instances = "gcloud compute instance-groups managed list-instances ${var.gcp_windows_lifecycle.instance_group_name} --region=${var.gcp_windows_lifecycle.region}"
    describe_health = "gcloud compute instance-groups managed get-health ${var.gcp_windows_lifecycle.instance_group_name} --region=${var.gcp_windows_lifecycle.region}"
    view_logs = "gcloud compute instances get-serial-port-output INSTANCE_NAME --zone=INSTANCE_ZONE"
    get_instance_ip = "gcloud compute instances describe INSTANCE_NAME --zone=INSTANCE_ZONE --format='get(networkInterfaces[0].accessConfigs[0].natIP)'"
  }
}
