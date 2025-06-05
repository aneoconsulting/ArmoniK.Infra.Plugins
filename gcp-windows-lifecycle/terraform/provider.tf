terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0.0"
    }
  }
  required_version = ">= 1.3.0"

  backend "gcs" {}
}

provider "google" {
  project = var.gcp_windows_lifecycle.project_id
  region  = var.gcp_windows_lifecycle.region
}
