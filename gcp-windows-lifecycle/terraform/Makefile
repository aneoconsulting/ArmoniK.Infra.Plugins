# ArmoniK Windows Lifecycle Service - Terraform Makefile
# Provides convenient commands for managing the infrastructure
# Variables are bound directly in the Makefile instead of using parameters.tfvars

.PHONY: help init-local init-gcs init plan deploy destroy validate fmt clean status test vars

# Default target
help:
	@echo "ArmoniK Windows Lifecycle Service - Terraform Management"
	@echo ""
	@echo "Available targets:"
	@echo "  help       - Show this help message"
	@echo "  vars       - Show current variable configuration"
	@echo "  init-local - Initialize Terraform with local backend (development)"
	@echo "  init-gcs   - Initialize Terraform with GCS backend (production)"
	@echo "  init       - Initialize Terraform (auto-detect backend)"
	@echo "  validate   - Validate Terraform configuration"
	@echo "  fmt        - Format Terraform files"
	@echo "  plan       - Create execution plan"
	@echo "  deploy     - Deploy changes automatically"
	@echo "  deploy-auto - Deploy changes without confirmation (alias for deploy)"
	@echo "  destroy    - Destroy infrastructure"
	@echo "  clean      - Clean local Terraform files"
	@echo "  status     - Show current infrastructure status"
	@echo "  test       - Run infrastructure tests"
	@echo ""
	@echo "Backend Configuration:"
	@echo "  init-local - Uses local state file (for development)"
	@echo "  init-gcs   - Uses GCS bucket for state (for production)"
	@echo ""
	@echo "Variable Configuration (override with environment or make variables):"
	@echo "  GCP_PROJECT_ID           - GCP Project ID (default: auto-detected)"
	@echo "  GCP_REGION               - GCP Region (default: europe-west1)"
	@echo "  ENVIRONMENT              - Environment name (default: complete)"
	@echo "  BASE_INSTANCE_NAME       - Instance name prefix (default: armonik-lifecycle)"
	@echo "  MACHINE_TYPE             - VM machine type (default: e2-medium)"
	@echo "  DISK_SIZE_GB             - Boot disk size (default: 50)"
	@echo "  INITIAL_INSTANCE_COUNT   - Number of instances (default: 3)"
	@echo "  ASSIGN_PUBLIC_IP         - Assign public IPs (default: true)"
	@echo "  AUTO_HEALING_DELAY_SEC   - Auto-healing delay (default: 600)"
	@echo "  HEALTH_CHECK_PORT        - Health check port (default: 8080)"
	@echo ""
	@echo "Examples:"
	@echo "  make init-local                           # Initialize with local backend"
	@echo "  make init-gcs GCS_BUCKET=my-bucket       # Initialize with GCS backend"
	@echo "  make plan                                 # Plan with default variables"
	@echo "  make deploy ENVIRONMENT=production        # deploy with production environment"
	@echo "  make deploy INITIAL_INSTANCE_COUNT=5      # deploy with 5 instances"
	@echo "  make destroy                             # Destroy infrastructure"

# ==================================================================================
# VARIABLE CONFIGURATION - All variables bound directly in Makefile
# ==================================================================================

# Auto-detect GCP configuration from gcloud
GCP_PROJECT_ID ?= $(shell gcloud config get-value project 2>/dev/null || echo "armonik-gcp-13469")
GCP_REGION ?= europe-west1

# Service Configuration
ENVIRONMENT ?= complete
BASE_INSTANCE_NAME ?= armonik-lifecycle

# Instance Configuration
MACHINE_TYPE ?= e2-medium
SOURCE_IMAGE ?= projects/windows-cloud/global/images/family/windows-2022
DISK_SIZE_GB ?= 50
DISK_TYPE ?= pd-standard

# Network Configuration (auto-constructed from project)
NETWORK ?= projects/$(GCP_PROJECT_ID)/global/networks/default
SUBNETWORK ?= projects/$(GCP_PROJECT_ID)/regions/$(GCP_REGION)/subnetworks/default
ASSIGN_PUBLIC_IP ?= true
INSTANCE_TAGS ?= ["armonik-lifecycle"]

# Instance Template and Group
INSTANCE_TEMPLATE_NAME_PREFIX ?= armonik-windows-template
INSTANCE_GROUP_NAME ?= armonik-windows-mig
INITIAL_INSTANCE_COUNT ?= 3

# Auto-healing Configuration
AUTO_HEALING_DELAY_SEC ?= 600

# Health Check Configuration
HEALTH_CHECK_NAME ?= armonik-windows-health-check
HEALTH_CHECK_PORT ?= 8080
HEALTH_CHECK_PATH ?= /healthz

# Backend Configuration
GCS_BUCKET ?=

# Build Terraform variable arguments
TF_VAR_ARGS := \
	-var 'gcp_windows_lifecycle={ \
		project_id = "$(GCP_PROJECT_ID)", \
		region = "$(GCP_REGION)", \
		environment = "$(ENVIRONMENT)", \
		base_instance_name = "$(BASE_INSTANCE_NAME)", \
		machine_type = "$(MACHINE_TYPE)", \
		source_image = "$(SOURCE_IMAGE)", \
		disk_size_gb = $(DISK_SIZE_GB), \
		disk_type = "$(DISK_TYPE)", \
		network = "$(NETWORK)", \
		subnetwork = "$(SUBNETWORK)", \
		assign_public_ip = $(ASSIGN_PUBLIC_IP), \
		instance_tags = $(INSTANCE_TAGS), \
		instance_template_name_prefix = "$(INSTANCE_TEMPLATE_NAME_PREFIX)", \
		instance_group_name = "$(INSTANCE_GROUP_NAME)", \
		initial_instance_count = $(INITIAL_INSTANCE_COUNT), \
		auto_healing_delay_sec = $(AUTO_HEALING_DELAY_SEC), \
		health_check_name = "$(HEALTH_CHECK_NAME)", \
		health_check_port = $(HEALTH_CHECK_PORT), \
		health_check_path = "$(HEALTH_CHECK_PATH)" \
	}'

# ==================================================================================
# MAKEFILE CONFIGURATION
# ==================================================================================

# Colors for output
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
BLUE := \033[34m
NC := \033[0m

# Directory configuration
CURRENT_DIR := $(shell pwd)
GENERATED_DIR := $(CURRENT_DIR)/generated
STATE_FILE := terraform.tfstate
OUTPUT_FILE := $(GENERATED_DIR)/armonik-output.json

# Terraform configuration
export TF_DATA_DIR ?= $(GENERATED_DIR)
export TF_PLUGIN_CACHE_DIR ?= $(GENERATED_DIR)/terraform-plugins
SHELL := /bin/bash

# ==================================================================================
# TARGETS
# ==================================================================================

# Show current variable configuration
vars:
	@echo -e "$(BLUE)ArmoniK Windows Lifecycle Service - Variable Configuration$(NC)"
	@echo "================================================================="
	@echo ""
	@echo -e "$(YELLOW)Project Configuration:$(NC)"
	@echo "  GCP_PROJECT_ID:             $(GCP_PROJECT_ID)"
	@echo "  GCP_REGION:                 $(GCP_REGION)"
	@echo "  ENVIRONMENT:                $(ENVIRONMENT)"
	@echo ""
	@echo -e "$(YELLOW)Instance Configuration:$(NC)"
	@echo "  BASE_INSTANCE_NAME:         $(BASE_INSTANCE_NAME)"
	@echo "  MACHINE_TYPE:               $(MACHINE_TYPE)"
	@echo "  DISK_SIZE_GB:               $(DISK_SIZE_GB)"
	@echo "  INITIAL_INSTANCE_COUNT:     $(INITIAL_INSTANCE_COUNT)"
	@echo ""
	@echo -e "$(YELLOW)Network Configuration:$(NC)"
	@echo "  NETWORK:                    $(NETWORK)"
	@echo "  SUBNETWORK:                 $(SUBNETWORK)"
	@echo "  ASSIGN_PUBLIC_IP:           $(ASSIGN_PUBLIC_IP)"
	@echo ""
	@echo -e "$(YELLOW)Health Check Configuration:$(NC)"
	@echo "  HEALTH_CHECK_NAME:          $(HEALTH_CHECK_NAME)"
	@echo "  HEALTH_CHECK_PORT:          $(HEALTH_CHECK_PORT)"
	@echo "  HEALTH_CHECK_PATH:          $(HEALTH_CHECK_PATH)"
	@echo "  AUTO_HEALING_DELAY_SEC:     $(AUTO_HEALING_DELAY_SEC)"
	@echo ""
	@echo -e "$(YELLOW)Instance Group Configuration:$(NC)"
	@echo "  INSTANCE_TEMPLATE_NAME_PREFIX: $(INSTANCE_TEMPLATE_NAME_PREFIX)"
	@echo "  INSTANCE_GROUP_NAME:        $(INSTANCE_GROUP_NAME)"
	@echo ""

# Validate configuration before running commands
validate-config:
	@if [ -z "$(GCP_PROJECT_ID)" ]; then \
		echo -e "$(RED)Error: GCP_PROJECT_ID is not set$(NC)"; \
		echo "Run: gcloud config set project YOUR_PROJECT_ID"; \
		exit 1; \
	fi
	@if [ -z "$(GCP_REGION)" ]; then \
		echo -e "$(RED)Error: GCP_REGION is not set$(NC)"; \
		exit 1; \
	fi
	@echo -e "$(GREEN)✓ Configuration validated$(NC)"

# Initialize with local backend (for development)
init-local: validate-config
	@echo -e "$(YELLOW)Initializing Terraform with local backend...$(NC)"
	@mkdir -p $(GENERATED_DIR)
	@# Remove backend configuration temporarily
	@if [ -f provider.tf.backup ]; then rm provider.tf.backup; fi
	@cp provider.tf provider.tf.backup
	@sed 's/backend "gcs" {}/# backend "gcs" {} # Temporarily disabled for local backend/' provider.tf > provider.tf.tmp
	@mv provider.tf.tmp provider.tf
	@terraform init -upgrade -reconfigure
	@mv provider.tf.backup provider.tf
	@echo -e "$(GREEN)✓ Terraform initialized with local backend$(NC)"

# Initialize with GCS backend (for production)
init-gcs: validate-config
	@echo -e "$(YELLOW)Initializing Terraform with GCS backend...$(NC)"
	@if [ -z "$(GCS_BUCKET)" ]; then \
		echo -e "$(RED)Error: GCS_BUCKET must be set for GCS backend$(NC)"; \
		echo "Usage: make init-gcs GCS_BUCKET=your-terraform-state-bucket"; \
		exit 1; \
	fi
	@mkdir -p $(GENERATED_DIR)
	@echo "bucket = \"$(GCS_BUCKET)\"" > backend-gcs-temp.hcl
	@echo "prefix = \"armonik-windows-lifecycle\"" >> backend-gcs-temp.hcl
	@terraform init -upgrade -reconfigure -backend-config=backend-gcs-temp.hcl
	@rm -f backend-gcs-temp.hcl
	@echo -e "$(GREEN)✓ Terraform initialized with GCS backend: $(GCS_BUCKET)$(NC)"

# Auto-detect backend and initialize
init: validate-config
	@if [ -f "terraform.tfstate" ]; then \
		echo -e "$(YELLOW)Local state file detected, using local backend...$(NC)"; \
		$(MAKE) init-local; \
	elif [ -n "$(GCS_BUCKET)" ]; then \
		echo -e "$(YELLOW)GCS_BUCKET specified, using GCS backend...$(NC)"; \
		$(MAKE) init-gcs; \
	else \
		echo -e "$(YELLOW)No state detected, defaulting to local backend...$(NC)"; \
		$(MAKE) init-local; \
	fi

# Validate Terraform configuration
validate: validate-config
	@echo -e "$(YELLOW)Validating Terraform configuration...$(NC)"
	@terraform validate
	@echo -e "$(GREEN)✓ Terraform configuration is valid$(NC)"

# Format Terraform files
fmt:
	@echo -e "$(YELLOW)Formatting Terraform files...$(NC)"
	@terraform fmt -recursive
	@echo -e "$(GREEN)✓ Terraform files formatted$(NC)"

# Create execution plan
plan: validate-config
	@echo -e "$(YELLOW)Creating Terraform execution plan...$(NC)"
	@echo -e "$(BLUE)Using variables:$(NC)"
	@echo "  Project: $(GCP_PROJECT_ID)"
	@echo "  Region: $(GCP_REGION)"
	@echo "  Environment: $(ENVIRONMENT)"
	@echo "  Instances: $(INITIAL_INSTANCE_COUNT)"
	@echo ""
	@terraform plan $(TF_VAR_ARGS) -out=$(GENERATED_DIR)/terraform.plan
	@echo -e "$(GREEN)✓ Execution plan created$(NC)"

# deploy changes
deploy: validate-config
	@echo -e "$(YELLOW)Applying Terraform changes...$(NC)"
	@echo -e "$(BLUE)Configuration Summary:$(NC)"
	@echo "  Project ID: $(GCP_PROJECT_ID)"
	@echo "  Region: $(GCP_REGION)"
	@echo "  Environment: $(ENVIRONMENT)"
	@echo "  Instance Count: $(INITIAL_INSTANCE_COUNT)"
	@echo "  Machine Type: $(MACHINE_TYPE)"
	@echo ""
	@terraform apply $(TF_VAR_ARGS) -auto-approve
	@echo -e "$(GREEN)✓ Infrastructure deployed successfully$(NC)"


# deploy changes without confirmation (for CI/CD)
deploy-auto: validate-config
	@echo -e "$(YELLOW)Applying Terraform changes automatically...$(NC)"
	@terraform apply $(TF_VAR_ARGS) -auto-approve
	@echo -e "$(GREEN)✓ Infrastructure deployed successfully$(NC)"

# Destroy infrastructure
destroy: validate-config
	@echo -e "$(RED)WARNING: This will destroy all infrastructure!$(NC)"
	@echo "Project: $(GCP_PROJECT_ID)"
	@echo "Region: $(GCP_REGION)"
	@echo ""
	@read -p "Are you sure you want to destroy all resources? [y/N] " -n 1 -r; \
	echo ""; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		terraform destroy $(TF_VAR_ARGS) -auto-approve; \
		echo -e "$(GREEN)✓ Infrastructure destroyed$(NC)"; \
	else \
		echo -e "$(YELLOW)Destroy cancelled$(NC)"; \
	fi

# Refresh state
refresh: validate-config
	@terraform refresh $(TF_VAR_ARGS)

# Open Terraform console
console: validate-config
	@terraform console $(TF_VAR_ARGS)

# Generate outputs
output:
	@mkdir -p $(GENERATED_DIR)
	@terraform output -json | jq 'map_values(.value)' > $(OUTPUT_FILE)
	@echo -e "$(GREEN)✓ Outputs saved to: $(OUTPUT_FILE)$(NC)"

# Show current infrastructure status
status: validate-config
	@echo -e "$(BLUE)Infrastructure Status$(NC)"
	@echo "===================="
	@echo ""
	@if [ -f "$(STATE_FILE)" ] || [ -f "$(GENERATED_DIR)/terraform.tfstate" ]; then \
		echo -e "$(GREEN)✓ Terraform state exists$(NC)"; \
		terraform show -json | jq -r '.values.root_module.resources[] | select(.type == "google_compute_region_instance_group_manager") | "Regional Instance Group: " + .values.name + " (instances: " + (.values.target_size | tostring) + ", region: " + .values.region + ")"' 2>/dev/null || true; \
	else \
		echo -e "$(YELLOW)No Terraform state found$(NC)"; \
	fi
	@echo ""

# Clean local Terraform files
clean:
	@echo -e "$(YELLOW)Cleaning Terraform files...$(NC)"
	@rm -rf $(GENERATED_DIR)
	@rm -rf .terraform.lock.hcl .terraform
	@rm -f backend-gcs-temp.hcl provider.tf.backup
	@echo -e "$(GREEN)✓ Terraform files cleaned$(NC)"

# Run infrastructure tests
test: validate-config
	@echo -e "$(YELLOW)Running infrastructure tests...$(NC)"
	@if [ -f "../test-complete-deployment.sh" ]; then \
		cd .. && ./test-complete-deployment.sh; \
	else \
		echo -e "$(RED)Test script not found$(NC)"; \
		exit 1; \
	fi

# ==================================================================================
# UTILITY TARGETS
# ==================================================================================

# Show Terraform version info
version:
	@terraform version

# Show current workspace
workspace:
	@terraform workspace show

# List all workspaces
workspaces:
	@terraform workspace list

# Show providers
providers:
	@terraform providers

# Import existing resource
import:
	@echo "Usage: make import RESOURCE=resource_type.name ID=resource_id"
	@echo "Example: make import RESOURCE=google_compute_instance.example ID=projects/my-project/zones/us-central1-a/instances/my-instance"
	@if [ -n "$(RESOURCE)" ] && [ -n "$(ID)" ]; then \
		terraform import $(TF_VAR_ARGS) $(RESOURCE) $(ID); \
	fi

# ==================================================================================
# DEPLOYMENT SHORTCUTS
# ==================================================================================

# Quick development deployment
dev: GCP_REGION=europe-west1
dev: ENVIRONMENT=development
dev: INITIAL_INSTANCE_COUNT=1
dev: MACHINE_TYPE=e2-micro
dev: init-local plan deploy-auto

# Production deployment
prod: ENVIRONMENT=production
prod: INITIAL_INSTANCE_COUNT=3
prod: MACHINE_TYPE=e2-medium
prod: init deploy

# Testing deployment
test-deploy: ENVIRONMENT=testing
test-deploy: INITIAL_INSTANCE_COUNT=2
test-deploy: init-local deploy-auto test
