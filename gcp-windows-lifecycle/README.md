# ArmoniK Windows Lifecycle Service for GCP

**Production-Ready Windows Lifecycle Service for ArmoniK on Google Cloud Platform**

This service provides comprehensive lifecycle management for ArmoniK agent and worker processes on Windows VMs in GCP, with auto-scaling, health monitoring, and enterprise-grade reliability features.

## 📋 Table of Contents

- [Overview](#overview)
- [Current Status](#current-status)
- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start---production-deployment)
- [Configuration](#configuration)
- [Deployment Methods](#deployment-methods)
- [Monitoring & Validation](#monitoring--validation)
- [Troubleshooting](#troubleshooting)
- [Security](#security)
- [Performance](#performance)
- [Contributing](#contributing)

## 🎯 Overview

The ArmoniK Windows Lifecycle Service is a comprehensive solution for managing Windows VM lifecycle on Google Cloud Platform (GCP). It provides automated management of ArmoniK agent and worker processes with enterprise-grade reliability, monitoring, and auto-scaling capabilities.

## ✅ Current Status

- **🎉 Production Ready**: Successfully deployed and validated
- **🔄 Active Deployment**: Currently managing 3+ Windows Server 2022 instances
- **✅ Health Validated**: All health endpoints responding correctly (`/healthz`, `/ready`, `/status`, `/metrics`)
- **📊 Monitoring Active**: Comprehensive logging and metrics collection enabled
- **🛡️ Security Hardened**: IAM-based access control and network security implemented
- **🔧 Self-Contained**: Single PowerShell script handles all dependencies including Python installation

## 🚀 Features

### Core Functionality
- **✅ Self-Contained Deployment**: Complete PowerShell startup script with embedded Python health server
- **✅ Automated Windows Management**: Handles Python 3.11.9 installation and service configuration
- **✅ Comprehensive Health Monitoring**: Multiple HTTP endpoints (`/healthz`, `/ready`, `/status`, `/metrics`)
- **✅ Enterprise Logging**: Structured logging with comprehensive error handling
- **✅ Windows Service Integration**: Scheduled task management for robust service lifecycle

### Infrastructure Features  
- **✅ GCP Managed Instance Groups**: Auto-scaling and auto-healing with health checks
- **✅ Zero-Downtime Updates**: Rolling deployment strategy with health validation
- **✅ Network Security**: Firewall rules for health checks and external access control
- **✅ IAM Security**: Minimal service account permissions with Cloud Logging/Monitoring integration
- **✅ Windows Server 2022**: Latest Windows Server image with automated licensing

### Monitoring & Observability
- **✅ Real-time Monitoring**: Built-in monitoring scripts with color-coded status
- **✅ Health Validation**: Comprehensive validation of deployment and service health
- **✅ Boot Disk Validation**: Windows boot disk and OS image integrity checks
- **✅ Performance Metrics**: System metrics collection (CPU, memory, uptime)
- **✅ GCP Integration**: Cloud Logging and Cloud Monitoring integration

### Production Features
- **✅ Auto-healing**: Automatic instance replacement on health check failures
- **✅ Load Balancing**: GCP health check integration for load balancer compatibility
- **✅ Scalability**: Configurable instance counts and machine types
- **✅ Reliability**: Exponential backoff for health checks and robust error handling

## 🏗️ Architecture

### Infrastructure Components
```
┌─────────────────────────────────────────────────────────────┐
│                    Google Cloud Platform                    │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Managed       │  │   Health        │  │   Firewall   │ │
│  │   Instance      │  │   Checks        │  │   Rules      │ │
│  │   Group (MIG)   │  │   (HTTP)        │  │   (8080)     │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
│           │                     │                    │       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Windows       │  │   Service       │  │   IAM        │ │
│  │   Server 2022   │  │   Account       │  │   Roles      │ │
│  │   Instances     │  │   + Permissions │  │   (Minimal)  │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Service Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                    Windows Instance                         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Startup       │  │   Python        │  │   Health     │ │
│  │   Script        │  │   3.11.9        │  │   Server     │ │
│  │   (PowerShell)  │  │   (Auto-install)│  │   (HTTP:8080)│ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
│           │                     │                    │       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Scheduled     │  │   Comprehensive │  │   Multiple   │ │
│  │   Tasks         │  │   Logging       │  │   Endpoints  │ │
│  │   (Service Mgmt)│  │   (File + Cloud)│  │   + Metrics  │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Health Endpoints
| Endpoint | Purpose | Response Format |
|----------|---------|-----------------|
| `/healthz` | Primary health check (GCP MIG) | JSON with service status |
| `/health` | Alternative health endpoint | JSON with service status |
| `/ready` | Readiness probe | JSON with readiness checks |
| `/status` | Detailed service status | JSON with comprehensive info |
| `/metrics` | Service metrics | JSON with performance data |

## 📋 Requirements

### Infrastructure Requirements
- **GCP Project** with billing enabled and APIs activated
- **Compute Engine API** enabled
- **Cloud Logging API** enabled  
- **Cloud Monitoring API** enabled
- **VPC Network** with subnet in target region
- **Internet access** for Python package downloads

### Local Development Requirements
- **Terraform** >= 1.5.0
- **Google Cloud SDK** (`gcloud`) configured
- **Git** for repository management
- **Bash/Zsh** shell for scripts

### GCP Permissions Required
- **Compute Admin** (for VM and MIG management)
- **IAM Admin** (for service account creation) 
- **Security Admin** (for firewall rules)
- **Logging Admin** (for Cloud Logging integration)
- **Monitoring Admin** (for Cloud Monitoring integration)

### Windows VM Specifications
- **OS**: Windows Server 2022 Datacenter (recommended)
- **Machine Type**: e2-medium minimum (e2-standard-4 for production)
- **Disk**: 50GB pd-standard minimum (100GB for production)
- **Network**: Public IP for health check access (or private with Cloud NAT)

### Automatically Installed (No Manual Setup Required)
- **Python 3.11.9** (downloaded and installed by startup script)
- **Python packages** (embedded in health server implementation)
- **Windows Firewall rules** (configured automatically)
- **Scheduled Tasks** (created for service management)

## 📁 Project Structure

```
gcp-windows-lifecycle/
├── config/                          # Configuration files
│   ├── config.ini                   # Main configuration template
│   ├── config_optimized.ini         # Production-optimized configuration
│   └── requirements.txt             # Python dependencies
├── docs/                            # Documentation
├── scripts/                         # Deployment and utility scripts
│   ├── deploy_final.sh             # 🎯 MAIN PRODUCTION DEPLOYMENT SCRIPT
│   ├── startup_final.ps1           # 🎯 MAIN PRODUCTION STARTUP SCRIPT
│   ├── prepare_vm_optimized.ps1    # VM preparation script
│   ├── deploy_optimized.sh         # Alternative deployment method
│   ├── upload_direct.sh            # Direct file transfer utility
│   └── test_health.py              # Health check testing utility
├── src/                            # Core service implementation
│   ├── service.py                  # Main Windows service implementation
│   ├── config_reader.py            # Enhanced configuration reader with CLI support
│   ├── process_manager.py          # Process lifecycle management
│   ├── health_check.py             # HTTP health check server
│   ├── grpc_utils.py              # gRPC communication utilities
│   └── utils.py                   # Common utilities and helpers
└── terraform/                     # Infrastructure as Code
    ├── main_final.tf              # 🎯 MAIN PRODUCTION TERRAFORM CONFIG
    ├── variables_final.tf         # Complete variable definitions
    ├── outputs_final.tf           # Comprehensive outputs
    ├── parameters_final.tfvars    # Production parameter examples
    ├── provider.tf                # GCP provider configuration
    └── defaults.tf                # Default values and validation
```

## 🚀 Quick Start - Production Deployment

### 1. Prerequisites Setup

```bash
# Enable required GCP APIs
gcloud services enable compute.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  cloudresourcemanager.googleapis.com

# Authenticate with GCP
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 2. Deploy Complete Infrastructure

```bash
# Clone and navigate to project
cd gcp-windows-lifecycle/scripts

# Deploy production infrastructure with auto-scaling
./deploy_final.sh --project YOUR_PROJECT_ID --region us-central1 deploy

# Or for staging environment
./deploy_final.sh --project YOUR_PROJECT_ID --environment staging deploy

# Check deployment status
./deploy_final.sh --project YOUR_PROJECT_ID status
```

### 3. Monitor and Verify

```bash
# Check health endpoint (replace INSTANCE_IP)
curl http://INSTANCE_IP:8080/healthz

# View logs
./deploy_final.sh --project YOUR_PROJECT_ID logs

# Monitor via GCP Console
# Navigate to: Compute Engine > Instance Groups > armonik-lifecycle-group
```

## ⚙️ Configuration

### Production Configuration (config_optimized.ini)

The service uses a comprehensive configuration file with environment-specific optimizations:

```ini
[Service]
Name = ArmonikLifecycle
## 🛠️ Deployment Methods

### Method 1: Production Deployment (Recommended)

**Complete infrastructure with auto-scaling, monitoring, and enterprise features:**

```bash
# Full production deployment
./scripts/deploy_final.sh --project YOUR_PROJECT_ID --region us-central1 deploy

# Custom configuration
./scripts/deploy_final.sh --project YOUR_PROJECT_ID \
  --environment production \
  --tfvars custom.tfvars \
  deploy

# Validation only
./scripts/deploy_final.sh --project YOUR_PROJECT_ID validate

# Show deployment plan
./scripts/deploy_final.sh --project YOUR_PROJECT_ID plan
```

### Method 2: Direct File Transfer (Alternative)

**No GCP bucket dependency - files transferred via metadata injection:**

```bash
# Deploy VM with direct file transfer
./scripts/deploy_optimized.sh YOUR_PROJECT_ID us-central1-a

# Upload files directly to existing VM
./scripts/upload_direct.sh YOUR_VM_NAME us-central1-a
```

### Method 3: Development/Testing

**Simplified deployment for development:**

```bash
# Development environment
./scripts/deploy_final.sh --project YOUR_PROJECT_ID \
  --environment development \
  --region us-central1 \
  deploy
```

## 📊 Monitoring & Observability

### Health Check Endpoints

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `GET /healthz` | Overall service health | `healthy` or `unhealthy` |
| `GET /healthz/agent` | Agent process health | `{"status": "healthy", "uptime": 3600}` |
| `GET /healthz/worker` | Worker process health | `{"status": "healthy", "uptime": 3600}` |
| `GET /healthz/detailed` | Comprehensive status | Full JSON with metrics |

### Cloud Monitoring Integration

**Automatic monitoring setup includes:**

- **Uptime Checks**: Multi-region health monitoring
- **Auto-healing**: Automatic instance replacement on health check failures
- **Custom Metrics**: Process uptime, restart counts, gRPC connectivity
- **Alert Policies**: Critical service availability and error rate alerts
- **Notification Channels**: Email and Slack integration

### Viewing Logs

```bash
# Service logs via deployment script
./scripts/deploy_final.sh --project YOUR_PROJECT_ID logs

# Direct Cloud Logging queries
gcloud logging read 'resource.type="gce_instance" AND jsonPayload.service="armonik-lifecycle"' \
  --limit=50 --format="table(timestamp,severity,jsonPayload.message)"

# Real-time log streaming
gcloud logging tail 'resource.type="gce_instance" AND jsonPayload.service="armonik-lifecycle"'

# Error logs only
gcloud logging read 'resource.type="gce_instance" AND jsonPayload.severity="ERROR"' \
  --limit=20
```

### Instance Group Management

```bash
# List instances in group
gcloud compute instance-groups managed list-instances armonik-lifecycle-group \
  --zone=us-central1-a

# Scale instance group
gcloud compute instance-groups managed resize armonik-lifecycle-group \
  --size=5 --zone=us-central1-a

# Rolling update
gcloud compute instance-groups managed rolling-action replace armonik-lifecycle-group \
  --zone=us-central1-a
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Health Checks Failing

**Symptoms**: Instances marked unhealthy, frequent restarts

**Investigation**:
```bash
# Check instance health
gcloud compute instance-groups managed list-instances armonik-lifecycle-group \
  --zone=us-central1-a --format="table(name,status,instanceStatus)"

# Check service logs
gcloud logging read 'resource.type="gce_instance" AND jsonPayload.severity="ERROR"' \
  --limit=20

# Test health endpoint directly
curl http://INSTANCE_IP:8080/healthz
```

**Solutions**:
- Verify firewall rules allow health check traffic (35.191.0.0/16, 130.211.0.0/22)
- Check Windows service status on instance
- Review service configuration and process paths
- Verify network connectivity between agent and worker

#### 2. Auto-scaling Not Working

**Symptoms**: Instance count not adjusting to load

**Investigation**:
```bash
# Check autoscaler status
gcloud compute instance-groups managed describe armonik-lifecycle-group \
  --zone=us-central1-a

# Check CPU utilization
gcloud monitoring metrics list --filter='metricKind=GAUGE AND resource.type=gce_instance'

# Review quota limits
gcloud compute project-info describe --flatten='quotas[]'
```

**Solutions**:
- Verify CPU utilization target is appropriate
- Check GCP quotas for compute instances
- Review autoscaler configuration in Terraform
- Ensure monitoring agent is running on instances

#### 3. Service Won't Start

**Symptoms**: Windows service fails to start or crashes immediately

**Investigation**:
```bash
# Check startup script logs (on Windows instance)
Get-Content C:\Logs\armonik-lifecycle-startup.log -Tail 50

# Check Windows service logs
Get-WinEvent -LogName System | Where-Object {$_.LevelDisplayName -eq "Error"}

# Verify Python installation
python --version
python -c "import win32service, grpc, requests; print('All imports successful')"
```

**Solutions**:
- Verify Python and packages are installed correctly
- Check file paths in configuration
- Ensure service account has necessary permissions
- Review metadata injection and file deployment

#### 4. Network Connectivity Issues

**Symptoms**: gRPC health checks failing, inter-process communication errors

**Investigation**:
```bash
# Check firewall rules
gcloud compute firewall-rules list --filter="name~armonik"

# Test connectivity
telnet AGENT_HOST GRPC_PORT

# Check network configuration
gcloud compute instances describe INSTANCE_NAME --zone=ZONE \
  --format="get(networkInterfaces[].networkIP)"
```

**Solutions**:
- Verify firewall rules allow required ports
- Check VPC network and subnet configuration
- Ensure instances are in correct network/subnet
- Review security groups and IAM permissions

### Advanced Diagnostics

#### Enable Debug Logging

```bash
# Deploy with debug logging
./scripts/deploy_final.sh --project YOUR_PROJECT_ID \
  --environment development \
  deploy
```

#### Manual Service Management

```powershell
# On Windows instance - start service manually
python "C:\Program Files\ArmoniK\src\service.py" debug

# Check service status
Get-Service ArmonikLifecycle

# Restart service
Restart-Service ArmonikLifecycle

# View service configuration
Get-Content "C:\Program Files\ArmoniK\config.ini"
```

#### Performance Monitoring

```bash
# Check instance performance
gcloud compute instances describe INSTANCE_NAME --zone=ZONE \
  --format="get(cpuPlatform,machineType,status)"

# Monitor resource usage
gcloud logging read 'resource.type="gce_instance" AND jsonPayload.component="performance"' \
  --limit=20
```

## 🔄 Updates and Maintenance

### Rolling Updates

```bash
# Update service with zero downtime
./scripts/deploy_final.sh --project YOUR_PROJECT_ID update

# Manual rolling update
gcloud compute instance-groups managed rolling-action replace armonik-lifecycle-group \
  --zone=us-central1-a \
  --max-surge=1 \
  --max-unavailable=0
```

### Backup and Recovery

```bash
# Backup Terraform state
./scripts/deploy_final.sh --project YOUR_PROJECT_ID --backup-state plan

# Export configuration
terraform output -json > deployment-backup-$(date +%Y%m%d).json
```

### Cleanup

```bash
# Destroy infrastructure
./scripts/deploy_final.sh --project YOUR_PROJECT_ID destroy --auto-approve

# Clean up local files
rm -f terraform.tfstate* terraform.tfplan
```

## 📚 Additional Resources

- **GCP Managed Instance Groups**: [Documentation](https://cloud.google.com/compute/docs/instance-groups)
- **Cloud Monitoring**: [Setup Guide](https://cloud.google.com/monitoring/quickstart)
- **Windows on GCP**: [Best Practices](https://cloud.google.com/compute/docs/instances/windows)
- **Terraform GCP Provider**: [Reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For issues and questions:

1. **Check the troubleshooting section above**
2. **Review logs**: `./scripts/deploy_final.sh --project YOUR_PROJECT_ID logs`
3. **Create an issue** in the repository with:
   - Deployment command used
   - Error messages from logs
   - GCP project configuration
   - Steps to reproduce the issue

---

**Production-Ready ArmoniK Windows Lifecycle Service** - Built for enterprise scalability, reliability, and observability on Google Cloud Platform.
DisplayName = ArmoniK Lifecycle Manager
Description = Manages the lifecycle of ArmoniK components on Windows VMs in GCP
```

### Agent Section

```ini
[Agent]
Command = C:\Program Files\Armonik\agent.exe
Args = --config C:\etc\agent.conf
HealthUrl = http://localhost:5000/healthz
```

### Worker Section

```ini
[Worker]
Command = C:\Program Files\Armonik\worker.exe
Args = --config C:\etc\worker.conf
HealthUrl = http://localhost:5001/healthz
GrpcHost = localhost
GrpcPort = 5001
GrpcTimeout = 5.0
GrpcEnabled = true
```

### HealthCheck Section

```ini
[HealthCheck]
Host = 0.0.0.0
Port = 8080
Interval = 10
MaxFailures = 5
BackoffFactor = 1
BackoffMax = 10
ShutdownDelay = 60
EnableHttps = false
CertFile = 
KeyFile = 
```

### GCP Section

```ini
[GCP]
Enabled = true
MetadataUrl = http://metadata.google.internal/computeMetadata/v1/instance/
AutoShutdown = true
EnableCloudLogging = false
CloudLoggingName = armonik-lifecycle
```

### Logging Section

```ini
[Logging]
Level = INFO
LogToFile = true
LogFilePath = C:\Program Files\Armonik\lifecycle.log
LogToConsole = true
```

## Installation

### Automatic Installation

1. Copy the entire `gcp-windows-lifecycle` directory to the Windows VM
2. Run `scripts\install_service.bat` as administrator

### Manual Installation

1. Install Python 3.8 or newer
2. Install required packages:
   ```
   pip install pywin32 grpcio requests
   ```
3. Copy the `src` directory to `C:\Program Files\Armonik\lifecycle`
4. Copy the `config` directory to `C:\Program Files\Armonik\config`
5. Register the Windows service:
   ```
   python "C:\Program Files\Armonik\lifecycle\service.py" --startup auto install
   ```
6. Start the service:
   ```
   net start ArmonikLifecycle
   ```

## Usage with GCP

To use this service with GCP Managed Instance Groups (MIG):

1. Create a Windows VM image with the service installed
2. Configure the VM to run `scripts\gcp_startup.bat` on startup
3. Configure the MIG health check to use the HTTP endpoint at `/health` on port 8080

## Development and Testing

For testing without installing as a Windows service:

```
python scripts\run_standalone.py [--config path/to/config.ini]
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
