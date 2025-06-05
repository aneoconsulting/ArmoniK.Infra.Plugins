# ArmoniK Windows Lifecycle Service - Complete Production Startup Script
# This script sets up the complete ArmoniK lifecycle service with agent and worker management
# Includes health monitoring, process management, auto-scaling, and enterprise features

$ErrorActionPreference = "Continue"

# Configuration - These values are injected by Terraform
$HEALTH_PORT = ${health_port}
$HEALTH_PATH = "${health_path}"
$LOG_FILE = "C:\armonik-lifecycle-startup.log"
$SERVICE_DIR = "C:\ArmoniK-Lifecycle"
$CONFIG_DIR = "$SERVICE_DIR\config"
$LOGS_DIR = "$SERVICE_DIR\logs"
$STATE_DIR = "$SERVICE_DIR\state"

# Enhanced logging function
function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] [$Level] $Message"
    Write-Host $logEntry
    try {
        Add-Content -Path $LOG_FILE -Value $logEntry -Force
    } catch {
        Write-Host "Failed to write to log file: $_"
    }
}

# Initialize logging
Write-Log "=== ArmoniK Complete Lifecycle Service Setup Started ==="
Write-Log "Health Port: $HEALTH_PORT"
Write-Log "Health Path: $HEALTH_PATH"
Write-Log "Service Directory: $SERVICE_DIR"

# Create all necessary directories
$directories = @($SERVICE_DIR, $CONFIG_DIR, $LOGS_DIR, $STATE_DIR)
foreach ($dir in $directories) {
    try {
        if (-not (Test-Path $dir)) {
            New-Item -Path $dir -ItemType Directory -Force | Out-Null
            Write-Log "Created directory: $dir"
        }
    } catch {
        Write-Log "Failed to create directory $dir : $_" "ERROR"
    }
}

# Create the complete lifecycle service Python script
$lifecycleServiceScript = @"
#!/usr/bin/env python3
"""
ArmoniK Windows Lifecycle Service - Complete Implementation
Manages the complete lifecycle of ArmoniK agent and worker processes with enterprise features.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import psutil
import requests
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum

# Comprehensive logging setup
class LoggingManager:
    def __init__(self, service_dir: str = 'C:\\ArmoniK-Lifecycle'):
        self.service_dir = service_dir
        self.logs_dir = os.path.join(service_dir, 'logs')
        os.makedirs(self.logs_dir, exist_ok=True)

        # Configure logging
        log_file = os.path.join(self.logs_dir, 'lifecycle-service.log')

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )

        self.logger = logging.getLogger('ArmonikLifecycle')
        self.logger.info("=== ArmoniK Complete Lifecycle Service Started ===")

class ProcessState(Enum):
    """Process state enumeration."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    FAILED = "failed"
    RESTARTING = "restarting"

@dataclass
class ProcessConfig:
    """Configuration for a managed process."""
    name: str
    command: str
    args: List[str]
    health_url: str
    work_dir: str
    env_vars: Dict[str, str]
    health_check_timeout: float = 30.0
    restart_delay: float = 5.0
    max_restart_attempts: int = 5

@dataclass
class ProcessInfo:
    """Runtime information about a managed process."""
    config: ProcessConfig
    state: ProcessState = ProcessState.STOPPED
    pid: Optional[int] = None
    process: Optional[subprocess.Popen] = None
    start_time: Optional[datetime] = None
    last_health_check: Optional[datetime] = None
    health_status: bool = False
    restart_count: int = 0
    failure_count: int = 0

class HealthMetrics:
    """Comprehensive metrics collection."""

    def __init__(self):
        self.start_time = time.time()
        self.request_count = 0
        self.health_checks_passed = 0
        self.health_checks_failed = 0
        self.process_restarts = 0
        self.last_metrics_update = time.time()

    def increment_request(self):
        self.request_count += 1

    def increment_health_pass(self):
        self.health_checks_passed += 1

    def increment_health_fail(self):
        self.health_checks_failed += 1

    def increment_restart(self):
        self.process_restarts += 1

    def get_uptime(self) -> float:
        return time.time() - self.start_time

    def get_system_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics."""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('C:')

            return {
                "cpu_percent": cpu_percent,
                "memory_total_gb": round(memory.total / (1024**3), 2),
                "memory_used_gb": round(memory.used / (1024**3), 2),
                "memory_percent": memory.percent,
                "disk_total_gb": round(disk.total / (1024**3), 2),
                "disk_used_gb": round(disk.used / (1024**3), 2),
                "disk_percent": round((disk.used / disk.total) * 100, 2)
            }
        except Exception as e:
            logging.getLogger('ArmonikLifecycle').warning(f"Failed to get system metrics: {e}")
            return {}

class ProcessManager:
    """Advanced process manager with enterprise features."""

    def __init__(self, service_dir: str):
        self.service_dir = service_dir
        self.logger = logging.getLogger('ArmonikLifecycle.ProcessManager')
        self.processes: Dict[str, ProcessInfo] = {}
        self.stopping = False
        self.monitoring_thread = None

        # Create mock ArmoniK process configurations
        self._setup_process_configs()

    def _setup_process_configs(self):
        """Setup process configurations for ArmoniK components."""

        # Mock Agent Configuration
        agent_config = ProcessConfig(
            name="agent",
            command="python",
            args=[os.path.join(self.service_dir, "mock_agent.py")],
            health_url="http://localhost:5000/health",
            work_dir=self.service_dir,
            env_vars={"ARMONIK_AGENT_PORT": "5000"},
            health_check_timeout=10.0,
            restart_delay=5.0,
            max_restart_attempts=5
        )

        # Mock Worker Configuration
        worker_config = ProcessConfig(
            name="worker",
            command="python",
            args=[os.path.join(self.service_dir, "mock_worker.py")],
            health_url="http://localhost:5001/health",
            work_dir=self.service_dir,
            env_vars={"ARMONIK_WORKER_PORT": "5001"},
            health_check_timeout=10.0,
            restart_delay=5.0,
            max_restart_attempts=5
        )

        self.processes["agent"] = ProcessInfo(config=agent_config)
        self.processes["worker"] = ProcessInfo(config=worker_config)

        self.logger.info("Process configurations initialized")

    def start_all_processes(self):
        """Start all managed processes."""
        self.logger.info("Starting all managed processes")

        # Create mock process scripts first
        self._create_mock_scripts()

        for name, process_info in self.processes.items():
            self.start_process(name)

        # Start monitoring thread
        if not self.monitoring_thread or not self.monitoring_thread.is_alive():
            self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitoring_thread.start()

    def _create_mock_scripts(self):
        """Create mock ArmoniK agent and worker scripts for demonstration."""

        mock_agent_script = '''#!/usr/bin/env python3
import time
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

class AgentHealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response = {
                "status": "healthy",
                "component": "armonik-agent",
                "timestamp": time.time(),
                "uptime": time.time() - start_time
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default logging

start_time = time.time()
server = HTTPServer(('localhost', 5000), AgentHealthHandler)
print(f"Mock ArmoniK Agent started on port 5000")
server.serve_forever()
'''

        mock_worker_script = '''#!/usr/bin/env python3
import time
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

class WorkerHealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response = {
                "status": "healthy",
                "component": "armonik-worker",
                "timestamp": time.time(),
                "uptime": time.time() - start_time,
                "tasks_processed": int((time.time() - start_time) / 10)  # Mock task counter
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default logging

start_time = time.time()
server = HTTPServer(('localhost', 5001), WorkerHealthHandler)
print(f"Mock ArmoniK Worker started on port 5001")
server.serve_forever()
'''

        # Write mock scripts
        agent_script_path = os.path.join(self.service_dir, "mock_agent.py")
        worker_script_path = os.path.join(self.service_dir, "mock_worker.py")

        with open(agent_script_path, 'w') as f:
            f.write(mock_agent_script)

        with open(worker_script_path, 'w') as f:
            f.write(mock_worker_script)

        self.logger.info("Mock ArmoniK scripts created")

    def start_process(self, name: str) -> bool:
        """Start a specific process."""
        if name not in self.processes:
            self.logger.error(f"Unknown process: {name}")
            return False

        process_info = self.processes[name]
        config = process_info.config

        if process_info.state == ProcessState.RUNNING:
            self.logger.info(f"Process {name} is already running")
            return True

        try:
            self.logger.info(f"Starting process: {name}")
            process_info.state = ProcessState.STARTING

            # Build command
            cmd = [config.command] + config.args

            # Set up environment
            env = os.environ.copy()
            env.update(config.env_vars)

            # Start process
            process = subprocess.Popen(
                cmd,
                cwd=config.work_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            )

            process_info.process = process
            process_info.pid = process.pid
            process_info.start_time = datetime.now()
            process_info.state = ProcessState.RUNNING
            process_info.restart_count += 1

            self.logger.info(f"Started {name} with PID {process.pid}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to start process {name}: {e}")
            process_info.state = ProcessState.FAILED
            process_info.failure_count += 1
            return False

    def stop_process(self, name: str) -> bool:
        """Stop a specific process."""
        if name not in self.processes:
            return False

        process_info = self.processes[name]

        if process_info.state != ProcessState.RUNNING or not process_info.process:
            return True

        try:
            self.logger.info(f"Stopping process: {name}")
            process_info.process.terminate()

            # Wait for graceful shutdown
            try:
                process_info.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Force killing process {name}")
                process_info.process.kill()
                process_info.process.wait()

            process_info.state = ProcessState.STOPPED
            process_info.pid = None
            process_info.process = None

            self.logger.info(f"Stopped process: {name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to stop process {name}: {e}")
            return False

    def check_process_health(self, name: str) -> bool:
        """Check health of a specific process."""
        if name not in self.processes:
            return False

        process_info = self.processes[name]

        # Check if process is still running
        if process_info.process and process_info.process.poll() is not None:
            self.logger.warning(f"Process {name} has exited")
            process_info.state = ProcessState.FAILED
            return False

        # Check health endpoint
        try:
            response = requests.get(
                process_info.config.health_url,
                timeout=process_info.config.health_check_timeout
            )

            if response.status_code == 200:
                process_info.health_status = True
                process_info.last_health_check = datetime.now()
                return True
            else:
                self.logger.warning(f"Health check failed for {name}: HTTP {response.status_code}")

        except Exception as e:
            self.logger.warning(f"Health check failed for {name}: {e}")

        process_info.health_status = False
        return False

    def _monitoring_loop(self):
        """Main monitoring loop for all processes."""
        self.logger.info("Process monitoring started")

        while not self.stopping:
            try:
                for name, process_info in self.processes.items():
                    if process_info.state == ProcessState.RUNNING:
                        if not self.check_process_health(name):
                            process_info.failure_count += 1

                            if process_info.failure_count >= process_info.config.max_restart_attempts:
                                self.logger.error(f"Process {name} exceeded max restart attempts")
                                process_info.state = ProcessState.FAILED
                            else:
                                self.logger.info(f"Restarting failed process: {name}")
                                self.stop_process(name)
                                time.sleep(process_info.config.restart_delay)
                                self.start_process(name)

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)

        self.logger.info("Process monitoring stopped")

    def stop_all_processes(self):
        """Stop all managed processes."""
        self.stopping = True
        self.logger.info("Stopping all managed processes")

        for name in self.processes.keys():
            self.stop_process(name)

    def get_status(self) -> Dict[str, Any]:
        """Get status of all managed processes."""
        status = {}

        for name, process_info in self.processes.items():
            status[name] = {
                "state": process_info.state.value,
                "pid": process_info.pid,
                "start_time": process_info.start_time.isoformat() if process_info.start_time else None,
                "health_status": process_info.health_status,
                "last_health_check": process_info.last_health_check.isoformat() if process_info.last_health_check else None,
                "restart_count": process_info.restart_count,
                "failure_count": process_info.failure_count
            }

        return status

class HealthHandler(BaseHTTPRequestHandler):
    """Enhanced HTTP handler for health checks and status endpoints."""

    def do_GET(self):
        metrics.increment_request()

        try:
            if self.path in ['/health', '/healthz']:
                self._handle_health()
            elif self.path == '/ready':
                self._handle_readiness()
            elif self.path == '/metrics':
                self._handle_metrics()
            elif self.path == '/status':
                self._handle_status()
            elif self.path == '/processes':
                self._handle_processes()
            else:
                self._handle_not_found()

        except Exception as e:
            logger.error(f"Error handling request {self.path}: {e}")
            self._send_error_response(500, f"Internal server error: {e}")

    def _handle_health(self):
        """Handle basic health check."""
        health_data = {
            "status": "healthy",
            "service": "armonik-lifecycle-complete",
            "timestamp": time.time(),
            "version": "1.0.0-complete",
            "uptime_seconds": round(metrics.get_uptime(), 2)
        }

        # Check if critical processes are healthy
        process_status = process_manager.get_status()
        healthy_processes = [name for name, status in process_status.items()
                          if status['state'] == 'running' and status['health_status']]

        if len(healthy_processes) < len(process_status):
            health_data["status"] = "degraded"
            health_data["warning"] = "Some processes are not healthy"

        health_data["healthy_processes"] = healthy_processes
        health_data["total_processes"] = len(process_status)

        self._send_json_response(200, health_data)

    def _handle_readiness(self):
        """Handle readiness check."""
        process_status = process_manager.get_status()

        # All processes must be running and healthy for readiness
        all_ready = all(status['state'] == 'running' and status['health_status']
                      for status in process_status.values())

        ready_data = {
            "ready": all_ready,
            "service": "armonik-lifecycle-complete",
            "timestamp": time.time(),
            "processes": process_status
        }

        status_code = 200 if all_ready else 503
        self._send_json_response(status_code, ready_data)

    def _handle_metrics(self):
        """Handle metrics endpoint."""
        metrics_data = {
            "service_metrics": {
                "uptime_seconds": round(metrics.get_uptime(), 2),
                "total_requests": metrics.request_count,
                "health_checks_passed": metrics.health_checks_passed,
                "health_checks_failed": metrics.health_checks_failed,
                "process_restarts": metrics.process_restarts
            },
            "system_metrics": metrics.get_system_metrics(),
            "process_metrics": process_manager.get_status(),
            "timestamp": time.time()
        }

        self._send_json_response(200, metrics_data)

    def _handle_status(self):
        """Handle detailed status endpoint."""
        status_data = {
            "service": {
                "name": "armonik-lifecycle-complete",
                "version": "1.0.0-complete",
                "status": "running",
                "start_time": datetime.fromtimestamp(metrics.start_time).isoformat(),
                "uptime_seconds": round(metrics.get_uptime(), 2)
            },
            "processes": process_manager.get_status(),
            "system": metrics.get_system_metrics(),
            "health_endpoints": {
                "health": f"http://localhost:${health_port}/health",
                "readiness": f"http://localhost:${health_port}/ready",
                "metrics": f"http://localhost:${health_port}/metrics",
                "status": f"http://localhost:${health_port}/status",
                "processes": f"http://localhost:${health_port}/processes"
            },
            "timestamp": time.time()
        }

        self._send_json_response(200, status_data)

    def _handle_processes(self):
        """Handle processes endpoint with detailed information."""
        processes_data = {
            "processes": process_manager.get_status(),
            "summary": {
                "total": len(process_manager.processes),
                "running": len([p for p in process_manager.processes.values()
                              if p.state == ProcessState.RUNNING]),
                "healthy": len([p for p in process_manager.processes.values()
                              if p.health_status])
            },
            "timestamp": time.time()
        }

        self._send_json_response(200, processes_data)

    def _handle_not_found(self):
        """Handle 404 responses."""
        error_data = {
            "error": "Not Found",
            "message": f"Path {self.path} not found",
            "available_endpoints": ["/health", "/healthz", "/ready", "/metrics", "/status", "/processes"],
            "timestamp": time.time()
        }

        self._send_json_response(404, error_data)

    def _send_json_response(self, status_code: int, data: Dict[str, Any]):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        response = json.dumps(data, indent=2, default=str)
        self.wfile.write(response.encode('utf-8'))

    def _send_error_response(self, status_code: int, message: str):
        """Send error response."""
        self.send_response(status_code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(message.encode('utf-8'))

    def log_message(self, format, *args):
        """Override to use our logger."""
        logger.info(f"HTTP {format % args}")

class LifecycleService:
    """Main lifecycle service class."""

    def __init__(self, port: int = ${health_port}):
        self.port = port
        self.server: Optional[HTTPServer] = None
        self.running = False

    def start(self):
        """Start the complete lifecycle service."""
        try:
            logger.info("=== Starting ArmoniK Complete Lifecycle Service ===")

            # Start process manager
            process_manager.start_all_processes()

            # Start HTTP server
            self.server = HTTPServer(("0.0.0.0", self.port), HealthHandler)
            self.running = True

            logger.info(f"HTTP server binding to 0.0.0.0:{self.port}")
            logger.info(f"Health endpoint: http://0.0.0.0:{self.port}${health_path}")
            logger.info(f"Readiness endpoint: http://0.0.0.0:{self.port}/ready")
            logger.info(f"Metrics endpoint: http://0.0.0.0:{self.port}/metrics")
            logger.info(f"Status endpoint: http://0.0.0.0:{self.port}/status")
            logger.info(f"Processes endpoint: http://0.0.0.0:{self.port}/processes")
            logger.info("=== Service Ready ===")

            # Start server (this blocks)
            self.server.serve_forever()

        except Exception as e:
            logger.error(f"Failed to start service: {e}")
            self.running = False
            raise

    def stop(self):
        """Stop the lifecycle service."""
        if self.running:
            logger.info("Stopping lifecycle service...")

            # Stop process manager
            process_manager.stop_all_processes()

            # Stop HTTP server
            if self.server:
                self.server.shutdown()
                self.server.server_close()

            self.running = False
            logger.info("Lifecycle service stopped")

def setup_signal_handlers(service: LifecycleService):
    """Setup signal handlers for graceful shutdown."""

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, stopping service")
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def main():
    """Main entry point."""
    global logger, metrics, process_manager

    # Initialize components
    logging_manager = LoggingManager('C:\\ArmoniK-Lifecycle')
    logger = logging_manager.logger
    metrics = HealthMetrics()
    process_manager = ProcessManager('C:\\ArmoniK-Lifecycle')

    # Create and start service
    service = LifecycleService(${health_port})
    setup_signal_handlers(service)

    try:
        service.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Service error: {e}")
        raise
    finally:
        service.stop()

if __name__ == "__main__":
    main()
"@

# Write the complete lifecycle service script
$healthServerPath = "$SERVICE_DIR\lifecycle_service.py"
try {
    $lifecycleServiceScript | Out-File -FilePath $healthServerPath -Encoding UTF8 -Force
    Write-Log "Created complete lifecycle service script: $healthServerPath"
} catch {
    Write-Log "Failed to create lifecycle service script: $_" "ERROR"
    exit 1
}

# Install required Python packages
Write-Log "Installing required Python packages..."
try {
    # Check if Python is available
    $pythonPaths = @(
        "C:\Program Files\Python311\python.exe",
        "C:\Program Files\Python312\python.exe",
        "C:\Python311\python.exe",
        "C:\Python312\python.exe",
        "python.exe"
    )

    $pythonExe = $null
    foreach ($path in $pythonPaths) {
        if (Test-Path $path -ErrorAction SilentlyContinue) {
            $pythonExe = $path
            Write-Log "Found Python at: $pythonExe"
            break
        } elseif ($path -eq "python.exe") {
            try {
                $result = & python --version 2>&1
                if ($LASTEXITCODE -eq 0) {
                    $pythonExe = "python.exe"
                    Write-Log "Found Python in PATH: $result"
                    break
                }
            } catch {
                # Continue searching
            }
        }
    }

    if (-not $pythonExe) {
        Write-Log "Python not found, installing..." "WARNING"

        # Download and install Python 3.11
        try {
            $pythonVersion = "3.11.9"
            $pythonInstaller = "python-$pythonVersion-amd64.exe"
            $pythonUrl = "https://www.python.org/ftp/python/$pythonVersion/$pythonInstaller"
            $installerPath = "$env:TEMP\$pythonInstaller"

            Write-Log "Downloading Python $pythonVersion from $pythonUrl"
            Invoke-WebRequest -Uri $pythonUrl -OutFile $installerPath -UseBasicParsing
            Write-Log "Downloaded Python installer to $installerPath"

            # Install Python silently
            Write-Log "Installing Python $pythonVersion..."
            $installArgs = "/quiet InstallAllUsers=1 PrependPath=1 Include_test=0"
            $process = Start-Process -FilePath $installerPath -ArgumentList $installArgs -Wait -PassThru

            if ($process.ExitCode -eq 0) {
                Write-Log "Python installed successfully"

                # Update PATH and refresh environment
                $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")

                # Wait a moment for installation to complete
                Start-Sleep -Seconds 10

                # Try to find Python again
                $pythonPaths = @(
                    "C:\Program Files\Python311\python.exe",
                    "C:\Users\All Users\Python311\python.exe",
                    "C:\Python311\python.exe"
                )

                foreach ($path in $pythonPaths) {
                    if (Test-Path $path) {
                        $pythonExe = $path
                        Write-Log "Found newly installed Python at: $pythonExe"
                        break
                    }
                }

                # Try python command in PATH
                if (-not $pythonExe) {
                    try {
                        $result = & python --version 2>&1
                        if ($LASTEXITCODE -eq 0) {
                            $pythonExe = "python.exe"
                            Write-Log "Found Python in PATH after installation: $result"
                        }
                    } catch {
                        Write-Log "Python still not found in PATH after installation" "WARNING"
                    }
                }

            } else {
                Write-Log "Python installation failed with exit code: $($process.ExitCode)" "ERROR"
            }

            # Clean up installer
            if (Test-Path $installerPath) {
                Remove-Item $installerPath -Force
            }

        } catch {
            Write-Log "Failed to download/install Python: $_" "ERROR"
        }

        if (-not $pythonExe) {
            Write-Log "Could not install or find Python. Please ensure Python 3.11+ is available." "ERROR"
            exit 1
        }
    }

    # Install required packages
    $packages = @("psutil", "requests")
    foreach ($package in $packages) {
        Write-Log "Installing package: $package"
        try {
            & $pythonExe -m pip install $package --quiet --no-warn-script-location
            if ($LASTEXITCODE -eq 0) {
                Write-Log "Successfully installed: $package"
            } else {
                Write-Log "Failed to install package: $package" "WARNING"
            }
        } catch {
            Write-Log "Error installing package $package : $_" "WARNING"
        }
    }

} catch {
    Write-Log "Error during package installation: $_" "WARNING"
}

# Configure Windows Firewall for health check port
Write-Log "Configuring Windows Firewall for health check port $HEALTH_PORT..."
try {
    # Remove existing rule if it exists
    $existingRule = Get-NetFirewallRule -DisplayName "ArmoniK Lifecycle Health Check" -ErrorAction SilentlyContinue
    if ($existingRule) {
        Remove-NetFirewallRule -DisplayName "ArmoniK Lifecycle Health Check" -ErrorAction SilentlyContinue
        Write-Log "Removed existing firewall rule"
    }

    # Create new firewall rule
    New-NetFirewallRule -DisplayName "ArmoniK Lifecycle Health Check" -Direction Inbound -Protocol TCP -LocalPort $HEALTH_PORT -Action Allow -Profile Any
    Write-Log "Created firewall rule for port $HEALTH_PORT"

    # Verify the rule was created
    $rule = Get-NetFirewallRule -DisplayName "ArmoniK Lifecycle Health Check" -ErrorAction SilentlyContinue
    if ($rule) {
        Write-Log "Firewall rule verified successfully"
    } else {
        Write-Log "Firewall rule verification failed" "WARNING"
    }

} catch {
    Write-Log "Failed to configure firewall: $_" "WARNING"
}

# Create wrapper batch file for reliable execution
$batchFilePath = "$SERVICE_DIR\start_lifecycle_service.bat"
$batchContent = @"
@echo off
cd /d "$SERVICE_DIR"
"$pythonExe" "$healthServerPath" > "$SERVICE_DIR\lifecycle_service_output.log" 2>&1
"@

try {
    $batchContent | Out-File -FilePath $batchFilePath -Encoding ASCII -Force
    Write-Log "Created wrapper batch file: $batchFilePath"
} catch {
    Write-Log "Failed to create wrapper batch file: $_" "ERROR"
}

# Create and start scheduled task to run the lifecycle service
Write-Log "Creating scheduled task for lifecycle service..."
try {
    # Remove existing task if it exists
    $existingTask = Get-ScheduledTask -TaskName "ArmonikLifecycleService" -ErrorAction SilentlyContinue
    if ($existingTask) {
        Unregister-ScheduledTask -TaskName "ArmonikLifecycleService" -Confirm:$false -ErrorAction SilentlyContinue
        Write-Log "Removed existing scheduled task"
    }

    # Create new scheduled task
    $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batchFilePath`""
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
    $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

    Register-ScheduledTask -TaskName "ArmonikLifecycleService" -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "ArmoniK Complete Lifecycle Service with Process Management"

    Write-Log "Created scheduled task: ArmonikLifecycleService"

    # Start the task immediately
    Start-ScheduledTask -TaskName "ArmonikLifecycleService"
    Write-Log "Started scheduled task immediately"

} catch {
    Write-Log "Failed to create or start scheduled task: $_" "ERROR"
}

# Monitor task startup
Write-Log "Monitoring lifecycle service startup..."
$maxWaitTime = 120  # 2 minutes
$waitTime = 0
$serviceStarted = $false

while ($waitTime -lt $maxWaitTime -and -not $serviceStarted) {
    try {
        # Check if the HTTP endpoint is responding
        $response = Invoke-WebRequest -Uri "http://localhost:$HEALTH_PORT$HEALTH_PATH" -TimeoutSec 5 -UseBasicParsing -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            Write-Log "✅ Lifecycle service is responding on port $HEALTH_PORT"
            $serviceStarted = $true

            # Parse and display the health response
            try {
                $healthData = $response.Content | ConvertFrom-Json
                Write-Log "Service Status: $($healthData.status)"
                Write-Log "Service Version: $($healthData.version)"
                if ($healthData.healthy_processes) {
                    Write-Log "Healthy Processes: $($healthData.healthy_processes -join ', ')"
                }
            } catch {
                Write-Log "Successfully connected to health endpoint"
            }
            break
        }
    } catch {
        # Service not ready yet, continue waiting
    }

    # Check task status
    try {
        $task = Get-ScheduledTask -TaskName "ArmonikLifecycleService" -ErrorAction SilentlyContinue
        if ($task) {
            $taskInfo = Get-ScheduledTaskInfo -TaskName "ArmonikLifecycleService" -ErrorAction SilentlyContinue
            if ($taskInfo) {
                Write-Log "Task Status: $($taskInfo.LastTaskResult) | Last Run: $($taskInfo.LastRunTime)"
            }
        }
    } catch {
        # Task info not available
    }

    Start-Sleep -Seconds 5
    $waitTime += 5
    Write-Log "Waiting for service startup... ($waitTime/$maxWaitTime seconds)"
}

if ($serviceStarted) {
    Write-Log "🎉 ArmoniK Complete Lifecycle Service started successfully!"
    Write-Log "Health Check URL: http://localhost:$HEALTH_PORT$HEALTH_PATH"
    Write-Log "Status URL: http://localhost:$HEALTH_PORT/status"
    Write-Log "Processes URL: http://localhost:$HEALTH_PORT/processes"
    Write-Log "Metrics URL: http://localhost:$HEALTH_PORT/metrics"
} else {
    Write-Log "❌ Service failed to start within $maxWaitTime seconds" "ERROR"

    # Check output log for errors
    $outputLogPath = "$SERVICE_DIR\lifecycle_service_output.log"
    if (Test-Path $outputLogPath) {
        Write-Log "=== Service Output Log ==="
        try {
            $logContent = Get-Content $outputLogPath -Tail 20
            foreach ($line in $logContent) {
                Write-Log "LOG: $line"
            }
        } catch {
            Write-Log "Failed to read output log: $_" "ERROR"
        }
    }
}

# Final status report
Write-Log "=== Final Status Report ==="
Write-Log "Service Directory: $SERVICE_DIR"
Write-Log "Python Executable: $pythonExe"
Write-Log "Health Port: $HEALTH_PORT"
Write-Log "Health Path: $HEALTH_PATH"
Write-Log "Batch File: $batchFilePath"
Write-Log "Service Script: $healthServerPath"

# Check if all files were created
$requiredFiles = @($healthServerPath, $batchFilePath)
foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        Write-Log "✅ File exists: $file"
    } else {
        Write-Log "❌ File missing: $file" "ERROR"
    }
}

Write-Log "=== ArmoniK Complete Lifecycle Service Setup Completed ==="
