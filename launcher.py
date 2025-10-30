#!/usr/bin/env python3
"""
Avellaneda-Stoikov Scalping Bot - System Launcher
Starts all 9 microservices with unified logging and lifecycle management.

Usage:
    python launcher.py                    # Start all services
    python launcher.py --services marketdata_gw,features_svc  # Start specific services
    python launcher.py --help             # Show help
"""

import asyncio
import os
import signal
import sys
import subprocess
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import argparse

# Color codes for terminal output
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # Foreground colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

# Service definitions with ports and descriptions
SERVICES = {
    "marketdata_gw": {
        "port": 8001,
        "description": "Market Data Gateway (Binance WebSocket ingestion)",
        "order": 1,
    },
    "features_svc": {
        "port": 8002,
        "description": "Features Service (OFI, micro-price, queue imbalance)",
        "order": 2,
    },
    "volflow_estimator": {
        "port": 8003,
        "description": "Volatility/Flow Estimator (σ, k-parameter, VPIN)",
        "order": 3,
    },
    "as_engine": {
        "port": 8004,
        "description": "Avellaneda-Stoikov Strategy Engine (quote generation)",
        "order": 4,
    },
    "order_router": {
        "port": 8005,
        "description": "Order Router (Binance REST API execution)",
        "order": 5,
    },
    "inventory_svc": {
        "port": 8006,
        "description": "Inventory Service (position tracking, P&L)",
        "order": 6,
    },
    "risk_manager": {
        "port": 8007,
        "description": "Risk Manager (stop-loss, limits, circuit breaker)",
        "order": 7,
    },
    "metrics_svc": {
        "port": 8008,
        "description": "Metrics Service (Prometheus export)",
        "order": 8,
    },
    "sim_backtest": {
        "port": 8009,
        "description": "Simulation/Backtest Engine",
        "order": 9,
    },
}


class ServiceProcess:
    """Manages a single service process."""

    def __init__(self, name: str, port: int, description: str):
        self.name = name
        self.port = port
        self.description = description
        self.process: Optional[subprocess.Popen] = None
        self.running = False
        self.start_time: Optional[datetime] = None

    def start(self, project_root: Path, venv_path: Path) -> bool:
        """Start the service process."""
        try:
            # Build the command
            python_exe = venv_path / "bin" / "python"
            module = f"services.{self.name}.main"

            # Use the absolute path and activate venv
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"

            # Start the process
            self.process = subprocess.Popen(
                [str(python_exe), "-m", module],
                cwd=str(project_root),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            self.running = True
            self.start_time = datetime.now()

            print(
                f"{Colors.GREEN}✓{Colors.RESET} {Colors.BOLD}{self.name}{Colors.RESET} "
                f"(PID: {self.process.pid}) started on port {self.port}"
            )
            return True

        except Exception as e:
            print(
                f"{Colors.RED}✗{Colors.RESET} {Colors.BOLD}{self.name}{Colors.RESET} "
                f"failed to start: {str(e)}"
            )
            return False

    def stop(self) -> bool:
        """Stop the service process gracefully."""
        if not self.process or not self.running:
            return True

        try:
            # Send SIGTERM for graceful shutdown
            self.process.terminate()

            # Wait up to 5 seconds for graceful shutdown
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if graceful shutdown didn't work
                self.process.kill()
                self.process.wait(timeout=2)

            self.running = False
            print(f"{Colors.YELLOW}⊘{Colors.RESET} {Colors.BOLD}{self.name}{Colors.RESET} stopped")
            return True

        except Exception as e:
            print(f"{Colors.RED}✗{Colors.RESET} Error stopping {self.name}: {str(e)}")
            return False

    def is_healthy(self) -> bool:
        """Check if process is still running."""
        if not self.process:
            return False

        if self.process.poll() is None:
            return True
        else:
            self.running = False
            return False

    def uptime_seconds(self) -> int:
        """Get uptime in seconds."""
        if not self.start_time:
            return 0
        return int((datetime.now() - self.start_time).total_seconds())


class SystemLauncher:
    """Manages the entire system launch and monitoring."""

    def __init__(self, project_root: Path, venv_path: Path, services_to_run: Optional[List[str]] = None):
        self.project_root = project_root
        self.venv_path = venv_path
        self.services: List[ServiceProcess] = []
        self.running = False

        # Initialize services
        if services_to_run is None:
            services_to_run = list(SERVICES.keys())

        # Sort by order and filter
        sorted_services = sorted(
            SERVICES.items(),
            key=lambda x: x[1]["order"]
        )

        for service_name, config in sorted_services:
            if service_name in services_to_run:
                service = ServiceProcess(
                    service_name,
                    config["port"],
                    config["description"]
                )
                self.services.append(service)

    def print_header(self):
        """Print system header."""
        print("\n" + "=" * 80)
        print(
            f"{Colors.BOLD}{Colors.CYAN}Avellaneda-Stoikov Scalping Bot - System Launcher{Colors.RESET}"
        )
        print("=" * 80)
        print(f"Project Root: {self.project_root}")
        print(f"Virtual Env:  {self.venv_path}")
        print(f"Python:       {self.venv_path / 'bin' / 'python'}")
        print(f"Services:     {len(self.services)}")
        print("=" * 80 + "\n")

    def print_service_info(self):
        """Print service information table."""
        print(f"{Colors.BOLD}Services to Start:{Colors.RESET}\n")
        print(
            f"{'#':<3} {'Service':<25} {'Port':<6} {'Description':<45}"
        )
        print("-" * 80)

        for i, service in enumerate(self.services, 1):
            print(
                f"{i:<3} {service.name:<25} {service.port:<6} {service.description:<45}"
            )
        print()

    async def start_all(self) -> bool:
        """Start all services."""
        print(f"{Colors.BOLD}Starting services...{Colors.RESET}\n")

        failed_services = []

        for service in self.services:
            if not service.start(self.project_root, self.venv_path):
                failed_services.append(service.name)
            await asyncio.sleep(0.5)  # Stagger starts slightly

        if failed_services:
            print(
                f"\n{Colors.RED}{Colors.BOLD}Failed to start:{Colors.RESET} "
                f"{', '.join(failed_services)}"
            )
            return False

        print(
            f"\n{Colors.GREEN}{Colors.BOLD}✓ All services started successfully!{Colors.RESET}\n"
        )
        return True

    async def monitor_services(self):
        """Monitor services and report status."""
        print(f"{Colors.BOLD}Monitoring services...{Colors.RESET}\n")

        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds

                # Check for crashed services
                crashed = []
                for service in self.services:
                    if service.running and not service.is_healthy():
                        crashed.append(service)

                if crashed:
                    print(
                        f"\n{Colors.RED}{Colors.BOLD}⚠ Services crashed:{Colors.RESET}"
                    )
                    for service in crashed:
                        print(f"  - {service.name}")

                    # Attempt to restart
                    print(f"\n{Colors.YELLOW}Attempting to restart...{Colors.RESET}\n")
                    for service in crashed:
                        service.start(self.project_root, self.venv_path)
                        await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"{Colors.RED}Monitoring error: {str(e)}{Colors.RESET}")

    def print_status(self):
        """Print current system status."""
        print("\n" + "=" * 80)
        print(f"{Colors.BOLD}System Status{Colors.RESET}")
        print("=" * 80)
        print(
            f"{'Service':<25} {'PID':<8} {'Port':<6} {'Status':<12} {'Uptime':<10}"
        )
        print("-" * 80)

        for service in self.services:
            status = (
                f"{Colors.GREEN}Running{Colors.RESET}"
                if service.is_healthy()
                else f"{Colors.RED}Stopped{Colors.RESET}"
            )
            pid = service.process.pid if service.process else "N/A"
            uptime = f"{service.uptime_seconds()}s" if service.is_healthy() else "N/A"

            print(
                f"{service.name:<25} {str(pid):<8} {service.port:<6} {status:<12} {uptime:<10}"
            )

        print("=" * 80)

    async def handle_shutdown(self, signum):
        """Handle graceful shutdown."""
        print(
            f"\n\n{Colors.YELLOW}{Colors.BOLD}Received signal {signum}, shutting down...{Colors.RESET}\n"
        )
        self.running = False

        print(f"{Colors.BOLD}Stopping services...{Colors.RESET}\n")

        # Stop all services
        for service in reversed(self.services):  # Stop in reverse order
            service.stop()

        print(
            f"\n{Colors.YELLOW}{Colors.BOLD}Launcher terminated.{Colors.RESET}\n"
        )
        sys.exit(0)

    async def run(self):
        """Main launcher loop."""
        self.print_header()
        self.print_service_info()

        # Setup signal handlers
        loop = asyncio.get_event_loop()

        def signal_handler(signum):
            asyncio.create_task(self.handle_shutdown(signum))

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler, sig)

        # Start all services
        if not await self.start_all():
            print(f"{Colors.RED}Failed to start all services{Colors.RESET}")
            sys.exit(1)

        self.running = True

        # Print startup info
        print(f"{Colors.BOLD}Available Endpoints:{Colors.RESET}\n")
        print("  Prometheus:  http://localhost:9090")
        print("  Grafana:     http://localhost:3000 (admin/admin)")
        print("  NATS:        nats://localhost:4222")
        print("  Redis:       localhost:6379")
        print("  TimescaleDB: localhost:5432")
        print()
        print(f"{Colors.BOLD}Service Ports:{Colors.RESET}")
        for service in self.services:
            print(f"  {service.name:<25} http://localhost:{service.port}")
        print()

        # Print status periodically
        status_task = asyncio.create_task(self.status_printer())
        monitor_task = asyncio.create_task(self.monitor_services())

        try:
            # Keep the launcher running
            await asyncio.gather(status_task, monitor_task)
        except asyncio.CancelledError:
            pass

    async def status_printer(self):
        """Print status updates periodically."""
        try:
            while self.running:
                await asyncio.sleep(30)
                self.print_status()
        except asyncio.CancelledError:
            pass


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Launch Avellaneda-Stoikov Scalping Bot system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python launcher.py                    # Start all services
  python launcher.py --services marketdata_gw,features_svc  # Start specific services
  python launcher.py --list             # List available services
        """,
    )

    parser.add_argument(
        "--services",
        type=str,
        help="Comma-separated list of services to start (default: all)",
        default=None,
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available services and exit",
    )

    args = parser.parse_args()

    # Get project root
    project_root = Path(__file__).parent.absolute()
    venv_path = project_root / ".venv"

    # List services if requested
    if args.list:
        print("\n" + "=" * 80)
        print(f"{Colors.BOLD}{Colors.CYAN}Available Services{Colors.RESET}")
        print("=" * 80 + "\n")

        sorted_services = sorted(
            SERVICES.items(),
            key=lambda x: x[1]["order"]
        )

        for service_name, config in sorted_services:
            print(f"{Colors.BOLD}{service_name}{Colors.RESET}")
            print(f"  Port:        {config['port']}")
            print(f"  Description: {config['description']}\n")

        return 0

    # Parse services to run
    services_to_run = None
    if args.services:
        services_to_run = [s.strip() for s in args.services.split(",")]

        # Validate services
        invalid = set(services_to_run) - set(SERVICES.keys())
        if invalid:
            print(f"{Colors.RED}Error: Invalid services: {', '.join(invalid)}{Colors.RESET}")
            print(f"Run with --list to see available services")
            return 1

    # Verify virtual environment exists
    if not venv_path.exists():
        print(f"{Colors.RED}Error: Virtual environment not found at {venv_path}{Colors.RESET}")
        print(f"Please run: python -m venv .venv")
        return 1

    # Create launcher and run
    launcher = SystemLauncher(project_root, venv_path, services_to_run)

    try:
        asyncio.run(launcher.run())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Launcher interrupted{Colors.RESET}")
        return 0
    except Exception as e:
        print(f"{Colors.RED}Error: {str(e)}{Colors.RESET}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
