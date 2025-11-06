"""
Global Clock Synchronization Service.

This module provides distributed clock synchronization for the market-making system.
It ensures that all running services operate at synchronized time boundaries, preventing
race conditions and data misalignment in multi-service deployments.

Architecture:
- Central sync clock service that maintains the authoritative time
- Services register with the sync service and wait for sync points
- Supports multiple operation modes: real-time, virtual time, and hybrid
- Graceful degradation: missing services are skipped (flexible partial runs)

Message Flow:
1. Service starts → registers with sync_clock_service
2. Service receives data → calls sync_client.wait_for_sync_point()
3. Sync service checks if all enabled services are ready
4. When ready → publishes sync.ready.v1 event with sync point timestamp
5. All services proceed with that timestamp for calculations
6. Next cycle: Service publishes sync.ready.v1 message for its own data
"""

import asyncio
import json
import time
from typing import Optional, Dict, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from nats.aio.msg import Msg
from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = get_logger(__name__)


class SyncMode(str, Enum):
    """Operating mode for the sync clock."""
    REAL_TIME = "real_time"          # Wall-clock time, all services sync together
    VIRTUAL_TIME = "virtual_time"    # Virtual/simulated time for backtesting
    HYBRID = "hybrid"                 # External time source (for audit/replay)


@dataclass
class ServiceRegistration:
    """Registration info for a service."""
    service_name: str
    enabled: bool = True
    last_heartbeat_ms: float = field(default_factory=lambda: time.time() * 1000)
    is_ready: bool = False
    last_sync_point_ms: float = 0.0
    last_ready_timestamp_ms: float = 0.0  # Timestamp from the last ready signal


class SyncClockService:
    """
    Central clock synchronization service.

    Maintains consistent time boundaries across all running services.
    Services call sync_client.wait_for_sync_point() and this service
    ensures all enabled services are synchronized before proceeding.
    """

    def __init__(self, config, nats_client: NATSClient):
        """
        Initialize sync clock service.

        Args:
            config: Configuration object
            nats_client: NATS client for message publishing
        """
        self.config = config
        self.nats = nats_client

        # Sync configuration
        self.sync_mode: SyncMode = SyncMode.REAL_TIME
        self.sync_interval_ms: int = config.strategy.quoting.update_freq_ms  # Default: 100ms
        self.heartbeat_timeout_ms: float = config.monitoring.health_check_interval_sec * 1000

        # Expected services (read from config)
        self.expected_services: Set[str] = self._load_expected_services()

        # Runtime state
        self.registered_services: Dict[str, ServiceRegistration] = {}
        self.current_sync_point_ms: float = 0.0
        self.last_sync_time_ms: float = time.time() * 1000
        self.sync_counter: int = 0
        self.is_running: bool = False

    def _load_expected_services(self) -> Set[str]:
        """Load expected services from config based on enabled status."""
        expected = set()
        if self.config.services.marketdata_gw.enabled:
            expected.add("marketdata_gw")
        if self.config.services.features_svc.enabled:
            expected.add("features_svc")
        if self.config.services.volflow_estimator.enabled:
            expected.add("volflow_estimator")
        if self.config.services.as_engine.enabled:
            expected.add("as_engine")
        if self.config.services.order_router.enabled:
            expected.add("order_router")
        if self.config.services.inventory_svc.enabled:
            expected.add("inventory_svc")
        if self.config.services.risk_manager.enabled:
            expected.add("risk_manager")
        if self.config.services.metrics_svc.enabled:
            expected.add("metrics_svc")
        return expected

    async def start(self) -> None:
        """Start the sync clock service."""
        logger.info("Starting sync clock service", mode=self.sync_mode.value)
        self.is_running = True

        try:
            # Subscribe to service registrations
            await self.nats.subscribe("sync.register.v1", self._handle_service_register)

            # Subscribe to ready signals from services
            await self.nats.subscribe("sync.ready.v1", self._handle_service_ready)

            # Subscribe to heartbeats for liveness detection
            await self.nats.subscribe("sync.heartbeat.v1", self._handle_heartbeat)

            # Start the main synchronization loop
            await self._sync_loop()

        except Exception as e:
            logger.error("Sync clock service error", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the sync clock service."""
        logger.info("Stopping sync clock service")
        self.is_running = False

    async def _handle_service_register(self, msg: Msg) -> None:
        """
        Handle service registration.

        Args:
            msg: NATS message object
        """
        try:
            data = json.loads(msg.data.decode())
            service_name = data.get("service_name")

            if not service_name:
                logger.warning("Invalid registration message, missing service_name")
                return

            is_enabled = service_name in self.expected_services

            self.registered_services[service_name] = ServiceRegistration(
                service_name=service_name,
                enabled=is_enabled,
                last_heartbeat_ms=time.time() * 1000,
            )

            logger.info(
                "Service registered",
                service_name=service_name,
                enabled=is_enabled,
                total_services=len(self.registered_services),
            )

        except Exception as e:
            logger.error("Error handling service registration", error=str(e))

    async def _handle_service_ready(self, msg: Msg) -> None:
        """
        Handle service ready signal.

        Called when a service has new data and is ready to sync.

        Args:
            msg: NATS message object
        """
        try:
            data = json.loads(msg.data.decode())
            service_name = data.get("service_name")
            timestamp_ms = data.get("timestamp_ms", time.time() * 1000)

            if not service_name:
                logger.warning("Invalid ready message, missing service_name")
                return

            if service_name not in self.registered_services:
                logger.warning("Ready signal from unregistered service", service_name=service_name)
                return

            # Mark service as ready and capture timestamp
            reg = self.registered_services[service_name]
            reg.is_ready = True
            reg.last_heartbeat_ms = time.time() * 1000
            reg.last_ready_timestamp_ms = timestamp_ms  # Capture data timestamp for sync point

            logger.debug(
                "Service ready signal received",
                service_name=service_name,
                timestamp_ms=timestamp_ms,
            )

        except Exception as e:
            logger.error("Error handling service ready signal", error=str(e))

    async def _handle_heartbeat(self, msg: Msg) -> None:
        """
        Handle service heartbeat for liveness detection.

        Args:
            msg: NATS message object
        """
        try:
            data = json.loads(msg.data.decode())
            service_name = data.get("service_name")

            if service_name in self.registered_services:
                self.registered_services[service_name].last_heartbeat_ms = time.time() * 1000

        except Exception as e:
            logger.error("Error handling heartbeat", error=str(e))

    async def _sync_loop(self) -> None:
        """
        Main synchronization loop.

        Continuously checks if all enabled services are ready.
        When they are, publishes a sync point and resets ready flags.
        """
        while self.is_running:
            try:
                # Check which services are ready
                enabled_services = {
                    name: reg for name, reg in self.registered_services.items()
                    if reg.enabled
                }

                # If no services are registered yet, wait
                if not enabled_services:
                    await asyncio.sleep(0.1)
                    continue

                # Check if all enabled services are ready
                all_ready = all(reg.is_ready for reg in enabled_services.values())

                if all_ready:
                    # Generate sync point
                    await self._publish_sync_point()

                    # Reset ready flags for next cycle
                    for reg in self.registered_services.values():
                        reg.is_ready = False

                # Check for stale services (heartbeat timeout)
                self._check_stale_services()

                await asyncio.sleep(0.01)  # 10ms check interval

            except Exception as e:
                logger.error("Error in sync loop", error=str(e))
                await asyncio.sleep(0.1)

    async def _publish_sync_point(self) -> None:
        """Publish a sync point event."""
        try:
            # marketdata_gw is the clock source - use the timestamp from the last ready signal
            # This ensures all services synchronize to actual market data arrival times
            marketdata_reg = self.registered_services.get("marketdata_gw")

            if marketdata_reg and hasattr(marketdata_reg, 'last_ready_timestamp_ms'):
                # Use marketdata_gw's timestamp as the authoritative sync point
                sync_point_ms = int(marketdata_reg.last_ready_timestamp_ms)
            else:
                # Fallback: use current time if marketdata_gw hasn't sent ready signal yet
                sync_point_ms = int(time.time() * 1000)

            # Publish sync point
            message = {
                "sync_point_ms": sync_point_ms,
                "sync_counter": self.sync_counter,
                "mode": self.sync_mode.value,
                "num_services": len([s for s in self.registered_services.values() if s.enabled]),
                "timestamp_ms": time.time() * 1000,
            }

            await self.nats.publish("sync.clock.v1", message)

            self.current_sync_point_ms = sync_point_ms
            self.last_sync_time_ms = time.time() * 1000
            self.sync_counter += 1

            logger.info(
                "Sync point published (marketdata_gw-driven)",
                sync_point_ms=sync_point_ms,
                sync_counter=self.sync_counter,
                source="marketdata_gw",
            )

        except Exception as e:
            logger.error("Error publishing sync point", error=str(e))

    def _check_stale_services(self) -> None:
        """Check for services that haven't sent heartbeats (stale)."""
        current_ms = time.time() * 1000
        for service_name, reg in self.registered_services.items():
            time_since_heartbeat_ms = current_ms - reg.last_heartbeat_ms
            if time_since_heartbeat_ms > self.heartbeat_timeout_ms and reg.enabled:
                logger.warning(
                    "Service heartbeat timeout",
                    service_name=service_name,
                    time_since_heartbeat_ms=time_since_heartbeat_ms,
                )

    def get_status(self) -> Dict:
        """Get current sync status."""
        return {
            "mode": self.sync_mode.value,
            "sync_counter": self.sync_counter,
            "current_sync_point_ms": self.current_sync_point_ms,
            "registered_services": {
                name: {
                    "enabled": reg.enabled,
                    "is_ready": reg.is_ready,
                    "last_heartbeat_ms": reg.last_heartbeat_ms,
                }
                for name, reg in self.registered_services.items()
            },
        }


async def main():
    """Run the sync clock service."""
    global logger

    # Load configuration and setup logging
    config = load_config()
    setup_logging(config)
    logger = get_logger(__name__)

    # Initialize NATS client
    nats_client = NATSClient(config)
    await nats_client.connect()

    try:
        # Create and start sync clock service
        sync_service = SyncClockService(config, nats_client)
        await sync_service.start()

    except KeyboardInterrupt:
        logger.info("Sync clock service interrupted")
    except Exception as e:
        logger.error("Fatal error in sync clock service", error=str(e))
    finally:
        await nats_client.close()


if __name__ == "__main__":
    asyncio.run(main())
