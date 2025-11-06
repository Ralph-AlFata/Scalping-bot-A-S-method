"""
Sync Clock Client Library.

This module provides the client-side interface for services to synchronize with
the global clock. Services call wait_for_sync_point() to ensure all enabled services
are synchronized before processing messages.

Usage in a service:

    from shared.sync_client import SyncClient

    sync_client = SyncClient(config, nats_client, service_name="marketdata_gw")
    await sync_client.start()

    # When you have new data to process:
    sync_point_ms = await sync_client.wait_for_sync_point(data_timestamp_ms)

    # Now all services are at sync_point_ms, safe to proceed
    # Process your data with this synchronized timestamp
"""

import asyncio
import json
import time
from typing import Optional, Callable, Awaitable
from dataclasses import dataclass

from nats.aio.msg import Msg
from shared.nats_client import NATSClient
from shared.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SyncPointEvent:
    """A synchronization point event."""
    sync_point_ms: float
    sync_counter: int
    mode: str
    num_services: int
    timestamp_ms: float


class SyncClient:
    """
    Client for synchronizing with the global clock service.

    Each service instance creates one SyncClient and uses it to:
    1. Register with the sync clock service
    2. Signal when it has new data ready
    3. Wait for all other services to be ready
    4. Receive synchronized timestamps
    """

    def __init__(self, config, nats_client: NATSClient, service_name: str):
        """
        Initialize sync client.

        Args:
            config: Configuration object
            nats_client: NATS client for messaging
            service_name: Name of the service (e.g., "marketdata_gw")
        """
        self.config = config
        self.nats = nats_client
        self.service_name = service_name

        # Sync configuration
        self.sync_interval_ms = config.strategy.quoting.update_freq_ms
        self.heartbeat_interval_sec = max(1, config.monitoring.health_check_interval_sec // 10)

        # State
        self.is_registered = False
        self.current_sync_point_ms: Optional[float] = None
        self.sync_counter: int = 0
        self.last_heartbeat_ms: float = time.time() * 1000
        self.is_running = False

        # Callbacks
        self._on_sync_point_callback: Optional[Callable[[SyncPointEvent], Awaitable[None]]] = None

    async def start(self) -> None:
        """
        Start the sync client.

        Should be called when the service starts. This will:
        1. Register with the sync clock service
        2. Subscribe to sync points
        3. Start heartbeat timer
        """
        logger.info(f"Starting sync client for {self.service_name}")
        self.is_running = True

        try:
            # Register with sync clock service
            await self._register()

            # Subscribe to sync points
            await self.nats.subscribe("sync.clock.v1", self._on_sync_clock)

            # Start heartbeat task
            asyncio.create_task(self._heartbeat_loop())

            logger.info(f"Sync client started for {self.service_name}")

        except Exception as e:
            logger.error(f"Error starting sync client for {self.service_name}", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the sync client."""
        logger.info(f"Stopping sync client for {self.service_name}")
        self.is_running = False

    async def _register(self) -> None:
        """Register this service with the sync clock service."""
        try:
            message = {"service_name": self.service_name}
            await self.nats.publish("sync.register.v1", message)
            self.is_registered = True
            logger.info(f"Service {self.service_name} registered with sync clock")
        except Exception as e:
            logger.error(f"Error registering {self.service_name}", error=str(e))
            raise

    async def wait_for_sync_point(self, data_timestamp_ms: Optional[float] = None) -> float:
        """
        Wait for all enabled services to be ready at the same sync point.

        This method should be called when your service has new data and is ready
        to pass it to downstream services. It signals to the sync clock that you're
        ready, then waits for all other enabled services to be ready too. When all
        are ready, you receive a synchronized timestamp to use for processing.

        Args:
            data_timestamp_ms: Timestamp of the data you're about to process.
                             If None, uses current time.

        Returns:
            The synchronized sync point timestamp (in milliseconds)
        """
        if not self.is_registered:
            logger.warning(f"{self.service_name}: Not registered, skipping sync")
            return data_timestamp_ms or time.time() * 1000

        if data_timestamp_ms is None:
            data_timestamp_ms = time.time() * 1000

        try:
            # Signal that we're ready with new data
            await self.signal_ready(data_timestamp_ms)

            # Wait for sync point from the clock service
            sync_point_ms = await self._wait_for_sync_point_event()

            logger.debug(
                f"{self.service_name}: Received sync point",
                sync_point_ms=sync_point_ms,
                data_timestamp_ms=data_timestamp_ms,
            )

            return sync_point_ms

        except Exception as e:
            logger.error(f"Error in wait_for_sync_point for {self.service_name}", error=str(e))
            # Degrade gracefully: return the data timestamp
            return data_timestamp_ms

    async def signal_ready(self, data_timestamp_ms: float) -> None:
        """
        Signal to the sync clock that this service is ready with new data.

        Called by wait_for_sync_point internally.

        Args:
            data_timestamp_ms: Timestamp of the data ready for processing
        """
        try:
            message = {
                "service_name": self.service_name,
                "timestamp_ms": data_timestamp_ms,
            }
            await self.nats.publish("sync.ready.v1", message)

        except Exception as e:
            logger.error(f"Error signaling ready for {self.service_name}", error=str(e))

    async def _wait_for_sync_point_event(self, timeout_sec: float = 1.0) -> float:
        """
        Wait for a sync point event from the clock service.

        Args:
            timeout_sec: Timeout in seconds

        Returns:
            Synchronized timestamp from the sync point
        """
        start_time = time.time()

        while time.time() - start_time < timeout_sec:
            if self.current_sync_point_ms is not None:
                result = self.current_sync_point_ms
                self.current_sync_point_ms = None
                return result

            await asyncio.sleep(0.001)  # 1ms check interval

        # Timeout: return current time as fallback
        logger.warning(
            f"{self.service_name}: Sync point timeout, using current time",
            timeout_sec=timeout_sec,
        )
        return time.time() * 1000

    async def _on_sync_clock(self, msg: Msg) -> None:
        """
        Handle sync clock event from the sync clock service.

        Args:
            msg: NATS message object
        """
        try:
            data = json.loads(msg.data.decode())
            sync_point_ms = data.get("sync_point_ms")
            sync_counter = data.get("sync_counter")
            mode = data.get("mode")

            if sync_point_ms is None:
                logger.warning(f"{self.service_name}: Invalid sync clock message")
                return

            # Store for wait_for_sync_point to pick up
            self.current_sync_point_ms = sync_point_ms
            self.sync_counter = sync_counter

            # If a callback is registered, call it
            if self._on_sync_point_callback:
                event = SyncPointEvent(
                    sync_point_ms=sync_point_ms,
                    sync_counter=sync_counter,
                    mode=mode,
                    num_services=data.get("num_services", 0),
                    timestamp_ms=data.get("timestamp_ms", 0),
                )
                await self._on_sync_point_callback(event)

        except Exception as e:
            logger.error(f"Error handling sync clock message for {self.service_name}", error=str(e))

    async def _heartbeat_loop(self) -> None:
        """
        Send periodic heartbeats to the sync clock service.

        This helps the sync clock detect if a service becomes unresponsive.
        """
        while self.is_running:
            try:
                message = {"service_name": self.service_name}
                await self.nats.publish("sync.heartbeat.v1", message)
                self.last_heartbeat_ms = time.time() * 1000

                await asyncio.sleep(self.heartbeat_interval_sec)

            except Exception as e:
                logger.error(f"Error sending heartbeat for {self.service_name}", error=str(e))
                await asyncio.sleep(1)

    def set_sync_point_callback(self, callback: Callable[[SyncPointEvent], Awaitable[None]]) -> None:
        """
        Register a callback to be called when a sync point is received.

        This is optional and allows services to react to sync points.

        Args:
            callback: Async function that takes a SyncPointEvent
        """
        self._on_sync_point_callback = callback

    def get_status(self) -> dict:
        """Get sync client status."""
        return {
            "service_name": self.service_name,
            "is_registered": self.is_registered,
            "is_running": self.is_running,
            "sync_counter": self.sync_counter,
            "current_sync_point_ms": self.current_sync_point_ms,
            "last_heartbeat_ms": self.last_heartbeat_ms,
        }
