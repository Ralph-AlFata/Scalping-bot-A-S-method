"""
Sync Clock Service - Entry Point.

This service maintains the authoritative clock for the entire system.
It should be started before other services and ensures all enabled services
are synchronized at regular intervals.

Start with:
    python services/sync_clock_service/main.py

The service will:
1. Read enabled services from config.yaml
2. Wait for each service to register
3. Orchestrate synchronization every update_freq_ms (default 100ms)
4. Publish sync.clock.v1 events when all services are ready
"""

import asyncio
import signal
from typing import Optional

from shared.sync_clock import SyncClockService
from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None
sync_service: Optional[SyncClockService] = None


async def main() -> None:
    """Main entry point for sync clock service."""
    global logger, sync_service

    # Load configuration and setup logging
    config = load_config()
    logger = setup_logging(
        "sync_clock_service",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("=" * 80)
    logger.info("Sync Clock Service Starting")
    logger.info("=" * 80)

    # Initialize NATS client
    nats_client = NATSClient(config.infrastructure.nats.url)

    try:
        # Connect to NATS
        logger.info(f"Connecting to NATS at {config.infrastructure.nats.url}")
        await nats_client.connect()
        logger.info("Connected to NATS")

        # Create and start sync clock service
        sync_service = SyncClockService(config, nats_client)

        # Log expected services
        logger.info(f"Expected services: {sync_service.expected_services}")
        logger.info(f"Sync interval: {sync_service.sync_interval_ms}ms")
        logger.info(f"Sync mode: {sync_service.sync_mode.value}")

        # Register signal handlers
        def signal_handler(sig, frame):
            logger.warning(f"Received signal {sig}, shutting down")
            asyncio.create_task(sync_service.stop())

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Start the sync service
        logger.info("Starting sync clock service...")
        await sync_service.start()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in sync clock service: {e}")
        raise
    finally:
        logger.info("Closing NATS connection")
        await nats_client.close()
        logger.info("Sync Clock Service Stopped")


if __name__ == "__main__":
    asyncio.run(main())
