"""
Inventory Service.
Tracks position, calculates P&L, and monitors inventory limits.

Phase 1: Infrastructure scaffold.
"""

import asyncio
import signal
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None


class InventoryService:
    """Position tracking and P&L calculation service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.nats.url)
        self._running = False
        self._fills_processed = 0
        self._inventory_updates = 0
        self._errors = 0

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting InventoryService")
        try:
            await self.nats.connect()

            # Subscribe to trade fills
            await self.nats.subscribe("fill.v1", self.on_fill)
            await self.nats.subscribe("cancelled.v1", self.on_cancelled)

            self._running = True
            logger.info("InventoryService ready")

        except Exception as e:
            logger.error("Failed to start InventoryService", error=str(e))
            raise

    async def on_fill(self, msg) -> None:
        """Handle order fill."""
        self._fills_processed += 1
        logger.debug("Received order fill", count=self._fills_processed)

        # Phase 2: Update position and P&L
        # Phase 3: Publish inventory.v1

    async def on_cancelled(self, msg) -> None:
        """Handle order cancellation."""
        logger.debug("Received order cancelled")

        # Phase 2: Log cancellation
        # Phase 3: Update order tracking

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping InventoryService")
        self._running = False
        await self.nats.close()
        logger.info(
            "InventoryService stopped",
            fills_processed=self._fills_processed,
            inventory_updates=self._inventory_updates,
            errors=self._errors,
        )

    async def run(self) -> None:
        """Main service loop."""
        try:
            await self.start()
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Service cancelled")
        except Exception as e:
            logger.error("Service error", error=str(e))
        finally:
            await self.stop()


async def main() -> None:
    """Main entry point."""
    global logger
    config = load_config()
    logger = setup_logging(
        "inventory_svc",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing InventoryService")

    service = InventoryService(config)

    def signal_handler(sig, frame):
        logger.warning("Received signal", signal=sig)
        asyncio.create_task(service.stop())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())