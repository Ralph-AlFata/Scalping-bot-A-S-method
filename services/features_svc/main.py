"""
Features Service.
Calculates order flow imbalance (OFI), micro-price, and queue imbalance.

Phase 1: Infrastructure scaffold - no actual feature calculation yet.
"""

import asyncio
import signal
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None


class FeaturesService:
    """Feature calculation service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.nats.url)
        self._running = False
        self._messages_processed = 0
        self._messages_published = 0
        self._errors = 0

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting FeaturesService")
        try:
            await self.nats.connect()

            # Subscribe to market data
            await self.nats.subscribe("raw.depth.v1", self.on_depth_message)
            await self.nats.subscribe("raw.trades.v1", self.on_trade_message)

            self._running = True
            logger.info("FeaturesService ready")

        except Exception as e:
            logger.error("Failed to start FeaturesService", error=str(e))
            raise

    async def on_depth_message(self, msg) -> None:
        """Handle depth update."""
        self._messages_processed += 1
        logger.debug("Received depth message", count=self._messages_processed)

        # Phase 2: Parse and calculate features
        # Phase 3: Publish features.v1

    async def on_trade_message(self, msg) -> None:
        """Handle trade message."""
        self._messages_processed += 1
        logger.debug("Received trade message", count=self._messages_processed)

        # Phase 2: Update trade statistics
        # Phase 3: Publish to OFI calculation

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping FeaturesService")
        self._running = False
        await self.nats.close()
        logger.info(
            "FeaturesService stopped",
            messages_processed=self._messages_processed,
            messages_published=self._messages_published,
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
        "features_svc",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing FeaturesService")

    service = FeaturesService(config)

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
