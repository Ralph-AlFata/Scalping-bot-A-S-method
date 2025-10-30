"""
Market Data Gateway Service.
Ingests WebSocket data from Binance and publishes to NATS.

Phase 1: Infrastructure scaffold - no actual Binance connection yet.
"""

import asyncio
import signal
import sys
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None


class MarketDataGateway:
    """Binance WebSocket ingestion service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self._running = False
        self._messages_published = 0
        self._errors = 0

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting MarketDataGateway service")
        try:
            await self.nats.connect()
            self._running = True
            logger.info("MarketDataGateway ready")

            # Phase 1: Just wait for shutdown
            # Phase 2: Connect to Binance WebSocket
            # Phase 3: Publish market data to NATS

        except Exception as e:
            logger.error("Failed to start MarketDataGateway", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping MarketDataGateway service")
        self._running = False
        await self.nats.close()
        logger.info("MarketDataGateway stopped")

    async def run(self) -> None:
        """Main service loop."""
        try:
            await self.start()

            # Phase 1: Just wait for shutdown signal
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
    # Setup logging
    global logger
    config = load_config()
    logger = setup_logging(
        "marketdata_gw",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing MarketDataGateway")

    # Create service
    service = MarketDataGateway(config)

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logger.warning("Received signal", signal=sig)
        asyncio.create_task(service.stop())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Run service
    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
