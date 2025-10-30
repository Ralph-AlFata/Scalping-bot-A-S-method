"""
Volatility & Order Flow Service.
Estimates volatility, order intensity (k parameter), and VPIN.

Phase 1: Infrastructure scaffold.
"""

import asyncio
import signal
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None


class VolflowEstimator:
    """Volatility and order flow estimator service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self._running = False
        self._measurements = 0
        self._estimates_published = 0
        self._errors = 0

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting VolflowEstimator")
        try:
            await self.nats.connect()

            # Subscribe to market data
            await self.nats.subscribe("raw.trades.v1", self.on_trade)
            await self.nats.subscribe("raw.depth.v1", self.on_depth)

            self._running = True
            logger.info("VolflowEstimator ready")

        except Exception as e:
            logger.error("Failed to start VolflowEstimator", error=str(e))
            raise

    async def on_trade(self, msg) -> None:
        """Handle trade message."""
        self._measurements += 1
        logger.debug("Received trade for volflow", count=self._measurements)

        # Phase 2: Update volatility window
        # Phase 3: Calculate and publish volflow.v1

    async def on_depth(self, msg) -> None:
        """Handle depth message."""
        logger.debug("Received depth for volflow")

        # Phase 2: Update order book imbalance metrics
        # Phase 3: Calculate VPIN

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping VolflowEstimator")
        self._running = False
        await self.nats.close()
        logger.info(
            "VolflowEstimator stopped",
            measurements=self._measurements,
            estimates_published=self._estimates_published,
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
        "volflow_estimator",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing VolflowEstimator")

    service = VolflowEstimator(config)

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