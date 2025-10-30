"""
Risk Manager Service.
Monitors stop-loss, inventory limits, funding rate, and other risk metrics.

Phase 1: Infrastructure scaffold.
"""

import asyncio
import signal
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None


class RiskManager:
    """Risk monitoring and enforcement service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.nats.url)
        self._running = False
        self._checks_performed = 0
        self._alerts_issued = 0
        self._errors = 0

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting RiskManager")
        try:
            await self.nats.connect()

            # Subscribe to inventory and market data
            await self.nats.subscribe("inventory.v1", self.on_inventory)
            await self.nats.subscribe("features.v1", self.on_features)

            self._running = True
            logger.info("RiskManager ready")

        except Exception as e:
            logger.error("Failed to start RiskManager", error=str(e))
            raise

    async def on_inventory(self, msg) -> None:
        """Monitor inventory against limits."""
        self._checks_performed += 1
        logger.debug("Monitoring inventory", check_count=self._checks_performed)

        # Phase 2: Parse inventory
        # Phase 3: Check against max inventory, stop-loss
        # Phase 3: Publish alerts if needed

    async def on_features(self, msg) -> None:
        """Monitor market conditions."""
        logger.debug("Monitoring market conditions")

        # Phase 2: Check funding rates
        # Phase 3: Check market volatility
        # Phase 3: Issue alerts for toxic market conditions

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping RiskManager")
        self._running = False
        await self.nats.close()
        logger.info(
            "RiskManager stopped",
            checks_performed=self._checks_performed,
            alerts_issued=self._alerts_issued,
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
        "risk_manager",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing RiskManager")

    service = RiskManager(config)

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