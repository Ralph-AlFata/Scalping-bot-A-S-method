"""
Backtesting Service.
Simulates strategy performance on historical data.

Phase 1: Infrastructure scaffold.
Phase 4+: Implement backtesting engine.
"""

import asyncio
import signal
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None


class BacktestEngine:
    """Historical simulation and backtesting service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.nats.url)
        self._running = False
        self._simulations_run = 0
        self._errors = 0

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting BacktestEngine")
        try:
            await self.nats.connect()

            # Phase 2: Load historical data
            # Phase 3: Subscribe to live data for comparison

            self._running = True
            logger.info("BacktestEngine ready")

        except Exception as e:
            logger.error("Failed to start BacktestEngine", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping BacktestEngine")
        self._running = False
        await self.nats.close()
        logger.info(
            "BacktestEngine stopped",
            simulations_run=self._simulations_run,
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
        "sim_backtest",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing BacktestEngine")

    service = BacktestEngine(config)

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