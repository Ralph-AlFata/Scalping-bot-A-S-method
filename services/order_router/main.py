"""
Order Router Service.
Places and cancels orders via Binance REST API.

Phase 1: Infrastructure scaffold.
"""

import asyncio
import signal
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger
from shared.sync_client import SyncClient

logger: Optional[object] = None


class OrderRouter:
    """Order placement and management service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self._running = False
        self._quotes_received = 0
        self._orders_placed = 0
        self._orders_cancelled = 0
        self._errors = 0

        # Initialize sync client
        self.sync_client = SyncClient(config, self.nats, "order_router")

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting OrderRouter")
        try:
            await self.nats.connect()

            # Start sync client
            await self.sync_client.start()

            # Subscribe to quotes
            await self.nats.subscribe("quotes.v1", self.on_quotes)

            self._running = True
            logger.info("OrderRouter ready")

        except Exception as e:
            logger.error("Failed to start OrderRouter", error=str(e))
            raise

    async def on_quotes(self, msg) -> None:
        """Handle quote update."""
        self._quotes_received += 1
        logger.debug("Received quotes to execute", count=self._quotes_received)

        # Phase 2: Parse quotes
        # Phase 3: Place orders via Binance REST API
        # Phase 3: Publish fill notifications

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping OrderRouter")
        self._running = False
        await self.sync_client.stop()
        await self.nats.close()
        logger.info(
            "OrderRouter stopped",
            quotes_received=self._quotes_received,
            orders_placed=self._orders_placed,
            orders_cancelled=self._orders_cancelled,
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
        "order_router",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing OrderRouter")

    service = OrderRouter(config)

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