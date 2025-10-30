"""
Metrics Service.
Collects and exports metrics to Prometheus.

Phase 1: Infrastructure scaffold.
"""

import asyncio
import signal
from typing import Optional

from prometheus_client import Counter, Histogram, Gauge
from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger

logger: Optional[object] = None


class MetricsService:
    """Metrics collection and export service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self._running = False

        # Define metrics
        self.messages_received = Counter(
            "messages_received_total",
            "Total messages received",
            ["service", "topic"],
        )

        self.quotes_generated = Counter(
            "quotes_generated_total",
            "Total quotes generated",
            ["symbol"],
        )

        self.order_latency = Histogram(
            "order_latency_ms",
            "Order execution latency",
            buckets=[1, 5, 10, 50, 100, 500, 1000],
        )

        self.position_qty = Gauge(
            "position_quantity",
            "Current position quantity",
            ["symbol"],
        )

        self.pnl = Gauge(
            "pnl_usd",
            "Current P&L in USD",
            ["symbol", "type"],  # type: realized or unrealized
        )

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting MetricsService")
        try:
            await self.nats.connect()

            # Subscribe to all metric messages
            await self.nats.subscribe("metrics.v1", self.on_metrics)

            self._running = True
            logger.info("MetricsService ready on port 8001")

        except Exception as e:
            logger.error("Failed to start MetricsService", error=str(e))
            raise

    async def on_metrics(self, msg) -> None:
        """Handle metrics message."""
        logger.debug("Received metrics")

        # Phase 2: Parse metrics message
        # Phase 3: Update Prometheus metrics

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping MetricsService")
        self._running = False
        await self.nats.close()
        logger.info("MetricsService stopped")

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
        "metrics_svc",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing MetricsService")

    service = MetricsService(config)

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