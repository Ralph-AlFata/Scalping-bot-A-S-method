"""
Market Data Gateway Service.
Ingests WebSocket data from Binance and publishes to NATS.

Phase 2: Binance WebSocket ingestion with depth and trade streams.

Purpose: Ingest real-time market data from Binance Futures WebSocket API.

Responsibilities:
    - Establish and maintain WebSocket connections to Binance
    - Subscribe to order book depth streams (100ms snapshots)
    - Subscribe to aggregated trade streams
    - Parse and normalize incoming messages
    - Publish standardized events to NATS topics
    - Handle reconnection on connection loss
    - Monitor connection health

Inputs:
    Binance WebSocket streams:
        - btcusdt@depth@100ms (order book L2)
        - btcusdt@aggTrade (aggregated trades)
Outputs (NATS Topics):
    - raw.depth.v1 - Order book snapshots
    - raw.trades.v1 - Trade events
"""

import asyncio
import signal
import sys
import json
import time
from typing import Optional, Dict, Any

import websockets

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger
from shared.schemas import DepthSnapshot, TradeMessage
from shared.sync_client import SyncClient

logger: Optional[object] = None


class BinanceWebSocketManager:
    """Manager for Binance WebSocket streams."""

    def __init__(self, config, nats_client, on_depth_callback, on_trade_callback):
        """
        Initialize Binance WebSocket manager.

        Args:
            config: Configuration object.
            nats_client: NATS client for sync coordination.
            on_depth_callback: Async callback for depth messages.
            on_trade_callback: Async callback for trade messages.
        """
        self.config = config
        self.nats_client = nats_client
        self.on_depth_callback = on_depth_callback
        self.on_trade_callback = on_trade_callback
        self.symbol = config.system.symbol.lower()

        # Determine WS URL based on testnet setting
        if config.binance.testnet:
            self.ws_base_url = "wss://stream.binancefuture.com"
        else:
            self.ws_base_url = "wss://fstream.binance.com"

        self._running = False
        self._ws_depth = None
        self._ws_trades = None
        self._reconnect_delay = 1  # Start with 1s backoff
        self._max_reconnect_delay = 30
        self._errors = 0

        # Initialize sync client
        self.sync_client = SyncClient(config, nats_client, "marketdata_gw")

        # Sync batching: accumulate messages and sync once per cycle
        self.sync_interval_ms = config.strategy.quoting.update_freq_ms
        self.last_sync_ms = time.time() * 1000
        self.current_sync_point_ms = None

    async def start(self) -> None:
        """Start WebSocket streams."""
        logger.info("Starting Binance WebSocket streams")
        self._running = True

        # Start sync client
        await self.sync_client.start()

        # Start both streams concurrently
        depth_task = asyncio.create_task(self._stream_depth())
        trades_task = asyncio.create_task(self._stream_trades())

        # Wait for both (they run until _running is False)
        await asyncio.gather(depth_task, trades_task)

    async def stop(self) -> None:
        """Stop WebSocket streams."""
        logger.info("Stopping Binance WebSocket streams")
        self._running = False

        # Stop sync client
        await self.sync_client.stop()

        if self._ws_depth:
            await self._ws_depth.close()
        if self._ws_trades:
            await self._ws_trades.close()

    async def _stream_depth(self) -> None:
        """Subscribe to depth stream with auto-reconnect."""
        while self._running:
            try:
                # Format: symbol@depth<levels>@<speed>
                # 100ms updates, 20 levels
                stream_name = f"{self.symbol}@depth20@100ms"
                ws_url = f"{self.ws_base_url}/ws/{stream_name}"

                logger.info("Connecting to depth stream", url=ws_url)

                async with websockets.connect(ws_url, ping_interval=None) as ws:
                    self._ws_depth = ws
                    self._reconnect_delay = 1  # Reset on successful connect

                    async for message in ws:
                        if not self._running:
                            break

                        try:
                            data = json.loads(message)
                            await self._process_depth_message(data)
                        except json.JSONDecodeError:
                            logger.error("Failed to decode depth message", raw=message)
                            self._errors += 1
                        except Exception as e:
                            logger.error("Error processing depth message", error=str(e))
                            self._errors += 1

            except asyncio.CancelledError:
                logger.info("Depth stream task cancelled")
                break
            except Exception as e:
                logger.warning(
                    "Depth stream error, reconnecting",
                    error=str(e),
                    delay_sec=self._reconnect_delay
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    async def _stream_trades(self) -> None:
        """Subscribe to trades stream with auto-reconnect."""
        while self._running:
            try:
                # Format: symbol@aggTrade
                stream_name = f"{self.symbol}@aggTrade"
                ws_url = f"{self.ws_base_url}/ws/{stream_name}"

                logger.info("Connecting to trades stream", url=ws_url)

                async with websockets.connect(ws_url, ping_interval=None) as ws:
                    self._ws_trades = ws
                    self._reconnect_delay = 1  # Reset on successful connect

                    async for message in ws:
                        if not self._running:
                            break

                        try:
                            data = json.loads(message)
                            await self._process_trade_message(data)
                        except json.JSONDecodeError:
                            logger.error("Failed to decode trade message", raw=message)
                            self._errors += 1
                        except Exception as e:
                            logger.error("Error processing trade message", error=str(e))
                            self._errors += 1

            except asyncio.CancelledError:
                logger.info("Trades stream task cancelled")
                break
            except Exception as e:
                logger.warning(
                    "Trades stream error, reconnecting",
                    error=str(e),
                    delay_sec=self._reconnect_delay
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    async def _get_sync_point(self, data_timestamp_ms: float) -> float:
        """
        Get current sync point, requesting new one if needed.

        Uses batching to only request a new sync point every sync_interval_ms,
        rather than once per message.
        """
        current_time_ms = time.time() * 1000
        time_since_sync = current_time_ms - self.last_sync_ms

        # If we haven't synced recently, request a new sync point
        if time_since_sync >= self.sync_interval_ms or self.current_sync_point_ms is None:
            self.last_sync_ms = current_time_ms
            self.current_sync_point_ms = await self.sync_client.wait_for_sync_point(data_timestamp_ms)

        return self.current_sync_point_ms

    async def _process_depth_message(self, data: Dict[str, Any]) -> None:
        """Process depth snapshot from Binance."""
        try:
            # Binance depth format:
            # {
            #   "e": "depthUpdate",
            #   "E": 1234567890,
            #   "s": "BTCUSDT",
            #   "U": 157,
            #   "u": 160,
            #   "b": [["0.0024", "10"]],     # bids
            #   "a": [["0.0026", "100"]]     # asks
            # }

            if data.get("e") != "depthUpdate":
                return  # Not a depth message

            timestamp_ms = int(time.time() * 1000)
            exchange_timestamp_ms = data.get("E", timestamp_ms)

            # Get synchronized sync point (batched per sync cycle)
            sync_point_ms = int(await self._get_sync_point(timestamp_ms))

            # Parse bids and asks as floats
            bids = [(float(price), float(qty)) for price, qty in data.get("b", [])]
            asks = [(float(price), float(qty)) for price, qty in data.get("a", [])]

            # Create depth snapshot with synchronized timestamp
            depth = DepthSnapshot(
                symbol=self.symbol.upper(),
                timestamp_ms=sync_point_ms,
                exchange_timestamp_ms=exchange_timestamp_ms,
                bids=bids,
                asks=asks,
            )

            # Send to callback
            await self.on_depth_callback(depth)

        except Exception as e:
            logger.error("Failed to process depth message", error=str(e), data=data)
            raise

    async def _process_trade_message(self, data: Dict[str, Any]) -> None:
        """Process trade message from Binance."""
        try:
            # Binance aggTrade format:
            # {
            #   "e": "aggTrade",
            #   "E": 123456789,
            #   "s": "BNBBTC",
            #   "a": 12345,
            #   "p": "0.001",
            #   "q": "100",
            #   "f": 100,
            #   "l": 105,
            #   "T": 123456785,
            #   "m": true,
            #   "M": true
            # }

            if data.get("e") != "aggTrade":
                return  # Not a trade message

            timestamp_ms = data.get("E", int(time.time() * 1000))

            # Get synchronized sync point (batched per sync cycle)
            sync_point_ms = int(await self._get_sync_point(timestamp_ms))

            # Create trade message with synchronized timestamp
            trade = TradeMessage(
                symbol=self.symbol.upper(),
                timestamp_ms=sync_point_ms,
                trade_id=data.get("a", 0),
                price=float(data.get("p", 0)),
                quantity=float(data.get("q", 0)),
                is_buyer_maker=data.get("m", False),
            )

            # Send to callback
            await self.on_trade_callback(trade)

        except Exception as e:
            logger.error("Failed to process trade message", error=str(e), data=data)
            raise


class MarketDataGateway:
    """Binance WebSocket ingestion service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self._running = False
        self._messages_published = 0
        self._errors = 0

        # Create WebSocket manager
        self.ws_manager = BinanceWebSocketManager(
            config,
            self.nats,
            self.on_depth,
            self.on_trade
        )

    async def on_depth(self, depth: DepthSnapshot) -> None:
        """Handle depth snapshot."""
        try:
            await self.nats.publish("raw.depth.v1", depth)
            self._messages_published += 1
            logger.debug("Published depth message", published=self._messages_published)
        except Exception as e:
            logger.error("Failed to publish depth message", error=str(e))
            self._errors += 1

    async def on_trade(self, trade: TradeMessage) -> None:
        """Handle trade message."""
        try:
            await self.nats.publish("raw.trades.v1", trade)
            self._messages_published += 1
            logger.debug("Published trade message", published=self._messages_published)
        except Exception as e:
            logger.error("Failed to publish trade message", error=str(e))
            self._errors += 1

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting MarketDataGateway service")
        try:
            await self.nats.connect()
            self._running = True
            logger.info("MarketDataGateway ready")

        except Exception as e:
            logger.error("Failed to start MarketDataGateway", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping MarketDataGateway service")
        self._running = False
        await self.ws_manager.stop()
        await self.nats.close()
        logger.info(
            "MarketDataGateway stopped",
            messages_published=self._messages_published,
            errors=self._errors,
        )

    async def run(self) -> None:
        """Main service loop."""
        try:
            await self.start()

            # Run WebSocket manager
            await self.ws_manager.start()

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
