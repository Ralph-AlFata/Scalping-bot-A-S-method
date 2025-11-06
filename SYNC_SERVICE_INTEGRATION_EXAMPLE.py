"""
Example: How to Integrate SyncClient into Your Service

This file shows the minimal changes needed to add synchronization to any service.

Key Points:
1. Import SyncClient
2. Initialize in __init__
3. Call start() in service start()
4. Call wait_for_sync_point() before processing data
5. Call stop() in service stop()

Below are template examples for the 3 main services you're auditing.
"""

# ============================================================================
# TEMPLATE 1: marketdata_gw Integration
# ============================================================================

"""
File: services/marketdata_gw/main.py

Changes needed:
1. Add import: from shared.sync_client import SyncClient
2. Add to BinanceWebSocketManager.__init__:
   - self.sync_client = SyncClient(config, nats_client, "marketdata_gw")
3. Add to BinanceWebSocketManager.start():
   - await self.sync_client.start()
4. Add to on_depth callback (before publishing):
   - sync_point_ms = await self.sync_client.wait_for_sync_point(depth.timestamp_ms)
5. Add to on_trade callback (before publishing):
   - sync_point_ms = await self.sync_client.wait_for_sync_point(trade.timestamp_ms)
6. Add to stop():
   - await self.sync_client.stop()
"""

from shared.sync_client import SyncClient
from shared.nats_client import NATSClient
from shared.config import load_config


class BinanceWebSocketManager_WITH_SYNC:
    """Example: marketdata_gw with sync client."""

    def __init__(self, config, nats_client, on_depth_callback, on_trade_callback):
        """Initialize with sync client."""
        self.config = config
        self.nats = nats_client
        self.on_depth_callback = on_depth_callback
        self.on_trade_callback = on_trade_callback

        # NEW: Add sync client
        self.sync_client = SyncClient(config, nats_client, "marketdata_gw")

    async def start(self) -> None:
        """Start WebSocket streams."""
        logger.info("Starting Binance WebSocket streams")

        # NEW: Start sync client
        await self.sync_client.start()

        self._running = True
        # ... rest of existing code

    async def _on_depth(self, depth_data) -> None:
        """Process depth snapshot."""
        try:
            depth = DepthSnapshot(...)  # parse from depth_data

            # NEW: Wait for sync point with other services
            sync_point_ms = await self.sync_client.wait_for_sync_point(
                data_timestamp_ms=depth.timestamp_ms
            )

            # OLD: publish immediately
            # await self.nats.publish("raw.depth.v1", depth.model_dump_json())

            # NEW: Use sync point timestamp in message
            message = depth.model_dump()
            message["sync_point_ms"] = sync_point_ms
            await self.nats.publish("raw.depth.v1", message)

        except Exception as e:
            logger.error("Error processing depth", error=str(e))

    async def _on_trade(self, trade_data) -> None:
        """Process trade message."""
        try:
            trade = TradeMessage(...)  # parse from trade_data

            # NEW: Wait for sync point
            sync_point_ms = await self.sync_client.wait_for_sync_point(
                data_timestamp_ms=trade.timestamp_ms
            )

            # Publish with sync point
            message = trade.model_dump()
            message["sync_point_ms"] = sync_point_ms
            await self.nats.publish("raw.trades.v1", message)

        except Exception as e:
            logger.error("Error processing trade", error=str(e))

    async def stop(self) -> None:
        """Stop WebSocket streams."""
        logger.info("Stopping Binance WebSocket streams")

        # NEW: Stop sync client
        await self.sync_client.stop()

        self._running = False
        # ... rest of existing code


# ============================================================================
# TEMPLATE 2: features_svc Integration
# ============================================================================

"""
File: services/features_svc/main.py

Changes needed:
1. Add import: from shared.sync_client import SyncClient
2. Add to FeaturesService.__init__:
   - self.sync_client = SyncClient(config, nats_client, "features_svc")
3. Add to FeaturesService.start():
   - await self.sync_client.start()
4. Add to on_depth callback:
   - sync_point_ms = await self.sync_client.wait_for_sync_point(depth.timestamp_ms)
5. Add to on_trade callback (if it exists):
   - sync_point_ms = await self.sync_client.wait_for_sync_point(trade.timestamp_ms)
6. Add to stop():
   - await self.sync_client.stop()
"""


class FeaturesService_WITH_SYNC:
    """Example: features_svc with sync client."""

    def __init__(self, config, nats_client):
        """Initialize with sync client."""
        self.config = config
        self.nats = nats_client
        self.calculator = FeatureCalculator(config)

        # NEW: Add sync client
        self.sync_client = SyncClient(config, nats_client, "features_svc")

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting FeaturesService")
        await self.nats.connect()

        # NEW: Start sync client
        await self.sync_client.start()

        # Subscribe to inputs
        await self.nats.subscribe("raw.depth.v1", self.on_depth)
        await self.nats.subscribe("raw.trades.v1", self.on_trades)

        logger.info("FeaturesService ready")

    async def on_depth(self, msg) -> None:
        """Handle depth snapshot."""
        try:
            depth = DepthSnapshot.model_validate_json(msg.data)

            # NEW: Wait for sync point
            sync_point_ms = await self.sync_client.wait_for_sync_point(
                data_timestamp_ms=depth.timestamp_ms
            )

            # Calculate features
            features = self.calculator.calculate_features(depth)

            if features:
                # Update timestamp to sync point
                features.timestamp_ms = sync_point_ms

                # Publish features
                await self.nats.publish(
                    "features.v1",
                    features.model_dump_json().encode()
                )
                logger.debug(f"Features published for {features.symbol}")

        except Exception as e:
            logger.error("Error processing depth", error=str(e))

    async def on_trades(self, msg) -> None:
        """Handle trade message (optional, for reference)."""
        try:
            trade = TradeMessage.model_validate_json(msg.data)

            # NEW: Wait for sync point if you process trades
            sync_point_ms = await self.sync_client.wait_for_sync_point(
                data_timestamp_ms=trade.timestamp_ms
            )

            # Process trade (if needed)
            # ...

        except Exception as e:
            logger.error("Error processing trade", error=str(e))

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping FeaturesService")

        # NEW: Stop sync client
        await self.sync_client.stop()

        await self.nats.close()
        logger.info("FeaturesService stopped")


# ============================================================================
# TEMPLATE 3: volflow_estimator Integration
# ============================================================================

"""
File: services/volflow_estimator/main.py

Changes needed:
1. Add import: from shared.sync_client import SyncClient
2. Add to VolflowService.__init__:
   - self.sync_client = SyncClient(config, nats_client, "volflow_estimator")
3. Add to VolflowService.start():
   - await self.sync_client.start()
4. Add to on_trades callback:
   - sync_point_ms = await self.sync_client.wait_for_sync_point(trade.timestamp_ms)
5. Add to on_features callback (if exists):
   - sync_point_ms = await self.sync_client.wait_for_sync_point(features.timestamp_ms)
6. Add to stop():
   - await self.sync_client.stop()
"""


class VolflowService_WITH_SYNC:
    """Example: volflow_estimator with sync client."""

    def __init__(self, config, nats_client):
        """Initialize with sync client."""
        self.config = config
        self.nats = nats_client
        self.calculator = VolflowCalculator(config)

        # NEW: Add sync client
        self.sync_client = SyncClient(config, nats_client, "volflow_estimator")

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting VolflowService")
        await self.nats.connect()

        # NEW: Start sync client
        await self.sync_client.start()

        # Subscribe to inputs
        await self.nats.subscribe("raw.trades.v1", self.on_trades)
        await self.nats.subscribe("features.v1", self.on_features)

        logger.info("VolflowService ready")

    async def on_trades(self, msg) -> None:
        """Handle trade message."""
        try:
            trade = TradeMessage.model_validate_json(msg.data)

            # NEW: Wait for sync point
            sync_point_ms = await self.sync_client.wait_for_sync_point(
                data_timestamp_ms=trade.timestamp_ms
            )

            # Update volatility calculation
            volflow = self.calculator.process_trade(trade)

            if volflow:
                # Update timestamp to sync point
                volflow.timestamp_ms = sync_point_ms

                # Publish volflow
                await self.nats.publish(
                    "volflow.v1",
                    volflow.model_dump_json().encode()
                )
                logger.debug(f"Volflow published for {volflow.symbol}")

        except Exception as e:
            logger.error("Error processing trade", error=str(e))

    async def on_features(self, msg) -> None:
        """Handle features message (if you need to sync with features too)."""
        try:
            features = FeatureMessage.model_validate_json(msg.data)

            # NEW: Optional - wait for sync if you need feature data
            sync_point_ms = await self.sync_client.wait_for_sync_point(
                data_timestamp_ms=features.timestamp_ms
            )

            # Process features (if needed)
            # ...

        except Exception as e:
            logger.error("Error processing features", error=str(e))

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping VolflowService")

        # NEW: Stop sync client
        await self.sync_client.stop()

        await self.nats.close()
        logger.info("VolflowService stopped")


# ============================================================================
# IMPORTANT NOTES FOR INTEGRATION
# ============================================================================

"""
1. Order of wait_for_sync_point() calls:
   - Call BEFORE processing/publishing data
   - Use the returned sync_point_ms as the authoritative timestamp
   - All services will see the same sync_point_ms for synchronized events

2. Timestamp handling:
   - Data comes in with its original timestamp (e.g., depth.timestamp_ms)
   - Call wait_for_sync_point(depth.timestamp_ms)
   - Returns sync_point_ms (the synchronized boundary)
   - Use sync_point_ms in all downstream messages

3. Optional: Update message timestamps
   - You can update message.timestamp_ms = sync_point_ms (recommended)
   - Or keep original timestamp and add sync_point_ms as separate field
   - Choose whichever makes sense for your audit/analysis

4. Error handling:
   - If sync fails, wait_for_sync_point returns the original data_timestamp_ms
   - Service continues with graceful degradation
   - No exceptions thrown (fault-tolerant)

5. Configuration:
   - Make sure the service is enabled in config.yaml
   - The sync interval comes from: strategy.quoting.update_freq_ms (default 100ms)

6. Performance:
   - wait_for_sync_point() waits ~1ms at most
   - Timeout is 1 second (safe default)
   - If a service is slow, it will delay the sync point

7. Monitoring:
   - Check logs for "Service registered"
   - Check logs for "Received sync point"
   - Use sync_client.get_status() for health checks
"""
