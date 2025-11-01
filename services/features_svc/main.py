"""
Features Service.
Calculates order flow imbalance (OFI), micro-price, and queue imbalance.

Phase 2: OFI, micro-price, queue imbalance calculation and publishing.

Purpose: Calculate market microstructure features from raw market data.

Responsibilities:
    - Calculate micro-price (volume-weighted mid-price)
    - Compute order flow imbalance (OFI) over rolling windows
    - Calculate queue imbalance at best bid/ask
    - Compute z-scores for normalization
    - Maintain rolling buffers for calculations
    - Publish feature vectors

Inputs (NATS Subscriptions):
    - raw.depth.v1 - Order book snapshots
    - raw.trades.v1 - Trade events

Outputs (NATS Topics):
    - features.v1 - Feature vectors
"""

import asyncio
import signal
import json
from typing import Optional, List, Tuple, Deque
from collections import deque
import statistics

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger
from shared.schemas import DepthSnapshot, TradeMessage, FeatureMessage, OFIData, QueueImbalanceData

logger: Optional[object] = None


class FeatureCalculator:
    """Calculates trading features from market data."""

    def __init__(self, config):
        """Initialize feature calculator."""
        self.config = config
        self.symbol = config.system.symbol

        # Configuration
        self.ofi_window = config.strategy.features.ofi_window_ticks
        self.ofi_threshold = config.strategy.features.ofi_z_score_threshold
        self.queue_depth = config.strategy.features.queue_imbalance_depth
        self.micro_price_alpha = config.strategy.features.micro_price_alpha

        # State
        self.last_depth: Optional[DepthSnapshot] = None
        self.ofi_history: Deque[float] = deque(maxlen=self.ofi_window)
        self.micro_price_ema: Optional[float] = None

        # OFI calculation state - store depth snapshots for delta calculation
        self.depth_history: Deque[DepthSnapshot] = deque(maxlen=self.ofi_window + 1)
        self.last_mid_price: Optional[float] = None

    def calculate_features(self, depth: DepthSnapshot) -> Optional[FeatureMessage]:
        """
        Calculate all features from depth snapshot.

        Args:
            depth: Depth snapshot message.

        Returns:
            FeatureMessage with calculated features, or None if insufficient data.
        """
        try:
            # Extract best bid/ask
            best_bid, best_bid_qty = depth.bids[0] if depth.bids else (0, 0)
            best_ask, best_ask_qty = depth.asks[0] if depth.asks else (0, 0)

            if best_bid <= 0 or best_ask <= 0:
                logger.warning("Invalid price data", best_bid=best_bid, best_ask=best_ask)
                return None

            # Calculate mid price
            mid_price = (best_bid + best_ask) / 2.0

            # Store depth snapshot for OFI calculation
            self.depth_history.append(depth)

            # Calculate OFI
            ofi_value = self._calculate_ofi(depth, mid_price)
            self.ofi_history.append(ofi_value)

            # Calculate OFI z-score
            ofi_z_score = self._calculate_z_score(list(self.ofi_history))
            ofi_direction = None
            if abs(ofi_z_score) > self.ofi_threshold:
                ofi_direction = "BUY" if ofi_z_score > 0 else "SELL"

            ofi_data = OFIData(
                value=ofi_value,
                z_score=ofi_z_score,
                direction=ofi_direction,
            )

            # Update last mid price for next iteration
            self.last_mid_price = mid_price

            # Calculate micro price (volume-weighted mid)
            micro_price = self._calculate_micro_price(best_bid, best_bid_qty, best_ask, best_ask_qty)

            # Update EMA for smoothing
            if self.micro_price_ema is None:
                self.micro_price_ema = micro_price
            else:
                self.micro_price_ema = (
                    self.micro_price_alpha * micro_price +
                    (1 - self.micro_price_alpha) * self.micro_price_ema
                )

            # Calculate queue imbalance
            queue_imbalance = self._calculate_queue_imbalance(depth)

            # Calculate spread in basis points
            spread_bps = ((best_ask - best_bid) / mid_price) * 10000

            # Create feature message
            feature = FeatureMessage(
                symbol=self.symbol,
                timestamp_ms=depth.timestamp_ms,
                mid_price=mid_price,
                micro_price=self.micro_price_ema,
                spread_bps=spread_bps,
                best_bid=best_bid,
                best_ask=best_ask,
                best_bid_qty=best_bid_qty,
                best_ask_qty=best_ask_qty,
                ofi=ofi_data,
                queue_imbalance=queue_imbalance,
            )

            self.last_depth = depth
            return feature

        except Exception as e:
            logger.error("Error calculating features", error=str(e))
            return None

    def _calculate_ofi(self, depth: DepthSnapshot, mid_price: float) -> float:
        """
        Calculate Order Flow Imbalance (OFI) using the correct formula:

        OFI_t = Σ(i=t-w to t) sign(Δp_i) × ΔV_i

        where:
        - w = lookback window (typically 1-5 seconds)
        - sign(Δp_i) = direction of price change (+1 for up, -1 for down, 0 for no change)
        - ΔV_i = change in volume at each price level

        This implementation calculates the instantaneous OFI contribution for the current tick,
        and the rolling sum is maintained in self.ofi_history.
        """
        # Need at least 2 snapshots to calculate deltas
        if len(self.depth_history) < 2 or self.last_mid_price is None:
            return 0.0

        try:
            # Get previous depth snapshot
            prev_depth = self.depth_history[-2]
            curr_depth = depth

            # Calculate price change direction: sign(Δp)
            delta_price = mid_price - self.last_mid_price

            if abs(delta_price) < 1e-10:  # No significant price change
                price_sign = 0.0
            elif delta_price > 0:
                price_sign = 1.0
            else:
                price_sign = -1.0

            # Calculate volume change: ΔV
            # We'll focus on the volume changes at the best bid and ask levels
            # as a proxy for order flow imbalance

            # Convert order book to dictionaries for easier lookup
            prev_bid_dict = {price: qty for price, qty in prev_depth.bids[:10]}  # Top 10 levels
            prev_ask_dict = {price: qty for price, qty in prev_depth.asks[:10]}

            curr_bid_dict = {price: qty for price, qty in curr_depth.bids[:10]}
            curr_ask_dict = {price: qty for price, qty in curr_depth.asks[:10]}

            # Calculate volume deltas at each price level
            delta_volume = 0.0

            # Check bid side volume changes
            all_bid_prices = set(prev_bid_dict.keys()) | set(curr_bid_dict.keys())
            for price in all_bid_prices:
                prev_qty = prev_bid_dict.get(price, 0.0)
                curr_qty = curr_bid_dict.get(price, 0.0)
                delta_volume += (curr_qty - prev_qty)

            # Check ask side volume changes (subtract because it's supply)
            all_ask_prices = set(prev_ask_dict.keys()) | set(curr_ask_dict.keys())
            for price in all_ask_prices:
                prev_qty = prev_ask_dict.get(price, 0.0)
                curr_qty = curr_ask_dict.get(price, 0.0)
                delta_volume -= (curr_qty - prev_qty)

            # OFI = sign(Δp) × ΔV
            ofi_instant = price_sign * delta_volume

            return ofi_instant

        except Exception as e:
            logger.error("Error calculating OFI", error=str(e))
            return 0.0

    def _calculate_micro_price(self, best_bid: float, best_bid_qty: float, best_ask: float, best_ask_qty: float) -> float:
        """
        Calculate micro-price (volume-weighted mid-price).

        micro_price = (best_ask * bid_qty + best_bid * ask_qty) / (bid_qty + ask_qty)
        """
        total_qty = best_bid_qty + best_ask_qty
        if total_qty == 0:
            return (best_bid + best_ask) / 2.0
        return (best_ask * best_bid_qty + best_bid * best_ask_qty) / total_qty

    def _calculate_queue_imbalance(self, depth: DepthSnapshot) -> QueueImbalanceData:
        """
        Calculate queue imbalance at specific depth level.

        Queue depth is calculated as the number of orders (or ticks) at each level.
        """
        # Count levels up to queue_depth
        bid_queue = min(len(depth.bids), self.queue_depth)
        ask_queue = min(len(depth.asks), self.queue_depth)

        # Calculate imbalance ratio
        imbalance_ratio = bid_queue / ask_queue if ask_queue > 0 else 1.0

        return QueueImbalanceData(
            bid_queue=float(bid_queue),
            ask_queue=float(ask_queue),
            imbalance_ratio=imbalance_ratio,
        )

    def _calculate_z_score(self, values: List[float]) -> float:
        """Calculate z-score of the last value in the list."""
        if len(values) < 2:
            return 0.0

        try:
            mean = statistics.mean(values)
            stdev = statistics.stdev(values)
            if stdev == 0:
                return 0.0
            return (values[-1] - mean) / stdev
        except Exception as e:
            logger.error("Error calculating z-score", error=str(e))
            return 0.0


class FeaturesService:
    """Feature calculation service."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self._running = False
        self._messages_processed = 0
        self._messages_published = 0
        self._errors = 0

        # Create calculator
        self.calculator = FeatureCalculator(config)

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
        try:
            self._messages_processed += 1

            # Parse message
            data = json.loads(msg.data.decode())
            depth = DepthSnapshot(**data)

            # Calculate features
            feature = self.calculator.calculate_features(depth)

            if feature:
                # Publish features
                await self.nats.publish("features.v1", feature)
                self._messages_published += 1
                logger.debug("Published features", published=self._messages_published)
            else:
                logger.warning("Failed to calculate features for depth message")

        except json.JSONDecodeError as e:
            logger.error("Failed to decode depth message", error=str(e))
            self._errors += 1
        except Exception as e:
            logger.error("Error processing depth message", error=str(e))
            self._errors += 1

    async def on_trade_message(self, msg) -> None:
        """Handle trade message."""
        try:
            self._messages_processed += 1

            # Parse message
            data = json.loads(msg.data.decode())
            trade = TradeMessage(**data)

            logger.debug("Received trade message", trade_id=trade.trade_id)

            # Future: Use trades for order flow analysis

        except json.JSONDecodeError as e:
            logger.error("Failed to decode trade message", error=str(e))
            self._errors += 1
        except Exception as e:
            logger.error("Error processing trade message", error=str(e))
            self._errors += 1

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
