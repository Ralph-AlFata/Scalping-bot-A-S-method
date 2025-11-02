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
        self.ofi_depth_levels = config.strategy.features.get("ofi_depth_levels", 10)  # Depth for OFI volume calculation

        # State
        self.last_depth: Optional[DepthSnapshot] = None
        self.micro_price_ema: Optional[float] = None

        # OFI calculation state - store depth snapshots for delta calculation
        self.depth_history: Deque[DepthSnapshot] = deque(maxlen=self.ofi_window + 1)
        self.ofi_contributions: Deque[float] = deque(maxlen=self.ofi_window)  # Individual tick OFI contributions
        self.ofi_cumulative: float = 0.0  # Rolling sum of OFI contributions
        self.ofi_cumulative_history: Deque[float] = deque(maxlen=100)  # History of cumulative OFI for z-score
        self.last_mid_price: Optional[float] = None

        # OFI direction hysteresis - prevent rapid flips
        self.last_ofi_direction: Optional[str] = None
        self.direction_confirmation_count: int = 0
        self.direction_confirmation_threshold: int = 2  # Require 2 consecutive signals to change direction

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

            # Calculate instantaneous OFI contribution (for this tick)
            ofi_contribution = self._calculate_ofi_contribution(depth, mid_price)

            # Update cumulative rolling OFI
            # If we're at window capacity, subtract the oldest contribution
            if len(self.ofi_contributions) == self.ofi_window:
                self.ofi_cumulative -= self.ofi_contributions[0]

            # Add new contribution
            # If the queue is full, the queue.append() function automatically removes the first element and adds that item as the last one
            self.ofi_contributions.append(ofi_contribution)
            self.ofi_cumulative += ofi_contribution

            # Store cumulative OFI in history for z-score calculation
            self.ofi_cumulative_history.append(self.ofi_cumulative)

            # Calculate OFI z-score from cumulative history
            ofi_z_score = self._calculate_z_score(list(self.ofi_cumulative_history))

            # Determine OFI direction with hysteresis to prevent rapid flips
            ofi_direction = self._calculate_ofi_direction_with_hysteresis(ofi_z_score)

            ofi_data = OFIData(
                value=self.ofi_cumulative,  # Use cumulative OFI as the value
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

    def _calculate_ofi_contribution(self, depth: DepthSnapshot, mid_price: float) -> float:
        """
        Calculate instantaneous Order Flow Imbalance (OFI) contribution for a single tick.

        OFI_contribution_t = sign(Δp_t) × ΔV_t

        where:
        - sign(Δp_t) = direction of price change from t-1 to t (+1 for up, -1 for down, 0 for no change)
        - ΔV_t = change in volume at each price level from t-1 to t

        The rolling cumulative OFI is calculated by summing these contributions over a window:
        OFI_t = Σ(i=t-w to t) OFI_contribution_i

        Args:
            depth: Current depth snapshot
            mid_price: Current mid-price

        Returns:
            Instantaneous OFI contribution for this tick
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
            # Use configurable depth levels for OFI calculation
            depth_limit = self.ofi_depth_levels

            # Convert order book to dictionaries for easier lookup
            prev_bid_dict = {price: qty for price, qty in prev_depth.bids[:depth_limit]}
            prev_ask_dict = {price: qty for price, qty in prev_depth.asks[:depth_limit]}

            curr_bid_dict = {price: qty for price, qty in curr_depth.bids[:depth_limit]}
            curr_ask_dict = {price: qty for price, qty in curr_depth.asks[:depth_limit]}

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

            # OFI contribution = sign(Δp) × ΔV
            ofi_contribution = price_sign * delta_volume

            logger.debug(
                "OFI contribution calculated",
                contribution=ofi_contribution,
                price_sign=price_sign,
                delta_volume=delta_volume,
                delta_price=delta_price,
            )

            return ofi_contribution

        except Exception as e:
            logger.error("Error calculating OFI contribution", error=str(e))
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

        Queue imbalance is calculated as the sum of volumes at the top queue_depth price levels.
        This provides a measure of relative liquidity strength at each side of the book.
        """
        # Sum volumes at top queue_depth levels
        bid_queue = sum(qty for _, qty in depth.bids[:self.queue_depth])
        ask_queue = sum(qty for _, qty in depth.asks[:self.queue_depth])

        # Calculate imbalance ratio
        imbalance_ratio = bid_queue / ask_queue if ask_queue > 0 else 1.0

        return QueueImbalanceData(
            bid_queue=float(bid_queue),
            ask_queue=float(ask_queue),
            imbalance_ratio=imbalance_ratio,
        )

    def _calculate_ofi_direction_with_hysteresis(self, ofi_z_score: float) -> Optional[str]:
        """
        Calculate OFI direction with hysteresis to prevent rapid flips.

        Uses a confirmation threshold - requires direction to be consistent
        for N consecutive ticks before changing the direction.

        Args:
            ofi_z_score: Current OFI z-score

        Returns:
            "BUY", "SELL", or None if no significant signal
        """
        # Determine what the raw direction signal would be
        raw_direction = None
        if abs(ofi_z_score) > self.ofi_threshold:
            raw_direction = "BUY" if ofi_z_score > 0 else "SELL"

        # If no raw signal, reset confirmation count and return last known direction
        if raw_direction is None:
            self.direction_confirmation_count = 0
            return None

        # If raw direction matches last confirmed direction, maintain it
        if raw_direction == self.last_ofi_direction:
            self.direction_confirmation_count += 1
            return self.last_ofi_direction

        # If raw direction is different, start counting confirmations
        if raw_direction != self.last_ofi_direction:
            self.direction_confirmation_count += 1

            # Once we have enough confirmations, flip the direction
            if self.direction_confirmation_count >= self.direction_confirmation_threshold:
                self.last_ofi_direction = raw_direction
                self.direction_confirmation_count = 0
                logger.info(
                    "OFI direction changed with hysteresis",
                    new_direction=self.last_ofi_direction,
                    z_score=ofi_z_score,
                )
                return self.last_ofi_direction

            # Not enough confirmations yet, return old direction
            return self.last_ofi_direction

        return None

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
