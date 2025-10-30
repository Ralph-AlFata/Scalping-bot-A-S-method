"""
Volatility & Order Flow Service.
Estimates volatility, order intensity (k parameter), and VPIN.

Phase 2: Volatility, order intensity, and VPIN calculation.
"""

import asyncio
import signal
import json
import time
import math
from typing import Optional, List, Deque
from collections import deque
import statistics

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger
from shared.schemas import TradeMessage, DepthSnapshot, VolflowMessage, VolatilityData, OrderIntensityData, VPINData, VPINStatus

logger: Optional[object] = None


class VolflowCalculator:
    """Calculates volatility, order intensity, and VPIN."""

    def __init__(self, config):
        """Initialize volatility calculator."""
        self.config = config
        self.symbol = config.system.symbol

        # Configuration
        self.vol_window_sec = config.strategy.volflow.volatility_lookback_sec
        self.vol_min = config.strategy.volflow.volatility_min
        self.vol_max = config.strategy.volflow.volatility_max

        self.k_window_sec = config.strategy.volflow.order_intensity_window_sec
        self.k_min = config.strategy.volflow.order_intensity_k_min
        self.k_max = config.strategy.volflow.order_intensity_k_max

        self.vpin_threshold_normal = config.strategy.volflow.vpin_threshold_normal
        self.vpin_threshold_elevated = config.strategy.volflow.vpin_threshold_elevated
        self.vpin_threshold_toxic = config.strategy.volflow.vpin_threshold_toxic

        # State
        self.trade_prices: Deque[tuple] = deque()  # (timestamp, price)
        self.trade_volumes: Deque[tuple] = deque()  # (timestamp, volume, side)

        # Initialization
        self.last_publish_time = time.time()
        self.publish_interval_sec = 1.0

    def update_trade(self, trade: TradeMessage) -> None:
        """Update with new trade data."""
        timestamp = trade.timestamp_ms / 1000.0
        self.trade_prices.append((timestamp, trade.price))
        self.trade_volumes.append((timestamp, trade.quantity, trade.is_buyer_maker))

        # Cleanup old data
        self._cleanup_old_data(timestamp)

    def update_depth(self, depth: DepthSnapshot) -> None:
        """Update with depth snapshot."""
        # Used for order intensity and VPIN calculation
        pass

    def calculate_volflow(self) -> Optional[VolflowMessage]:
        """
        Calculate volatility, order intensity, and VPIN.

        Returns:
            VolflowMessage with estimates, or None if insufficient data.
        """
        current_time = time.time()

        # Check if it's time to publish
        if current_time - self.last_publish_time < self.publish_interval_sec:
            return None

        self.last_publish_time = current_time

        try:
            # Calculate volatility
            vol_data = self._calculate_volatility()

            # Calculate order intensity
            k_data = self._calculate_order_intensity()

            # Calculate VPIN
            vpin_data = self._calculate_vpin()

            # Create message
            message = VolflowMessage(
                symbol=self.symbol,
                timestamp_ms=int(current_time * 1000),
                volatility=vol_data,
                order_intensity=k_data,
                vpin=vpin_data,
            )

            return message

        except Exception as e:
            logger.error("Error calculating volflow", error=str(e))
            return None

    def _calculate_volatility(self) -> VolatilityData:
        """
        Calculate realized volatility from trade returns.

        Uses standard deviation of log returns over the lookback window.
        """
        current_time = time.time()
        cutoff_time = current_time - self.vol_window_sec

        # Filter prices within window
        prices = [p for t, p in self.trade_prices if t > cutoff_time]

        if len(prices) < 2:
            # Insufficient data - return minimum volatility
            return VolatilityData(value=self.vol_min, confidence=0.1)

        try:
            # Calculate log returns
            log_returns = []
            for i in range(1, len(prices)):
                log_return = math.log(prices[i] / prices[i - 1])
                log_returns.append(log_return)

            if len(log_returns) == 0:
                return VolatilityData(value=self.vol_min, confidence=0.1)

            # Standard deviation of log returns
            stdev = statistics.stdev(log_returns)

            # Annualize volatility (assume ~252 trading days, ~86400 sec/day)
            # 1 second frequency -> 252 * 86400 = 21,628,800 seconds per year
            annualized_vol = stdev * math.sqrt(252 * 86400)

            # Bound volatility
            vol = max(self.vol_min, min(annualized_vol, self.vol_max))

            # Confidence increases with number of observations
            confidence = min(0.95, len(prices) / 100.0)

            return VolatilityData(value=vol, confidence=confidence)

        except Exception as e:
            logger.error("Error calculating volatility", error=str(e))
            return VolatilityData(value=self.vol_min, confidence=0.1)

    def _calculate_order_intensity(self) -> OrderIntensityData:
        """
        Calculate order intensity (k parameter).

        Simplified approach: k = order_arrival_rate * average_spread
        Uses recent trade frequency as proxy for arrival rate.
        """
        current_time = time.time()
        cutoff_time = current_time - self.k_window_sec

        # Count trades in window
        trades_in_window = sum(1 for t, _, _ in self.trade_volumes if t > cutoff_time)

        if trades_in_window < 1:
            # Insufficient data
            return OrderIntensityData(k=1.0, confidence=0.1)

        try:
            # Arrival rate: trades per second
            arrival_rate = trades_in_window / self.k_window_sec

            # Convert to a reasonable k parameter
            # If we see ~1 trade/sec, k should be around 1.0
            # If we see ~10 trades/sec, k should be around 5.0
            k = arrival_rate * 0.5 + 0.5  # Add offset to avoid k=0

            # Bound k
            k = max(self.k_min, min(k, self.k_max))

            # Confidence increases with sample size
            confidence = min(0.95, trades_in_window / 50.0)

            return OrderIntensityData(k=k, confidence=confidence)

        except Exception as e:
            logger.error("Error calculating order intensity", error=str(e))
            return OrderIntensityData(k=1.0, confidence=0.1)

    def _calculate_vpin(self) -> VPINData:
        """
        Calculate Volume-Synchronized Probability of Informed Trading (VPIN).

        VPIN = |cumulative_buy_volume - cumulative_sell_volume| / total_volume

        Detects informed trading (high VPIN = toxic flow).
        """
        current_time = time.time()
        cutoff_time = current_time - self.k_window_sec

        # Filter trades in window
        trades = [(t, v, s) for t, v, s in self.trade_volumes if t > cutoff_time]

        if len(trades) == 0:
            return VPINData(value=0.5, status=VPINStatus.NORMAL)

        try:
            buy_volume = 0.0
            sell_volume = 0.0

            for _, volume, is_buyer_maker in trades:
                if is_buyer_maker:
                    # Buyer is maker = ask was hit = sell aggressor
                    sell_volume += volume
                else:
                    # Buyer is aggressor = bid was hit = buy aggressor
                    buy_volume += volume

            total_volume = buy_volume + sell_volume

            if total_volume == 0:
                return VPINData(value=0.5, status=VPINStatus.NORMAL)

            # Calculate VPIN
            vpin = abs(buy_volume - sell_volume) / total_volume

            # Determine status
            if vpin > self.vpin_threshold_toxic:
                status = VPINStatus.TOXIC
            elif vpin > self.vpin_threshold_elevated:
                status = VPINStatus.ELEVATED
            else:
                status = VPINStatus.NORMAL

            return VPINData(value=vpin, status=status)

        except Exception as e:
            logger.error("Error calculating VPIN", error=str(e))
            return VPINData(value=0.5, status=VPINStatus.NORMAL)

    def _cleanup_old_data(self, current_time: float) -> None:
        """Remove data older than the maximum lookback window."""
        max_lookback = max(self.vol_window_sec, self.k_window_sec)
        cutoff_time = current_time - max_lookback

        # Clean trade prices
        while self.trade_prices and self.trade_prices[0][0] < cutoff_time:
            self.trade_prices.popleft()

        # Clean trade volumes
        while self.trade_volumes and self.trade_volumes[0][0] < cutoff_time:
            self.trade_volumes.popleft()


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

        # Create calculator
        self.calculator = VolflowCalculator(config)

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
        try:
            self._measurements += 1

            # Parse message
            data = json.loads(msg.data.decode())
            trade = TradeMessage(**data)

            # Update calculator
            self.calculator.update_trade(trade)

            # Try to publish volflow estimate
            volflow = self.calculator.calculate_volflow()
            if volflow:
                await self.nats.publish("volflow.v1", volflow)
                self._estimates_published += 1
                logger.debug("Published volflow estimate", published=self._estimates_published)

        except json.JSONDecodeError as e:
            logger.error("Failed to decode trade message", error=str(e))
            self._errors += 1
        except Exception as e:
            logger.error("Error processing trade message", error=str(e))
            self._errors += 1

    async def on_depth(self, msg) -> None:
        """Handle depth message."""
        try:
            # Parse message
            data = json.loads(msg.data.decode())
            depth = DepthSnapshot(**data)

            # Update calculator with depth
            self.calculator.update_depth(depth)

        except json.JSONDecodeError as e:
            logger.error("Failed to decode depth message", error=str(e))
            self._errors += 1
        except Exception as e:
            logger.error("Error processing depth message", error=str(e))
            self._errors += 1

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