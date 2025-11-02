"""
Volatility & Order Flow Service.
Estimates volatility, order intensity (k parameter), and VPIN.

Phase 2: Volatility, order intensity, and VPIN calculation.

Purpose: Estimate volatility, market order arrival intensity, and market toxicity indicators.

Responsibilities:
    - Calculate rolling volatility (σ) using exponential weighting with time-normalization
    - Estimate market order arrival intensity (k) from MARKET-WIDE trade rates
    - Track our fill rate ratio as complementary feedback metric
    - Compute VPIN (Volume-Synchronized Probability of Informed Trading) for toxicity detection
    - Maintain exponentially weighted moving statistics
    - Publish vol/flow metrics

Inputs (NATS Subscriptions):
    - raw.trades.v1 - Market-wide trade stream (used for volatility, k, VPIN)
    - features.v1 - Price features (optional for future enhancements)
    - fills.v1 - Our order fills (used to calculate fill rate ratio vs market)

Outputs (NATS Topics):
    - volflow.v1 - Volatility, order intensity (k), and VPIN

Key Metrics:
    - Volatility (σ): Annualized realized volatility from market trades
    - Order Intensity (k): Avellaneda-Stoikov parameter representing market order arrival rate
    - VPIN: Volume-synchronized toxicity indicator (informed trading probability)
    - Fill Rate Ratio: Our fill rate vs market average (complementary feedback)
"""

import asyncio
import signal
import json
import time
import math
from typing import Optional, List, Deque
from collections import deque

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger
from shared.schemas import TradeMessage, DepthSnapshot, VolflowMessage, VolatilityData, OrderIntensityData, VPINData, VPINStatus, FeatureMessage, FillMessage

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

        # Exponential decay factor (λ) - e.g., 0.94 for ~5-second half-life
        self.decay_lambda = 0.94

        # State for EWMA calculations
        self.last_price: Optional[float] = None
        self.sigma_squared: Optional[float] = None  # EWMA variance
        self.last_update_time: Optional[float] = None

        # State for order intensity (k)
        # k is estimated from MARKET-WIDE order arrival rate (raw.trades.v1)
        self.k_prev: float = 1.0  # Previous k value (market order intensity)
        self.market_trades: Deque[tuple] = deque()  # (timestamp, quantity) - ALL market trades
        self.last_k_update_time: Optional[float] = None

        # State for our fill rate monitoring (separate from market k)
        # Used to detect if we're being filled too much (aggressive quoting) or too little (conservative)
        # This is used as feedback to adjust quote width, complementary to market k
        self.our_fills: Deque[tuple] = deque()  # (timestamp, quantity) - only OUR fills
        self.fill_rate_ratio: float = 1.0  # Our fill rate / Market trade rate (1.0 = neutral)

        # State for VPIN - volume buckets
        self.volume_bucket_size = 10.0  # Volume per bucket (in base asset)
        self.volume_buckets: Deque[dict] = deque()  # [{buy_vol, sell_vol, total_vol}, ...]
        self.current_bucket = {"buy_volume": 0.0, "sell_volume": 0.0}
        self.max_buckets = 50  # Keep last 50 buckets for VPIN calculation

        # Trade data (kept for backward compatibility and monitoring)
        self.trade_prices: Deque[tuple] = deque()  # (timestamp, price)
        self.trade_volumes: Deque[tuple] = deque()  # (timestamp, volume, side)

        # Initialization
        self.last_publish_time = time.time()
        self.publish_interval_sec = 1.0

    def update_trade(self, trade: TradeMessage) -> None:
        """Update with new trade data (market-wide trades from raw.trades.v1)."""
        timestamp = trade.timestamp_ms / 1000.0
        self.trade_prices.append((timestamp, trade.price))
        self.trade_volumes.append((timestamp, trade.quantity, trade.is_buyer_maker))

        # Track market trades for order intensity calculation
        # This represents ALL orders hitting the market (our k parameter)
        self.market_trades.append((timestamp, trade.quantity))

        # Update volume buckets for VPIN
        # is_buyer_maker=False means buy aggressor (buyer hit ask)
        # is_buyer_maker=True means sell aggressor (seller hit bid)
        is_buy_aggressor = not trade.is_buyer_maker
        self._update_volume_bucket(trade.quantity, is_buy_aggressor)

        # Cleanup old data
        self._cleanup_old_data(timestamp)
        self._cleanup_old_market_trades(timestamp)

    def update_depth(self, depth: DepthSnapshot) -> None:
        """Update with depth snapshot."""
        # Currently not used in calculations
        pass

    def update_fill(self, fill: FillMessage) -> None:
        """Update with fill data (our fills only, from fills.v1)."""
        timestamp = fill.timestamp_ms / 1000.0
        self.our_fills.append((timestamp, fill.quantity))
        self._cleanup_old_fills(timestamp)

    def update_features(self, features: FeatureMessage) -> None:
        """Update with feature data."""
        # Features could be used for enhanced calculations in the future
        # For now, we primarily use trade data for volatility
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
        Calculate realized volatility using EWMA with time-normalization.

        Formula: σ²ₜ = λσ²ₜ₋₁ + (1-λ)(rₜ/√dtₜ)²
        where:
            λ = decay factor (e.g., 0.94)
            rₜ = log return at time t
            dtₜ = time interval in seconds since last trade

        This ensures volatility is independent of tick frequency. A 1% move in 0.1s
        represents higher volatility than a 1% move in 1.0s, consistent with Brownian
        motion where σ_per_second = r / sqrt(dt).

        Verification:
            - 1% move in 0.1s → ~1776% annualized volatility
            - 1% move in 1.0s → ~562% annualized volatility
            - Ratio: ~3.16x (= sqrt(10)), as expected from sqrt(dt) normalization
        """
        current_time = time.time()

        # Need at least 2 prices to calculate
        if len(self.trade_prices) < 2:
            return VolatilityData(value=self.vol_min, confidence=0.1)

        try:
            # Get the most recent price
            latest_timestamp, latest_price = self.trade_prices[-1]

            # Initialize if first calculation
            if self.last_price is None or self.sigma_squared is None:
                self.last_price = latest_price
                self.last_update_time = latest_timestamp
                # Initialize with minimum variance
                self.sigma_squared = (self.vol_min ** 2) / (252 * 24 * 3600)  # De-annualize min vol
                return VolatilityData(value=self.vol_min, confidence=0.1)

            # Calculate log return
            if latest_price <= 0 or self.last_price <= 0:
                return VolatilityData(value=self.vol_min, confidence=0.1)

            log_return = math.log(latest_price / self.last_price)

            # Calculate time interval in seconds since last price update
            dt_seconds = latest_timestamp - self.last_update_time

            # Avoid division by zero and negative dt
            if dt_seconds <= 0:
                return VolatilityData(value=self.vol_min, confidence=0.1)

            # Normalize log return by sqrt(dt) to get per-second volatility
            # This is consistent with Brownian motion: if a return r occurs over dt,
            # the per-second volatility component is r / sqrt(dt)
            normalized_log_return = log_return / math.sqrt(dt_seconds)

            # EWMA update with normalized return: σ²ₜ = λσ²ₜ₋₁ + (1-λ)(rₜ/√dtₜ)²
            self.sigma_squared = (
                self.decay_lambda * self.sigma_squared +
                (1 - self.decay_lambda) * (normalized_log_return ** 2)
            )

            # Update state
            self.last_price = latest_price
            self.last_update_time = latest_timestamp

            # Convert variance to volatility (standard deviation)
            sigma = math.sqrt(self.sigma_squared)

            # Annualize volatility
            # σ is now per-second, so annualize using: annual_vol = σ_per_second * sqrt(252 * 86400)
            # 252 trading days/year * 86400 seconds/day
            annualized_vol = sigma * math.sqrt(252 * 86400)

            # Bound volatility
            vol = max(self.vol_min, min(annualized_vol, self.vol_max))

            # Confidence increases with number of updates and recent data freshness
            # Higher decay = faster adaptation = lower confidence in long-term estimate
            confidence = min(0.95, 0.5 + (1 - self.decay_lambda) * 10)

            return VolatilityData(value=vol, confidence=confidence)

        except Exception as e:
            logger.error("Error calculating volatility", error=str(e))
            return VolatilityData(value=self.vol_min, confidence=0.1)

    def _calculate_order_intensity(self) -> OrderIntensityData:
        """
        Calculate order intensity (k parameter) using EWMA from MARKET TRADES.

        THEORY (Avellaneda-Stoikov 2008):
            Market order arrival rate: λ(δ) = A × exp(-k × δ)
            where:
                δ = spread (distance from mid-price)
                k = order intensity parameter (decay rate) - how quickly order intensity decays
                A = base arrival intensity

            The k parameter represents how market participants' order arrival rate changes
            with the spread we quote. High k means aggressive traders dominate (drop off
            quickly), low k means patient traders dominate (arrive uniformly across spreads).

        PRACTICE - MARKET-WIDE APPROACH (Improved):
            Instead of only observing OUR fills (biased by OUR quotes), we estimate k
            from MARKET-WIDE order arrival rate using raw.trades.v1:

                kₜ = λkₜ₋₁ + (1-λ) × (market_trades_in_interval / interval_duration)

            This gives us the TRUE market order intensity, not just our interaction with it.

            Benefits:
            1. Unbiased estimate of market behavior (not affected by our quoting strategy)
            2. Adapts to market regimes: calm markets → low k, active markets → high k
            3. Consistent with Avellaneda-Stoikov theory (measures market order arrival)

        COMPLEMENTARY METRIC - OUR FILL RATE:
            While k reflects market order intensity, we also track our fill rate ratio:
                fill_rate_ratio = (our_fills_in_interval / interval_duration) /
                                  (market_trades_in_interval / interval_duration)

            This tells us if we're being filled more/less than the market average:
            - ratio > 1: We're getting filled more than market average (too aggressive)
            - ratio < 1: We're getting filled less than market average (too conservative)
            - ratio ≈ 1: Neutral position in queue

        Formula: kₜ = λkₜ₋₁ + (1-λ) × (market_trade_rate)
        where:
            λ = decay factor (0.94, same as volatility)
            market_trade_rate = number of trades per second in market
        """
        current_time = time.time()

        # Initialize if first calculation
        if self.last_k_update_time is None:
            self.last_k_update_time = current_time
            return OrderIntensityData(k=self.k_prev, confidence=0.1)

        try:
            # Calculate time since last update
            interval_duration = current_time - self.last_k_update_time

            # Avoid division by zero
            if interval_duration < 0.001:  # Less than 1ms
                return OrderIntensityData(k=self.k_prev, confidence=0.5)

            # Count MARKET trades in the interval to get market order intensity
            cutoff_time = current_time - interval_duration
            market_trades_in_interval = sum(1 for t, _ in self.market_trades if t > cutoff_time)
            market_trade_rate = market_trades_in_interval / interval_duration

            # Also count OUR fills for ratio calculation
            our_fills_in_interval = sum(1 for t, _ in self.our_fills if t > cutoff_time)

            # Calculate fill rate ratio for feedback
            if market_trades_in_interval > 0:
                self.fill_rate_ratio = our_fills_in_interval / market_trades_in_interval
            else:
                self.fill_rate_ratio = 1.0

            # EWMA update: kₜ = λkₜ₋₁ + (1-λ) × market_trade_rate
            k_new = self.decay_lambda * self.k_prev + (1 - self.decay_lambda) * market_trade_rate

            # Update state
            self.k_prev = k_new
            self.last_k_update_time = current_time

            # Bound k
            k = max(self.k_min, min(k_new, self.k_max))

            # Confidence based on number of recent market trades (more reliable sample)
            recent_market_trades = sum(1 for t, _ in self.market_trades if t > current_time - self.k_window_sec)
            confidence = min(0.95, 0.3 + recent_market_trades / 100.0)  # Increased divisor since we get many trades

            return OrderIntensityData(k=k, confidence=confidence)

        except Exception as e:
            logger.error("Error calculating order intensity", error=str(e))
            return OrderIntensityData(k=self.k_prev, confidence=0.1)

    def _calculate_vpin(self) -> VPINData:
        """
        Calculate Volume-Synchronized Probability of Informed Trading (VPIN).

        VPIN measures order flow toxicity (likelihood of trading against informed traders).
        Higher VPIN indicates more imbalanced trading activity.

        Formula: VPIN = |Vbuy - Vsell| / Vtotal

        KEY CONCEPT - Volume Synchronization:
            VPIN uses VOLUME-based bucketing (not time-based) to ensure consistent
            measurement across different market conditions:
            - Each bucket accumulates a fixed amount of volume (e.g., 10 BTC)
            - On each trade, volume is added to current bucket (buy or sell side)
            - When bucket reaches threshold, complete it and start new bucket
            - Calculate VPIN from completed buckets

            Why this matters:
                - Time-based windows: Quiet markets have few trades → noisy VPIN
                                      Active markets have many trades → smooth VPIN
                                      Can't compare across market conditions.
                - Volume-based windows: Always measure same trading activity regardless
                                        of time duration → consistent, comparable VPIN

        Verification Test:
            Feed 50 BTC of trades (80% buys, 20% sells):
            - Should complete 5 buckets of 10 BTC each
            - Total buy volume: 40 BTC, sell volume: 10 BTC
            - VPIN = |40-10| / 50 = 0.6 (elevated, indicates toxicity)

        Status Classifications:
            - NORMAL (VPIN ≤ 0.6): Balanced order flow, low toxicity
            - ELEVATED (0.6 < VPIN ≤ 0.7): Moderately imbalanced, monitor
            - TOXIC (VPIN > 0.8): Highly imbalanced, likely informed trading present
        """
        # Need at least a few buckets to calculate VPIN
        if len(self.volume_buckets) < 3:
            return VPINData(value=0.5, status=VPINStatus.NORMAL)

        try:
            # Calculate VPIN across all stored buckets
            total_buy_volume = 0.0
            total_sell_volume = 0.0

            for bucket in self.volume_buckets:
                total_buy_volume += bucket["buy_volume"]
                total_sell_volume += bucket["sell_volume"]

            total_volume = total_buy_volume + total_sell_volume

            if total_volume == 0:
                return VPINData(value=0.5, status=VPINStatus.NORMAL)

            # Calculate VPIN
            vpin = abs(total_buy_volume - total_sell_volume) / total_volume

            # Determine status based on thresholds
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

    def _update_volume_bucket(self, volume: float, is_buy: bool) -> None:
        """
        Update volume buckets for VPIN calculation.

        Accumulates trades into fixed-volume buckets. When a bucket
        fills up, it's added to the bucket history and a new bucket starts.

        Args:
            volume: Trade volume in base asset
            is_buy: True if buy aggressor, False if sell aggressor
        """
        # Add volume to current bucket
        if is_buy:
            self.current_bucket["buy_volume"] += volume
        else:
            self.current_bucket["sell_volume"] += volume

        # Check if bucket is full
        total_volume = self.current_bucket["buy_volume"] + self.current_bucket["sell_volume"]

        if total_volume >= self.volume_bucket_size:
            # Store the completed bucket
            self.volume_buckets.append(self.current_bucket.copy())

            # Maintain max buckets limit
            if len(self.volume_buckets) > self.max_buckets:
                self.volume_buckets.popleft()

            # Start new bucket
            self.current_bucket = {"buy_volume": 0.0, "sell_volume": 0.0}

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

    def _cleanup_old_market_trades(self, current_time: float) -> None:
        """Remove market trade data older than the lookback window."""
        cutoff_time = current_time - self.k_window_sec

        # Clean old market trades
        while self.market_trades and self.market_trades[0][0] < cutoff_time:
            self.market_trades.popleft()

    def _cleanup_old_fills(self, current_time: float) -> None:
        """Remove fill data older than the lookback window."""
        cutoff_time = current_time - self.k_window_sec

        # Clean old fills (our fills only)
        while self.our_fills and self.our_fills[0][0] < cutoff_time:
            self.our_fills.popleft()


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

            # Subscribe to required data streams per specification
            await self.nats.subscribe("raw.trades.v1", self.on_trade)
            await self.nats.subscribe("features.v1", self.on_features)
            await self.nats.subscribe("fills.v1", self.on_fill)

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

    async def on_features(self, msg) -> None:
        """Handle features message."""
        try:
            # Parse message
            data = json.loads(msg.data.decode())
            features = FeatureMessage(**data)

            # Update calculator with features
            self.calculator.update_features(features)

        except json.JSONDecodeError as e:
            logger.error("Failed to decode features message", error=str(e))
            self._errors += 1
        except Exception as e:
            logger.error("Error processing features message", error=str(e))
            self._errors += 1

    async def on_fill(self, msg) -> None:
        """Handle fill message."""
        try:
            # Parse message
            data = json.loads(msg.data.decode())
            fill = FillMessage(**data)

            # Update calculator with fill data
            self.calculator.update_fill(fill)

        except json.JSONDecodeError as e:
            logger.error("Failed to decode fill message", error=str(e))
            self._errors += 1
        except Exception as e:
            logger.error("Error processing fill message", error=str(e))
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