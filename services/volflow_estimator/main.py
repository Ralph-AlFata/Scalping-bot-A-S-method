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
    - Volatility (σ): Per-second realized volatility from market trades (used with time_horizon in AS framework)
    - Order Intensity (k): Avellaneda-Stoikov parameter representing market order arrival rate
    - VPIN: Volume-synchronized toxicity indicator (informed trading probability)
    - Fill Rate Ratio: Our fill rate vs market average (complementary feedback)
"""

import asyncio
import signal
import json
import time
import math
import numpy as np
from typing import Optional, Deque, Dict, Tuple
from collections import deque
from dataclasses import dataclass

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger
from shared.schemas import TradeMessage, DepthSnapshot, VolflowMessage, VolatilityData, OrderIntensityData, VPINData, VPINStatus, FeatureMessage, FillMessage
from shared.sync_client import SyncClient

logger: Optional[object] = None


@dataclass
class OHLC:
    """OHLC data for a time period."""
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: float


@dataclass
class VolatilityEstimate:
    """Result of volatility estimation."""
    ewma: float  # EWMA volatility
    rv: Optional[float]  # Realized volatility
    tsrv: Optional[float]  # Two-scale RV
    har: Optional[float]  # HAR forecast
    yang_zhang: Optional[float]  # Yang-Zhang OHLC volatility
    aggregated: float  # Weighted aggregate
    confidence: float
    quarticity: Optional[float]  # Realized quarticity for uncertainty


class VolflowCalculator:
    """
    Calculates volatility, order intensity, and VPIN with multi-scale estimation.

    UNIT SYSTEM - ALL VOLATILITY IN PER-SECOND:
    ============================================
    All volatility estimates (EWMA, RV, TSRV, HAR, Yang-Zhang) return per-second
    volatility (σ) for consistency across the entire system. This allows direct
    comparison and blending of different estimation methods without unit conversion.

    Example: σ = 0.001 means 0.1% volatility per second
             In 1 minute (60 seconds): σ_minute = 0.001 * sqrt(60) ≈ 0.0077
             In 1 hour (3600 seconds): σ_hour = 0.001 * sqrt(3600) ≈ 0.06

    Lookback windows differ by method (e.g., EWMA uses exponential decay, RV uses
    2-hour window) but all are normalized to the same per-second unit.
    """

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

        # === EWMA Configuration ===
        # Use faster decay (0.88-0.90) for short horizons instead of RiskMetrics 0.94
        self.decay_lambda = getattr(config.strategy.volflow, 'ewma_lambda', 0.88)
        self.decay_lambda_fast = 0.85  # Even faster for extreme responsiveness

        # === Realized Volatility Configuration ===
        self.rv_window_sec = 300  # 5 minutes for RV calculation
        self.rv_lookback_periods = 24  # Last 24 periods (2 hours with 5-min buckets)

        # === HAR Configuration ===
        self.har_lookback_days = 250  # Standard for HAR coefficient estimation
        self.har_enabled = getattr(config.strategy.volflow, 'har_enabled', True)
        self.har_coefficients: Optional[Dict[str, float]] = None
        self.har_last_reestimate_time: Optional[float] = None
        self.har_reestimate_interval_sec = 86400  # Daily reestimation

        # === State for EWMA calculations ===
        self.last_price: Optional[float] = None
        self.sigma_squared: Optional[float] = None  # EWMA variance (per-second)
        self.sigma_squared_fast: Optional[float] = None  # Fast EWMA variance
        self.last_update_time: Optional[float] = None

        # === State for Realized Volatility ===
        self.rv_5min_periods: Deque[float] = deque(maxlen=288)  # Last 24 hours of 5-min RV
        self.rv_current_period_start: Optional[float] = None
        self.rv_current_period_returns: Deque[float] = deque()  # Returns in current 5-min bucket
        self.rv_history: Deque[float] = deque(maxlen=self.rv_lookback_periods)

        # === State for TSRV (Two-Scale RV) ===
        # Store actual returns (not squared) for proper TSRV calculation
        self.tsrv_tick_returns: Deque[tuple] = deque(maxlen=3000)  # (timestamp, return) - tick-level returns
        self.tsrv_low_freq_period_start: Optional[float] = None
        self.tsrv_low_freq_window_sec = 60  # 60 seconds for low-frequency accumulation
        self.tsrv_accumulated_return = 0.0  # Accumulating returns for current low-freq period

        # === State for Yang-Zhang OHLC ===
        self.current_ohlc: Optional[OHLC] = None
        self.ohlc_1min: Deque[OHLC] = deque(maxlen=60)  # Last 60 minutes of OHLC
        self.ohlc_5min: Deque[OHLC] = deque(maxlen=288)  # Last 24 hours of 5-min OHLC

        # === State for order intensity (k) and arrival intensity (A) ===
        self.k_prev: float = 1.0  # Previous k value (spread sensitivity parameter)
        self.A_prev: float = 1.0  # Previous A value (base arrival intensity)
        self.market_trades: Deque[tuple] = deque()  # (timestamp, quantity) - ALL market trades
        self.last_k_update_time: Optional[float] = None

        # State for order book depth (used for k estimation)
        self.last_depth_snapshot: Optional[DepthSnapshot] = None
        self.depth_snapshots: Deque[DepthSnapshot] = deque(maxlen=100)

        # Historical market spread tracking for k estimation
        self.market_spreads: Deque[float] = deque(maxlen=100)

        # === State for fill rate monitoring ===
        self.our_fills: Deque[tuple] = deque()  # (timestamp, quantity) - only OUR fills
        self.fill_rate_ratio: float = 1.0  # Our fill rate / Market trade rate (1.0 = neutral)

        # === State for VPIN - volume buckets ===
        self.volume_bucket_size = 1  # Volume per bucket (in base asset)
        self.volume_buckets: Deque[dict] = deque()  # [{buy_vol, sell_vol}, ...]
        self.current_bucket = {"buy_volume": 0.0, "sell_volume": 0.0}
        self.max_buckets = 50  # Keep last 50 buckets for VPIN calculation

        # === Trade data (for backward compatibility and monitoring) ===
        self.trade_prices: Deque[tuple] = deque()  # (timestamp, price)
        self.trade_volumes: Deque[tuple] = deque()  # (timestamp, volume, side)

        # === Volatility signature plot data ===
        self.vol_sig_1sec: Optional[float] = None
        self.vol_sig_5sec: Optional[float] = None
        self.vol_sig_10sec: Optional[float] = None
        self.vol_sig_15sec: Optional[float] = None

        # === Autocorrelation for noise detection ===
        self.returns_for_acf: Deque[float] = deque(maxlen=500)

        # === Performance tracking ===
        self.vol_estimate_history: Deque[VolatilityEstimate] = deque(maxlen=100)

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

        # Update OHLC buckets for Yang-Zhang volatility estimator
        self._update_ohlc_buckets(timestamp, trade.price, trade.quantity)

        # Cleanup old data
        self._cleanup_old_data(timestamp)
        self._cleanup_old_market_trades(timestamp)

    def update_depth(self, depth: DepthSnapshot) -> None:
        """Update with depth snapshot for k parameter estimation."""
        self.last_depth_snapshot = depth
        self.depth_snapshots.append(depth)

        # Calculate and store current market spread for k estimation
        if depth.bids and depth.asks:
            best_bid = depth.bids[0][0]
            best_ask = depth.asks[0][0]
            spread = best_ask - best_bid
            mid_price = (best_bid + best_ask) / 2.0
            relative_spread = spread / mid_price if mid_price > 0 else 0
            self.market_spreads.append(relative_spread)

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
        Calculate per-second realized volatility using multi-scale estimation.

        All estimators return CONSISTENT per-second units (σ):
        - EWMA: Per-second volatility, exponentially weighted by time
        - RV: Per-second volatility over 2-hour window with 5-min buckets
        - TSRV: Per-second volatility, noise-corrected over 20-minute window
        - HAR: Per-second volatility forecast from multi-horizon components
        - Yang-Zhang: Per-second volatility from OHLC data

        Returns weighted aggregate of all available estimators with confidence metric.
        """
        current_time = time.time()

        # Need at least 2 prices to calculate
        if len(self.trade_prices) < 2:
            return VolatilityData(value=self.vol_min, confidence=0.1)

        try:
            # Calculate all available volatility estimates
            est = self._compute_volatility_estimates(current_time)

            # Store in history for performance analysis
            self.vol_estimate_history.append(est)

            # Return aggregated estimate
            vol = max(self.vol_min, min(est.aggregated, self.vol_max))

            return VolatilityData(value=vol, confidence=est.confidence)

        except Exception as e:
            logger.error("Error calculating volatility", error=str(e))
            return VolatilityData(value=self.vol_min, confidence=0.1)

    def _compute_volatility_estimates(self, current_time: float) -> VolatilityEstimate:
        """
        Compute all volatility estimates and return aggregated result.

        Returns VolatilityEstimate with individual and aggregated values.
        """
        # Update base data structures
        latest_timestamp, latest_price = self.trade_prices[-1]

        # Initialize on first call
        if self.last_price is None or self.sigma_squared is None:
            self._initialize_volatility(latest_timestamp, latest_price)

        # Handle invalid prices
        if latest_price <= 0 or self.last_price <= 0:
            return VolatilityEstimate(
                ewma=self.vol_min, rv=None, tsrv=None, har=None,
                yang_zhang=None, aggregated=self.vol_min, confidence=0.1, quarticity=None
            )

        # Update all volatility tracking structures
        dt_seconds = latest_timestamp - self.last_update_time
        if dt_seconds > 0:
            log_return = math.log(latest_price / self.last_price)
            normalized_return = log_return / math.sqrt(dt_seconds)
            self.returns_for_acf.append(normalized_return)

            # Update RV components
            self._update_rv_buckets(log_return, latest_timestamp)
            self._update_tsrv_returns(log_return, latest_timestamp)

        self.last_price = latest_price
        self.last_update_time = latest_timestamp

        # Calculate individual estimates
        ewma_vol = self._calculate_ewma_volatility()
        rv_vol = self._calculate_realized_volatility()
        tsrv_vol = self._calculate_tsrv_volatility()
        har_vol = self._calculate_har_forecast()
        yy_vol = self._calculate_yang_zhang_volatility()
        quarticity = self._calculate_realized_quarticity()

        # Aggregate estimates with performance-based weighting
        aggregated_vol, confidence = self._aggregate_estimates(
            ewma_vol, rv_vol, tsrv_vol, har_vol, yy_vol
        )

        return VolatilityEstimate(
            ewma=ewma_vol,
            rv=rv_vol,
            tsrv=tsrv_vol,
            har=har_vol,
            yang_zhang=yy_vol,
            aggregated=aggregated_vol,
            confidence=confidence,
            quarticity=quarticity
        )

    def _initialize_volatility(self, timestamp: float, price: float) -> None:
        """Initialize volatility state variables."""
        self.last_price = price
        self.last_update_time = timestamp
        self.rv_current_period_start = timestamp

        # Initialize sigma_squared as per-second variance (not annualized)
        # Start at vol_min to avoid extremely small initial values
        self.sigma_squared = self.vol_min ** 2
        self.sigma_squared_fast = self.sigma_squared

    def _update_rv_buckets(self, log_return: float, timestamp: float) -> None:
        """Update 5-minute RV buckets for realized volatility calculation."""
        if self.rv_current_period_start is None:
            self.rv_current_period_start = timestamp

        squared_return = log_return ** 2
        self.rv_current_period_returns.append(squared_return)

        # Check if 5-minute period is complete
        elapsed = timestamp - self.rv_current_period_start
        if elapsed >= self.rv_window_sec:
            # Calculate RV for this period and add to history
            period_rv = sum(self.rv_current_period_returns)
            self.rv_5min_periods.append(period_rv)
            self.rv_history.append(period_rv)

            # Reset for next period
            self.rv_current_period_returns.clear()
            self.rv_current_period_start = timestamp

    def _update_tsrv_returns(self, log_return: float, timestamp: float) -> None:
        """
        Update returns for TSRV calculation.

        TSRV requires tick-level returns stored separately, not squared yet.
        We'll accumulate returns into low-frequency periods during calculation.
        """
        # Store tick-level return with timestamp
        self.tsrv_tick_returns.append((timestamp, log_return))

    def _calculate_ewma_volatility(self) -> float:
        """
        Calculate EWMA per-second volatility with time normalization.

        FIXED VERSION - Handles irregular time intervals properly:

        Theory: When observations arrive at irregular intervals, the decay factor
        must be adjusted based on actual time elapsed. From RiskMetrics:
        lambda_adjusted = lambda^(dt_actual / dt_target)

        This ensures that a 2-second gap gives the same weight as two 1-second gaps,
        rather than treating long intervals the same as short ones.

        Formula with time adjustment:
        1. Calculate time-adjusted lambda: λ_adj = λ^(dt / dt_target)
        2. Update variance: σ²ₜ = λ_adj × σ²ₜ₋₁ + (1-λ_adj) × r²/dt

        Note: We normalize the squared return by dt because a return over 2 seconds
        should contribute twice the variance per second as a return over 1 second.

        Returns: Per-second volatility (σ)
        """
        if self.last_price is None or len(self.trade_prices) < 2:
            return self.vol_min

        try:
            latest_timestamp, latest_price = self.trade_prices[-1]
            dt_seconds = latest_timestamp - self.last_update_time

            if dt_seconds <= 0:
                return self.vol_min

            log_return = math.log(latest_price / self.last_price)

            # ========================================
            # Time-adjusted EWMA with proper handling of irregular intervals
            # ========================================

            # Target interval: 1 second (our base unit for volatility)
            dt_target = 1.0

            # Time-adjusted lambda: λ_adj = λ^(dt_actual / dt_target)
            # This makes the effective decay proportional to time elapsed
            lambda_adjusted = self.decay_lambda ** (dt_seconds / dt_target)
            lambda_adjusted_fast = self.decay_lambda_fast ** (dt_seconds / dt_target)

            # Squared return normalized by time interval
            # Divide by dt because variance accumulates linearly with time
            squared_return_per_second = (log_return ** 2) / dt_seconds

            # EWMA update with time-adjusted lambda
            # σ²ₜ = λ_adj × σ²ₜ₋₁ + (1-λ_adj) × (r²/dt)
            self.sigma_squared = (
                lambda_adjusted * self.sigma_squared +
                (1 - lambda_adjusted) * squared_return_per_second
            )

            # Fast EWMA with time-adjusted lambda
            self.sigma_squared_fast = (
                lambda_adjusted_fast * self.sigma_squared_fast +
                (1 - lambda_adjusted_fast) * squared_return_per_second
            )

            sigma = math.sqrt(self.sigma_squared)
            return max(self.vol_min, min(sigma, self.vol_max))

        except Exception as e:
            return self.vol_min

    def _calculate_realized_volatility(self) -> Optional[float]:
        """
        Calculate per-second realized volatility over recent window.

        FIXED VERSION - Reduced lookback for faster adaptation:

        Changed from 24 periods (2 hours) to 12 periods (1 hour) by default.
        For second-to-minute scale trading, a 2-hour window is too slow to react
        to changing market conditions.

        Uses 5-minute buckets to avoid microstructure noise while capturing
        actual volatility in the recent trading period.

        Formula: RV = sqrt(sum(r²_i) / num_seconds)
        where r²_i = squared log returns in each 5-minute period

        The lookback period is now configurable via config.strategy.volflow.rv_lookback_periods

        Returns: Per-second volatility (σ)
        """
        if len(self.rv_5min_periods) < 2:
            return None

        try:
            # Calculate RV over last N periods (configurable, default 12 = 1 hour)
            lookback = min(self.rv_lookback_periods, len(self.rv_5min_periods))
            recent_rv = list(self.rv_5min_periods)[-lookback:]
            total_rv = sum(recent_rv)

            # Convert to per-second volatility
            # Each 5-minute period = 300 seconds
            num_seconds = len(recent_rv) * 300
            rv_volatility = math.sqrt(total_rv / num_seconds)

            return max(self.vol_min, min(rv_volatility, self.vol_max))

        except Exception:
            return None

    def _calculate_tsrv_volatility(self) -> Optional[float]:
        """
        Calculate Two-Scale Realized Volatility (TSRV) - per-second.

        FIXED VERSION - Addresses dimensional inconsistencies:

        Theory: TSRV corrects for microstructure noise by comparing RV at different
        sampling frequencies over the SAME time window. The key insight is that noise
        gets amplified at higher frequencies.

        Proper implementation:
        1. Define a time window (e.g., last 20 minutes)
        2. Calculate high-frequency RV: sum of all tick-level squared returns in window
        3. Calculate low-frequency RV: accumulate tick returns into 60-second periods,
           then sum squared accumulated returns
        4. Both measure variance over the SAME calendar time
        5. Apply Zhang-Mykland-Aït-Sahalia bias correction

        Formula: TSRV = RV_low - bias_correction
                 where bias_correction accounts for noise contamination
                 bias_factor = (avg_spacing - 1) / avg_spacing
                 avg_spacing = number of high-freq obs per low-freq obs

        Returns: Per-second volatility (σ)
        """
        if len(self.tsrv_tick_returns) < 100:
            return None

        try:
            current_time = time.time()
            window_sec = 1200  # 20-minute window

            # Get tick returns within window
            cutoff_time = current_time - window_sec
            window_returns = [(t, r) for t, r in self.tsrv_tick_returns if t >= cutoff_time]

            if len(window_returns) < 50:
                return None

            # ========================================
            # High-frequency RV: sum of squared tick returns
            # ========================================
            rv_high = sum(r ** 2 for _, r in window_returns)
            n_high = len(window_returns)

            # ========================================
            # Low-frequency RV: accumulate into 60-second periods
            # ========================================
            # Group returns into 60-second buckets
            low_freq_returns = []
            bucket_start = window_returns[0][0]
            accumulated_return = 0.0

            for timestamp, ret in window_returns:
                if timestamp - bucket_start >= self.tsrv_low_freq_window_sec:
                    # Complete current bucket
                    low_freq_returns.append(accumulated_return)
                    # Start new bucket
                    bucket_start = timestamp
                    accumulated_return = ret
                else:
                    # Accumulate into current bucket
                    accumulated_return += ret

            # Add final bucket if it has data
            if len(window_returns) > 0:
                time_in_final_bucket = window_returns[-1][0] - bucket_start
                if time_in_final_bucket > 0.1 * self.tsrv_low_freq_window_sec:  # At least 10% of period
                    low_freq_returns.append(accumulated_return)

            if len(low_freq_returns) < 5:  # Need at least 5 low-freq periods
                return None

            # Calculate low-frequency RV
            rv_low = sum(r ** 2 for r in low_freq_returns)
            n_low = len(low_freq_returns)

            # ========================================
            # Bias correction (Zhang-Mykland-Aït-Sahalia)
            # ========================================
            # Average spacing: how many high-freq obs per low-freq obs
            avg_spacing = n_high / n_low if n_low > 0 else 1

            # Bias factor
            bias_factor = (avg_spacing - 1) / avg_spacing if avg_spacing > 1 else 0

            # Estimate noise variance from the difference
            # The difference (rv_high - rv_low) estimates noise contamination
            noise_contribution = max(0, rv_high - rv_low)

            # Apply bias correction
            # TSRV = rv_low - bias_factor × noise_contribution / n_low
            tsrv_var = max(0, rv_low - bias_factor * noise_contribution)

            # ========================================
            # Convert to per-second volatility
            # ========================================
            # Total window duration
            actual_window_duration = window_returns[-1][0] - window_returns[0][0]
            if actual_window_duration <= 0:
                return None

            # Per-second variance, then take square root
            tsrv_volatility = math.sqrt(tsrv_var / actual_window_duration)

            return max(self.vol_min, min(tsrv_volatility, self.vol_max))

        except Exception as e:
            logger.debug("Error calculating TSRV", error=str(e))
            return None

    def _calculate_har_forecast(self) -> Optional[float]:
        """
        Calculate HAR (Heterogeneous Autoregressive) volatility forecast - per-second.

        FIXED VERSION - Corrected variable naming for crypto 24/7 markets:

        HAR-RV: σ²_forecast = β₀ + β₁·RV_1h + β₂·RV_12h + β₃·RV_24h

        In traditional finance, HAR uses daily/weekly/monthly horizons. For crypto
        trading 24/7 at second-to-minute scale, we adapt these to:
        - Short-term (rv_1h): Last 12 periods (1 hour) - replaces "daily"
        - Medium-term (rv_12h): Last 144 periods (12 hours) - replaces "weekly"
        - Long-term (rv_24h): Last 288 periods (24 hours) - replaces "monthly"

        NOTE: The current implementation uses FIXED coefficients (not estimated from data).
        For production use, coefficients should be estimated via OLS regression on
        historical data, which requires collecting at least 250 periods of RV history.

        Returns: Per-second volatility (σ)
        """
        if not self.har_enabled or len(self.rv_history) < 50:
            return None

        try:
            # Re-estimate coefficients if needed
            current_time = time.time()
            if self.har_last_reestimate_time is None or \
               (current_time - self.har_last_reestimate_time) > self.har_reestimate_interval_sec:
                self._estimate_har_coefficients(current_time)

            if self.har_coefficients is None:
                return None

            # Calculate HAR components with correct naming
            rv_hist = list(self.rv_history)

            # Short-term component: last 12 periods (1 hour with 5-min buckets)
            rv_1h = np.mean(rv_hist[-12:]) if len(rv_hist) >= 12 else np.mean(rv_hist)

            # Medium-term component: last 144 periods (12 hours)
            rv_12h = np.mean(rv_hist[-144:]) if len(rv_hist) >= 144 else np.mean(rv_hist)

            # Long-term component: last 288 periods (24 hours)
            rv_24h = np.mean(rv_hist[-288:]) if len(rv_hist) >= 288 else np.mean(rv_hist)

            # HAR forecast: weighted average of variance at different horizons
            # Each component is already per-second variance (from rv_5min_periods)
            har_var = (
                self.har_coefficients.get('beta_0', 0) +
                self.har_coefficients.get('beta_1h', 0.4) * rv_1h +
                self.har_coefficients.get('beta_12h', 0.3) * rv_12h +
                self.har_coefficients.get('beta_24h', 0.3) * rv_24h
            )

            har_vol = math.sqrt(max(0, har_var))
            return max(self.vol_min, min(har_vol, self.vol_max))

        except Exception:
            return None

    def _estimate_har_coefficients(self, current_time: float) -> None:
        """
        Estimate HAR coefficients using OLS regression on historical RV data.

        FIXED VERSION - Updated coefficient names to reflect actual time periods:

        Fit: RV_{t+1} = β₀ + β₁·RV_1h + β₂·RV_12h + β₃·RV_24h + ε_t

        TODO: For production deployment, implement proper OLS regression using
        historical data (requires at least 250 periods). The current fixed coefficients
        (0.4, 0.3, 0.3) are placeholders and may not be optimal for your specific
        cryptocurrency and market conditions.
        """
        try:
            if len(self.rv_history) < 50:
                return

            # TEMPORARY: Use fixed default coefficients
            # TODO: Implement OLS estimation with historical data
            self.har_coefficients = {
                'beta_0': 0.0001,
                'beta_1h': 0.4,   # 1-hour horizon weight
                'beta_12h': 0.3,  # 12-hour horizon weight
                'beta_24h': 0.3   # 24-hour horizon weight
            }
            self.har_last_reestimate_time = current_time

            logger.debug(
                "HAR coefficients set (using fixed values - not estimated from data)",
                coefficients=self.har_coefficients
            )

        except Exception as e:
            logger.debug("Error setting HAR coefficients", error=str(e))

    def _calculate_yang_zhang_volatility(self) -> Optional[float]:
        """
        Calculate Yang-Zhang volatility estimator using OHLC data - per-second.

        FIXED VERSION - Addresses double-counting time normalization:

        Theory: Yang-Zhang combines multiple OHLC-based variance components:
        - Rogers-Satchell (RS): drift-independent range estimator
        - Garman-Klass (GK): high-low range estimator
        Each component measures variance for a single OHLC period

        Proper aggregation:
        1. Calculate Yang-Zhang variance for EACH 5-minute bar separately
        2. Each estimate is variance for that 300-second period
        3. Average these variances to get mean variance per 300-second period
        4. Divide by 300 to get per-second variance
        5. Take square root to get per-second volatility

        The KEY FIX: Don't average variance components then divide by period length.
        Instead, recognize each component measures its own 300-second period,
        so averaging gives mean variance per 300-second period, then convert to per-second.

        Alternative (clearer) approach:
        For each bar, calculate YZ volatility (vol for 300-sec period),
        convert to per-second (divide by sqrt(300)),
        then average those per-second volatilities.

        Returns: Per-second volatility (σ)
        """
        if len(self.ohlc_5min) < 3:
            return None

        try:
            ohlc_data = list(self.ohlc_5min)[-20:]  # Last 20 periods (100 minutes)

            if not ohlc_data:
                return None

            # Calculate per-second volatility for each OHLC bar
            per_second_vols = []

            for ohlc in ohlc_data:
                if ohlc.open <= 0 or ohlc.high <= 0 or ohlc.low <= 0 or ohlc.close <= 0:
                    continue

                # Rogers-Satchell component (drift-independent)
                rs = math.log(ohlc.high / ohlc.close) * math.log(ohlc.high / ohlc.open) + \
                     math.log(ohlc.low / ohlc.close) * math.log(ohlc.low / ohlc.open)

                # Garman-Klass components
                hl = math.log(ohlc.high / ohlc.low) ** 2 / (4 * math.log(2))
                co = (2 * math.log(2) - 1) * (math.log(ohlc.close / ohlc.open) ** 2)

                # Yang-Zhang variance for this bar (variance over 300 seconds)
                yy_var_bar = max(0, hl - co + rs)

                # Convert to per-second variance then to per-second volatility
                # This bar represents 300 seconds, so var_per_second = yy_var_bar / 300
                per_second_var = yy_var_bar / 300.0
                per_second_vol = math.sqrt(per_second_var)

                per_second_vols.append(per_second_vol)

            if not per_second_vols:
                return None

            # Average the per-second volatilities
            avg_yy_vol = np.mean(per_second_vols)

            return max(self.vol_min, min(avg_yy_vol, self.vol_max))

        except Exception as e:
            logger.debug("Error calculating Yang-Zhang volatility", error=str(e))
            return None

    def _calculate_realized_quarticity(self) -> Optional[float]:
        """
        Calculate realized quarticity for uncertainty quantification.

        RQ = (1/n) * Σ(r_i^4)

        Used in HARQ model to construct confidence intervals for volatility estimates.
        """
        try:
            if len(self.returns_for_acf) < 50:
                return None

            recent_returns = list(self.returns_for_acf)[-100:]
            quarticity = np.mean([r ** 4 for r in recent_returns])

            return max(0, quarticity)

        except Exception:
            return None

    def _aggregate_estimates(self, ewma: float, rv: Optional[float],
                            tsrv: Optional[float], har: Optional[float],
                            yy: Optional[float]) -> Tuple[float, float]:
        """
        Aggregate multiple volatility estimates using performance-based weighting.

        Weighting strategy:
        - EWMA: 0.25 (responsive, but only uses latest data)
        - RV: 0.35 (accurate, robust, recommended in research)
        - TSRV: 0.20 (noise-robust, good for high-frequency)
        - HAR: 0.15 (forward-looking, captures multi-horizon structure)
        - Yang-Zhang: 0.05 (efficient but less available)
        """
        estimates = []
        weights = []
        active_estimators = []

        # Always have EWMA
        estimates.append(ewma)
        weights.append(0.25)
        active_estimators.append(f"EWMA={ewma:.6f}")

        # Add RV if available (primary backup)
        if rv is not None:
            estimates.append(rv)
            weights.append(0.35)
            active_estimators.append(f"RV={rv:.6f}")

        # Add TSRV if available (noise-robust)
        if tsrv is not None:
            estimates.append(tsrv)
            weights.append(0.20)
            active_estimators.append(f"TSRV={tsrv:.6f}")

        # Add HAR if available (forward-looking)
        if har is not None:
            estimates.append(har)
            weights.append(0.15)
            active_estimators.append(f"HAR={har:.6f}")

        # Add Yang-Zhang if available
        if yy is not None:
            estimates.append(yy)
            weights.append(0.05)
            active_estimators.append(f"YZ={yy:.6f}")

        # Normalize weights
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]

        # Calculate weighted average
        aggregated = sum(e * w for e, w in zip(estimates, weights))

        # Confidence: higher when we have multiple estimates
        base_confidence = 0.5
        num_estimates = len(estimates)
        confidence = min(0.95, base_confidence + (num_estimates - 1) * 0.1)

        # Log active estimators for debugging
        logger.debug(
            "Volatility estimation",
            active_estimators=", ".join(active_estimators),
            aggregated=f"{aggregated:.6f}",
            confidence=f"{confidence:.2f}",
            num_estimators=num_estimates
        )

        return aggregated, confidence

    def _calculate_order_intensity(self) -> OrderIntensityData:
        """
        Calculate order intensity parameters for Avellaneda-Stoikov framework.

        THEORY (Avellaneda-Stoikov 2008):
            Fill probability: λ(δ) = A × exp(-k × δ)
            where:
                λ(δ) = probability of order fill per unit time at distance δ from mid-price
                A = base arrival intensity (fills per second at δ=0, at mid-price)
                k = spread sensitivity parameter (how fast fill probability decays with distance)
                δ = distance from mid-price (spread)

        CRITICAL DISTINCTION:
            - A (arrival intensity): Base rate of market orders (trades/second)
            - k (spread sensitivity): How sensitive fills are to spread distance

            The previous implementation confused these! It calculated A and called it k.

        IMPLEMENTATION - Three-Method Hybrid:

            1. Calculate A (arrival intensity):
               Aₜ = λAₜ₋₁ + (1-λ) × (market_trades_in_interval / interval_duration)
               This is the base market order arrival rate (what the old code calculated).

            2. Calculate k (spread sensitivity) using one of three methods:

               Method A (Order Book): Fit exponential decay to order book depth
               If depth follows: depth(δ) ∝ exp(-k × δ)
               Then k = -slope of log(depth) vs distance

               Method B (Simple Heuristic): Use inverse of average market spread
               k = 1 / average_market_spread
               Intuition: Tight spreads → high k (steep drop-off)
                         Wide spreads → low k (gentle drop-off)

               Method C (Fallback): Use configured default if insufficient data

        Returns:
            OrderIntensityData with k parameter (and A is tracked internally)
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

            # ========================================
            # STEP 1: Calculate A (arrival intensity)
            # ========================================
            cutoff_time = current_time - interval_duration
            market_trades_in_interval = sum(1 for t, _ in self.market_trades if t > cutoff_time)
            market_trade_rate = market_trades_in_interval / interval_duration

            # EWMA update for A: Aₜ = λAₜ₋₁ + (1-λ) × market_trade_rate
            self.A_prev = self.decay_lambda * self.A_prev + (1 - self.decay_lambda) * market_trade_rate

            # ========================================
            # STEP 2: Calculate k (spread sensitivity)
            # ========================================
            k_new = self._estimate_spread_sensitivity_k()

            # Update state
            self.k_prev = k_new
            self.last_k_update_time = current_time

            # Bound k
            k = max(self.k_min, min(k_new, self.k_max))
            k_bounded = k != k_new

            # ========================================
            # STEP 3: Calculate fill rate ratio (monitoring metric)
            # ========================================
            our_fills_in_interval = sum(1 for t, _ in self.our_fills if t > cutoff_time)
            if market_trades_in_interval > 0:
                self.fill_rate_ratio = our_fills_in_interval / market_trades_in_interval
            else:
                self.fill_rate_ratio = 1.0

            # Confidence based on data availability
            recent_market_trades = sum(1 for t, _ in self.market_trades if t > current_time - self.k_window_sec)
            num_spreads = len(self.market_spreads)
            confidence = min(0.95, 0.3 + (recent_market_trades / 100.0) * 0.3 + (num_spreads / 50.0) * 0.35)

            # Log order intensity details
            logger.debug(
                "Order intensity calculation",
                market_trades_in_interval=market_trades_in_interval,
                A_arrival_intensity=f"{self.A_prev:.2f} trades/sec",
                k_spread_sensitivity=f"{k:.4f}",
                k_unbounded=f"{k_new:.4f}",
                was_capped=k_bounded,
                k_min=self.k_min,
                k_max=self.k_max,
                num_spreads_tracked=len(self.market_spreads),
                num_depth_snapshots=len(self.depth_snapshots),
                recent_market_trades=recent_market_trades,
                confidence=f"{confidence:.2f}",
                fill_rate_ratio=f"{self.fill_rate_ratio:.3f}"
            )

            return OrderIntensityData(k=k, confidence=confidence)

        except Exception as e:
            logger.error("Error calculating order intensity", error=str(e))
            return OrderIntensityData(k=self.k_prev, confidence=0.1)

    def _estimate_spread_sensitivity_k(self) -> float:
        """
        Estimate k (spread sensitivity parameter) using available data.

        Three methods in order of preference:
        1. Order book depth exponential decay fitting
        2. Simple heuristic: k = 1 / average_market_spread
        3. Fallback: previous k value

        Returns:
            Estimated k parameter
        """
        # Method 1: Try order book fitting if we have depth snapshots
        if len(self.depth_snapshots) >= 5:
            k_from_orderbook = self._estimate_k_from_orderbook()
            if k_from_orderbook is not None:
                return k_from_orderbook

        # Method 2: Simple heuristic from market spreads
        if len(self.market_spreads) >= 10:
            avg_spread = np.mean(list(self.market_spreads)[-20:])
            if avg_spread > 0:
                # k = 1 / average_spread (relative spread)
                # Intuition: tight spreads → high k, wide spreads → low k
                k_estimate = 1.0 / avg_spread

                # Apply smoothing with previous k
                k_smoothed = 0.7 * self.k_prev + 0.3 * k_estimate
                return k_smoothed

        # Method 3: Fallback to previous k
        return self.k_prev

    def _estimate_k_from_orderbook(self) -> Optional[float]:
        """
        Estimate k from order book depth distribution.

        Theory: If depth decays exponentially: depth(δ) = D₀ × exp(-k × δ)
        Then: log(depth) = log(D₀) - k × δ
        So k is the negative slope of log(depth) vs distance from mid.

        Returns:
            Estimated k from order book, or None if insufficient data
        """
        try:
            if not self.last_depth_snapshot:
                return None

            depth = self.last_depth_snapshot
            if not depth.bids or not depth.asks:
                return None

            # Calculate mid price
            best_bid = depth.bids[0][0]
            best_ask = depth.asks[0][0]
            mid_price = (best_bid + best_ask) / 2.0

            if mid_price <= 0:
                return None

            # Collect (distance, depth) pairs for both sides
            distances = []
            depths = []

            # Ask side (distance positive)
            for price, qty in depth.asks[:10]:  # Use top 10 levels
                if qty > 0:
                    distance = abs(price - mid_price) / mid_price  # Relative distance
                    distances.append(distance)
                    depths.append(qty)

            # Bid side (distance positive)
            for price, qty in depth.bids[:10]:
                if qty > 0:
                    distance = abs(price - mid_price) / mid_price  # Relative distance
                    distances.append(distance)
                    depths.append(qty)

            if len(distances) < 5:  # Need at least 5 points
                return None

            # Fit exponential decay: log(depth) = a - k × distance
            # Use numpy for linear regression on log scale
            log_depths = np.log(np.array(depths))
            distances_arr = np.array(distances)

            # Simple linear regression: slope = -k
            # slope = cov(x, y) / var(x)
            mean_dist = np.mean(distances_arr)
            mean_log_depth = np.mean(log_depths)

            cov = np.mean((distances_arr - mean_dist) * (log_depths - mean_log_depth))
            var_dist = np.var(distances_arr)

            if var_dist < 1e-10:  # Avoid division by zero
                return None

            slope = cov / var_dist
            k_estimate = -slope  # k = -slope since depth(δ) = exp(-k×δ)

            # Sanity check: k should be positive and reasonable
            if k_estimate > 0 and k_estimate < 1000:
                return k_estimate

            return None

        except Exception as e:
            logger.debug("Error estimating k from order book", error=str(e))
            return None

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

    def _update_ohlc_buckets(self, timestamp: float, price: float, volume: float) -> None:
        """
        Update OHLC buckets for Yang-Zhang volatility estimator.

        Creates 1-minute and 5-minute OHLC bars from trade data.

        Args:
            timestamp: Trade timestamp in seconds
            price: Trade price
            volume: Trade volume
        """
        # Initialize current OHLC if needed
        if self.current_ohlc is None:
            self.current_ohlc = OHLC(
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
                timestamp=timestamp
            )
            return

        # Update current OHLC
        self.current_ohlc.high = max(self.current_ohlc.high, price)
        self.current_ohlc.low = min(self.current_ohlc.low, price)
        self.current_ohlc.close = price
        self.current_ohlc.volume += volume

        # Check if we should complete current 1-minute bar
        elapsed_1min = timestamp - self.current_ohlc.timestamp
        if elapsed_1min >= 60.0:  # 1 minute
            # Store completed 1-minute OHLC
            self.ohlc_1min.append(self.current_ohlc)

            logger.debug(
                "Completed 1-minute OHLC bar",
                ohlc_1min_count=len(self.ohlc_1min),
                open=self.current_ohlc.open,
                high=self.current_ohlc.high,
                low=self.current_ohlc.low,
                close=self.current_ohlc.close,
                volume=self.current_ohlc.volume
            )

            # Start new OHLC bar
            self.current_ohlc = OHLC(
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
                timestamp=timestamp
            )

        # Check if we should complete current 5-minute bar
        # Create 5-minute bars from 1-minute bars when we have 5 of them
        if len(self.ohlc_1min) >= 5:
            # Check if last 5 bars span at least 5 minutes
            last_5_bars = list(self.ohlc_1min)[-5:]
            time_span = last_5_bars[-1].timestamp - last_5_bars[0].timestamp

            if time_span >= 300.0:  # 5 minutes
                # Combine last 5 bars into 1 five-minute bar
                ohlc_5min = OHLC(
                    open=last_5_bars[0].open,
                    high=max(bar.high for bar in last_5_bars),
                    low=min(bar.low for bar in last_5_bars),
                    close=last_5_bars[-1].close,
                    volume=sum(bar.volume for bar in last_5_bars),
                    timestamp=last_5_bars[0].timestamp
                )
                self.ohlc_5min.append(ohlc_5min)

                logger.debug(
                    "Completed 5-minute OHLC bar",
                    ohlc_5min_count=len(self.ohlc_5min),
                    open=ohlc_5min.open,
                    high=ohlc_5min.high,
                    low=ohlc_5min.low,
                    close=ohlc_5min.close,
                    volume=ohlc_5min.volume
                )

                # Remove the bars we just aggregated
                for _ in range(5):
                    if len(self.ohlc_1min) > 0:
                        self.ohlc_1min.popleft()

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

    def get_volatility_signature(self) -> Dict[str, Optional[float]]:
        """
        Calculate volatility signature plot data to detect microstructure noise.

        Returns volatility at different sampling frequencies. If volatility increases
        as frequency increases, this indicates microstructure noise contamination.

        Expected pattern without noise: RV should be relatively stable or decrease
        slightly as sampling frequency increases (high-frequency noise filters out).
        """
        try:
            returns = list(self.returns_for_acf)
            if len(returns) < 100:
                return {
                    '1sec': None,
                    '5sec': None,
                    '10sec': None,
                    '15sec': None
                }

            # Calculate RV at different frequencies
            rv_1sec = np.mean([r ** 2 for r in returns[-100:]])  # ~100 returns
            rv_5sec = np.mean([r ** 2 for r in returns[-20:]])   # ~20 returns
            rv_10sec = np.mean([r ** 2 for r in returns[-10:]])  # ~10 returns
            rv_15sec = np.mean([r ** 2 for r in returns[-6:]])   # ~6 returns

            self.vol_sig_1sec = math.sqrt(rv_1sec) if rv_1sec > 0 else None
            self.vol_sig_5sec = math.sqrt(rv_5sec) if rv_5sec > 0 else None
            self.vol_sig_10sec = math.sqrt(rv_10sec) if rv_10sec > 0 else None
            self.vol_sig_15sec = math.sqrt(rv_15sec) if rv_15sec > 0 else None

            return {
                '1sec': self.vol_sig_1sec,
                '5sec': self.vol_sig_5sec,
                '10sec': self.vol_sig_10sec,
                '15sec': self.vol_sig_15sec
            }

        except Exception:
            return {
                '1sec': None,
                '5sec': None,
                '10sec': None,
                '15sec': None
            }

    def detect_microstructure_noise(self) -> Dict[str, object]:
        """
        Detect microstructure noise contamination using autocorrelation test.

        Microstructure noise induces negative autocorrelation in returns
        (from inventory control models) or positive autocorrelation
        (from order flow continuation).

        Returns dict with:
        - acf_lag1: First-order autocorrelation
        - is_noisy: Boolean indicating if noise is detected
        - noise_type: 'negative' (bid-ask bounce), 'positive' (order flow), or 'none'
        """
        try:
            if len(self.returns_for_acf) < 50:
                return {
                    'acf_lag1': None,
                    'is_noisy': False,
                    'noise_type': 'unknown'
                }

            returns = list(self.returns_for_acf)[-100:]
            mean_ret = np.mean(returns)
            demeaned = [r - mean_ret for r in returns]

            # Calculate lag-1 autocorrelation
            numerator = sum(demeaned[i] * demeaned[i + 1] for i in range(len(demeaned) - 1))
            denominator = sum(r ** 2 for r in demeaned)

            acf_lag1 = numerator / denominator if denominator > 0 else 0

            # Thresholds for noise detection
            is_noisy = abs(acf_lag1) > 0.1
            if acf_lag1 < -0.1:
                noise_type = 'negative (bid-ask bounce)'
            elif acf_lag1 > 0.1:
                noise_type = 'positive (order flow continuation)'
            else:
                noise_type = 'none'

            return {
                'acf_lag1': float(acf_lag1),
                'is_noisy': is_noisy,
                'noise_type': noise_type
            }

        except Exception:
            return {
                'acf_lag1': None,
                'is_noisy': False,
                'noise_type': 'error'
            }

    def get_volatility_diagnostics(self) -> Dict[str, object]:
        """
        Get comprehensive volatility diagnostics for monitoring and debugging.

        Includes:
        - Current estimates from all methods
        - Confidence metrics
        - Noise detection results
        - Signature plot data
        - HAR coefficient status
        """
        try:
            latest_est = self.vol_estimate_history[-1] if self.vol_estimate_history else None

            return {
                'current_estimate': {
                    'aggregated': latest_est.aggregated if latest_est else None,
                    'ewma': latest_est.ewma if latest_est else None,
                    'rv': latest_est.rv if latest_est else None,
                    'tsrv': latest_est.tsrv if latest_est else None,
                    'har': latest_est.har if latest_est else None,
                    'yang_zhang': latest_est.yang_zhang if latest_est else None,
                    'confidence': latest_est.confidence if latest_est else None,
                    'quarticity': latest_est.quarticity if latest_est else None
                },
                'noise_detection': self.detect_microstructure_noise(),
                'volatility_signature': self.get_volatility_signature(),
                'har_status': {
                    'enabled': self.har_enabled,
                    'coefficients': self.har_coefficients,
                    'last_reestimate': self.har_last_reestimate_time
                },
                'data_health': {
                    'trade_prices_count': len(self.trade_prices),
                    'rv_history_count': len(self.rv_history),
                    'returns_for_acf_count': len(self.returns_for_acf),
                    'ohlc_5min_count': len(self.ohlc_5min)
                }
            }

        except Exception:
            return {
                'error': 'Failed to generate diagnostics'
            }


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

        # Initialize sync client
        self.sync_client = SyncClient(config, self.nats, "volflow_estimator")

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting VolflowEstimator")
        try:
            await self.nats.connect()

            # Start sync client
            await self.sync_client.start()

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

            # Wait for sync point
            sync_point_ms = int(await self.sync_client.wait_for_sync_point(trade.timestamp_ms))

            # Update calculator
            self.calculator.update_trade(trade)

            # Try to publish volflow estimate
            volflow = self.calculator.calculate_volflow()
            if volflow:
                # Update volflow timestamp to sync point
                volflow.timestamp_ms = sync_point_ms
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
        await self.sync_client.stop()
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