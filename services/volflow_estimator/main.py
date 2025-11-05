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
        self.tsrv_high_freq_returns: Deque[float] = deque(maxlen=3000)  # Last 1 hour of 1.2s returns
        self.tsrv_low_freq_returns: Deque[float] = deque(maxlen=288)  # Last 24 hours of 5-min returns

        # === State for Yang-Zhang OHLC ===
        self.current_ohlc: Optional[OHLC] = None
        self.ohlc_1min: Deque[OHLC] = deque(maxlen=60)  # Last 60 minutes of OHLC
        self.ohlc_5min: Deque[OHLC] = deque(maxlen=288)  # Last 24 hours of 5-min OHLC

        # === State for order intensity (k) ===
        self.k_prev: float = 1.0  # Previous k value (market order intensity)
        self.market_trades: Deque[tuple] = deque()  # (timestamp, quantity) - ALL market trades
        self.last_k_update_time: Optional[float] = None

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
        """Update returns for TSRV calculation."""
        self.tsrv_high_freq_returns.append(log_return ** 2)

    def _calculate_ewma_volatility(self) -> float:
        """
        Calculate EWMA per-second volatility with time normalization.

        Uses optimized lambda (0.88) for short-horizon crypto trading
        instead of RiskMetrics 0.94.

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
            # Normalize return to per-second basis
            normalized_return = log_return / math.sqrt(dt_seconds)

            # EWMA update: σ²ₜ = λσ²ₜ₋₁ + (1-λ)r²ₜ (per-second variance)
            self.sigma_squared = (
                self.decay_lambda * self.sigma_squared +
                (1 - self.decay_lambda) * (normalized_return ** 2)
            )

            # Fast EWMA with lambda=0.85 for comparison
            self.sigma_squared_fast = (
                self.decay_lambda_fast * self.sigma_squared_fast +
                (1 - self.decay_lambda_fast) * (normalized_return ** 2)
            )

            sigma = math.sqrt(self.sigma_squared)
            return max(self.vol_min, min(sigma, self.vol_max))

        except Exception:
            return self.vol_min

    def _calculate_realized_volatility(self) -> Optional[float]:
        """
        Calculate per-second realized volatility over recent 2-hour window.

        Uses 5-minute buckets to avoid microstructure noise while capturing
        actual volatility in the recent trading period.

        Formula: RV = sqrt(sum(r²_i) / num_seconds)
        where r²_i = squared log returns in each 5-minute period

        Returns: Per-second volatility (σ)
        """
        if len(self.rv_5min_periods) < 2:
            return None

        try:
            # Calculate RV over last 24 periods (2 hours with 5-min samples)
            recent_rv = list(self.rv_5min_periods)[-24:]
            total_rv = sum(recent_rv)

            # Convert to per-second volatility
            # Each 5-minute period = 300 seconds
            num_seconds = len(recent_rv) * 300  # 24 * 300 = 7200 seconds (2 hours)
            rv_volatility = math.sqrt(total_rv / num_seconds)

            return max(self.vol_min, min(rv_volatility, self.vol_max))

        except Exception:
            return None

    def _calculate_tsrv_volatility(self) -> Optional[float]:
        """
        Calculate Two-Scale Realized Volatility (TSRV) - per-second.

        Robust to microstructure noise by using high-frequency and low-frequency
        samples to estimate and correct for noise contamination.

        TSRV = sqrt((RV_low - noise_correction) / num_seconds)

        Returns: Per-second volatility (σ)
        """
        if len(self.tsrv_high_freq_returns) < 100 or len(self.rv_5min_periods) < 5:
            return None

        try:
            # High-frequency RV (sum of squared returns over ~20 minutes)
            rv_high = sum(list(self.tsrv_high_freq_returns)[-1000:])  # ~1000 samples

            # Low-frequency RV (sum of 4 five-minute periods = 20 minutes)
            rv_low = sum(list(self.rv_5min_periods)[-4:])

            n_high = min(1000, len(self.tsrv_high_freq_returns))
            n_low = min(4, len(self.rv_5min_periods))

            if n_high < 100 or n_low < 2:
                return None

            # Estimate noise variance
            noise_var = max(0, (rv_high - rv_low) / (n_high - n_low))

            # TSRV with bias correction (sum of squared returns)
            tsrv = max(0, rv_low - 2 * noise_var * n_low)

            # Convert to per-second volatility
            # Window: 4 periods * 300 seconds/period = 1200 seconds (20 minutes)
            num_seconds = 4 * 300
            tsrv_volatility = math.sqrt(tsrv / num_seconds)

            return max(self.vol_min, min(tsrv_volatility, self.vol_max))

        except Exception:
            return None

    def _calculate_har_forecast(self) -> Optional[float]:
        """
        Calculate HAR (Heterogeneous Autoregressive) volatility forecast - per-second.

        HAR-RV: σ²_forecast = β₀ + β₁·RV_daily + β₂·RV_weekly + β₃·RV_monthly

        Captures multi-horizon volatility structure. Daily re-estimation of coefficients
        using 250 days of historical data.

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

            # Calculate HAR components
            rv_hist = list(self.rv_history)

            # Daily component (last 12 periods with 5-min buckets = 1 hour)
            rv_daily = np.mean(rv_hist[-12:]) if len(rv_hist) >= 12 else np.mean(rv_hist)

            # Weekly component (last 144 periods = 12 hours, representing weekly in crypto)
            rv_weekly = np.mean(rv_hist[-144:]) if len(rv_hist) >= 144 else np.mean(rv_hist)

            # Monthly component (last 288 periods = 24 hours, representing monthly in crypto)
            rv_monthly = np.mean(rv_hist[-288:]) if len(rv_hist) >= 288 else np.mean(rv_hist)

            # HAR forecast: weighted average of squared returns at different horizons
            # Each component is already per-second (normalized from rv_5min_periods)
            har_var = (
                self.har_coefficients.get('beta_0', 0) +
                self.har_coefficients.get('beta_d', 0.4) * rv_daily +
                self.har_coefficients.get('beta_w', 0.3) * rv_weekly +
                self.har_coefficients.get('beta_m', 0.3) * rv_monthly
            )

            har_vol = math.sqrt(max(0, har_var))
            return max(self.vol_min, min(har_vol, self.vol_max))

        except Exception:
            return None

    def _estimate_har_coefficients(self, current_time: float) -> None:
        """
        Estimate HAR coefficients using OLS regression on historical RV data.

        Fit: RV_{t+1} = β₀ + β₁·RV_t + β₂·RV^{5d}_t + β₃·RV^{22d}_t + ε_t
        """
        try:
            if len(self.rv_history) < 50:
                return

            # For now, use simple default coefficients
            # In production, would fit OLS with historical data
            self.har_coefficients = {
                'beta_0': 0.0001,
                'beta_d': 0.4,  # Daily weight
                'beta_w': 0.3,  # Weekly weight
                'beta_m': 0.3   # Monthly weight
            }
            self.har_last_reestimate_time = current_time

        except Exception:
            pass

    def _calculate_yang_zhang_volatility(self) -> Optional[float]:
        """
        Calculate Yang-Zhang volatility estimator using OHLC data - per-second.

        YZ = sqrt(σ²_night + k·σ²_open-close + (1-k)·σ²_RS)

        More efficient than close-to-close (14x more efficient for GBM).
        Handles opening jumps better than standard estimators.
        Good for 24/7 crypto markets.

        Returns: Per-second volatility (σ)
        """
        if len(self.ohlc_5min) < 3:
            return None

        try:
            ohlc_data = list(self.ohlc_5min)[-20:]  # Last 20 periods (100 minutes)

            total_yy_var = 0.0
            for ohlc in ohlc_data:
                if ohlc.open <= 0 or ohlc.high <= 0 or ohlc.low <= 0 or ohlc.close <= 0:
                    continue

                # Rogers-Satchell component
                rs = math.log(ohlc.high / ohlc.close) * math.log(ohlc.high / ohlc.open) + \
                     math.log(ohlc.low / ohlc.close) * math.log(ohlc.low / ohlc.open)

                # Garman-Klass components
                hl = math.log(ohlc.high / ohlc.low) ** 2 / (4 * math.log(2))
                co = (2 * math.log(2) - 1) * (math.log(ohlc.close / ohlc.open) ** 2)

                # Yang-Zhang formula (simplified)
                yy_component = hl - co + rs
                total_yy_var += max(0, yy_component)

            avg_yy_var = total_yy_var / len(ohlc_data) if ohlc_data else 0

            # Convert to per-second volatility
            # Each OHLC period is 5 minutes = 300 seconds
            num_seconds = 300
            yy_vol = math.sqrt(max(0, avg_yy_var / num_seconds))

            return max(self.vol_min, min(yy_vol, self.vol_max))

        except Exception:
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
            k_bounded = k != k_new  # Check if k was capped

            # Confidence based on number of recent market trades (more reliable sample)
            recent_market_trades = sum(1 for t, _ in self.market_trades if t > current_time - self.k_window_sec)
            confidence = min(0.95, 0.3 + recent_market_trades / 100.0)  # Increased divisor since we get many trades

            # Log order intensity details
            logger.debug(
                "Order intensity calculation",
                market_trades_in_interval=market_trades_in_interval,
                market_trade_rate=f"{market_trade_rate:.2f} trades/sec",
                k_unbounded=f"{k_new:.2f}",
                k_bounded=f"{k:.2f}",
                was_capped=k_bounded,
                k_min=self.k_min,
                k_max=self.k_max,
                recent_market_trades=recent_market_trades,
                confidence=f"{confidence:.2f}",
                fill_rate_ratio=f"{self.fill_rate_ratio:.3f}"
            )

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