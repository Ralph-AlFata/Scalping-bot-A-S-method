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

class VolflowCalculator:
    
    def __init__(self, config):
        """Initialize volatility calculator"""
        self.config = config
        self.symbol = config.system.symbol 

        # EWMA Configuration
        # self.decay_lambda = getattr(config.strategy.volflow, 'ewma_lambda', 0.88)
        self.ewma_halflife = getattr(config.strategy.volflow, 'ewma_halflife', 0.88)
        self.max_history_seconds = getattr(config.strategy.volflow, 'max_history_seconds', 300)

        # Calculate decay constant for EWMA
        # λ = ln(2) / halflife
        self.decay_constant = np.log(2) / self.ewma_haflife

        # Binance socket updates at a rat of 100ms, therefore there are 10 updates per second
        # Storage: (timestamps, mid_price)
        # Keep enough history for the EWMA calculation
        max_samples = int(self.max_history_seconds * 10)
        self.price_history: Deque[Tuple[float, float]] = deque(maxlen=max_samples)

        # Cached values for efficiency
        self._last_sigma_sq_log = None 
        self._last_sigma_sq_absolute = None 
        self._last_calculation_time = 0
        self._cache_duration = 0.1 # Cache for 100 ms to avoid recalculation

        # ----- Order-intensity configuration ----
        self.deltas_spreads = getattr(config.strategy.volflow, "deltas", [0.0, 0.1, 0.25, 0.5, 1.0])
        self.horizon_s = getattr(config.strategy.volflow, "horizon_s", 1.0)
        self.warmup_minutes = getattr(config.strategy.volflow, "warmup_minutes", 5)
        self.warmup_end_time = time.time() + self.warmup_minutes * 60
        self.min_exposure = 60.0 # Check again what is this min_exposure
        self.min_fills = 3
        self.k = None 
        self.A = None 
        self._lambda_mode = "market_proxy" # Will later switch to "own_fills"

        # buckets for exposure/fills and markets crossings
        self.market_counts = {
            "bid": {d: 0 for d in self.deltas_spreads},
            "ask": {d: 0 for d in self.deltas_spreads}
        }
        self.market_expos = {
            "bid": {d: 0 for d in self.deltas_spreads},
            "ask": {d: 0 for d in self.deltas_spreads}
        }
        self.fill_buckets = {d:{"E": 0.0, "N": 0} for d in self.deltas_spreads}


                
    def update_prices(self, depth:DepthSnapshot) -> None:
        """
        Add new mid price observation.
        
        Args:
            timestamp_ms: Timestamp in milliseconds
            mid_price: Current mid price in $/BTC
        """
        mid_price = (depth.bids[0][0] + depth.asks[0][0]) / 2
        timestamp = depth.exchange_timestamp_ms
        self.price_history.append((timestamp, mid_price))
        
        # Invalidate cache when new data arrives
        self._last_calculation_time = 0

    # This is the way it's calculated and assumed in the paper
    def _calculate_variance_absolute(self):
        """
        Compute realized volatility over the current queue of mid prices.
        
        Uses squared returns normalized by time differences:
            σ² = (1 / T) * Σ[(Δp)² / Δt]
        where:
            Δp = price difference between consecutive samples
            Δt = time difference between consecutive timestamps
            T  = total elapsed time (last_timestamp - first_timestamp)
        """

        """Later, the more efficient way is to have the incoming ones directly subtracted and added as differences, instead of recomputing everytime."""

        if len(self.price_history) < 2:
            return None
        
        # Calculate the incremental differences
        arr = np.array(self.price_history, dtype=float)
        deltas = np.diff(arr, axis=0)

        delta_t = deltas[:, 0] / 1000
        delta_p = deltas[:, 1]

        # Guard against zero time differences (can happen in high-frequency data)
        nonzero_mask = delta_t > 0
        if not np.any(nonzero_mask):
            return None
        
        delta_t = delta_t[nonzero_mask]
        delta_p = delta_p[nonzero_mask]

        # Compute time-normalized squared returns
        normalized = np.square(delta_p)

        # Total time interval
        T = (arr[-1][0] - arr[0][0]) / 1000
        if T <= 0:
            return None 
        
        # Realized volatility estimate (per-second variance)
        volatility = np.sum(normalized) / T

        return volatility

    
    def _calculate_volatility_log_returns(self):
        """
        Realized volatility using LOG RETURNS with time normalization.
        
        Formula:
            σ² = (1/T) * Σ[(ln(p_i/p_{i-1}))² / Δt_i]
            σ = sqrt(σ²)
        
        Returns:
            Per-second volatility (standard deviation), or None if insufficient data
        """
        if len(self.price_history) < 2:
            return None
        
        arr = np.array(self.price_history, dtype=float)

        delta_t = np.diff(arr[:, 0]) / 1000

        prices = arr[:, 1]
        log_returns = np.diff(np.log(prices))

        valid_mask = delta_t > 0
        if not np.any(valid_mask):
            return None 
        
        delta_t = delta_t[valid_mask]
        log_returns = log_returns[valid_mask]

        # Time-normalized squre returns
        normalized_squared_returns = np.square(log_returns)

        # Total time interval (seconds)
        T = (arr[-1, 0] - arr[0, 0]) / 1000
        
        if T <= 0:
            return None 
        
        variance = np.sum(normalized_squared_returns) / T

        volatility = np.sqrt(variance)

        return volatility
    

    def _calculate_log_return_variance_ewma(self) -> Optional[float]:
        """
        Calculate variance of LOG RETURNS per second using EWMA.
        
        Process:
        1. Compute log returns between consecutive prices
        2. Normalize by time differences
        3. Apply exponential weights (more weight to recent)
        4. Return weighted variance
        
        Returns:
            σ²_log [dimensionless per second] or None if insufficient data
        """
        if len(self.price_history) < 2:
            return None 
        
        # Convert to numpy array for vectorized operations
        arr = np.array(self.price_history, dtype=float)

        # Extract timestamps in seconds and prices
        timestamps = arr[:,0] / 1000.0 # Convert to seconds
        prices = arr[:,1]

        # Calculate time differences between consecutive observations
        delta_t = np.diff(timestamps) # [seconds]

        # Calculate log returns [dimensionless]
        # r_i = log(p_i/p_i-1)
        log_returns = np.diff(np.log(prices))

        # Filter out invalid data (zero or negative time intervals)
        valid_mask = delta_t > 0
        if not np.any(valid_mask):
            return None 
        
        delta_t = delta_t[valid_mask]
        log_returns = log_returns[valid_mask]
        timestamps_mid = timestamps[1:][valid_mask] # Timestamp of each return 

        if len(log_returns) < 2:
            return None 
        
        # Time-normalize the squared returns
        # This gives us (dimensionless)² / second
        normalized_squared_returns = np.square(log_returns) / delta_t 

        # ============================================
        # EWMA Weighting
        # ============================================
        # Weight: w_i = exp(-λ × (T - t_i))
        # where T is the most recent time, t_i is time of observation i

        current_time = timestamps_mid[-1]
        time_from_end = current_time - timestamps_mid

        # Calculate exponential weights
        weights = np.exp(-self.decay_constant * time_from_end)

        # Normalize weights to sum to 1
        weights = weights / np.sum(weights)

        # Calculate weighted variance
        # σ²_log = Σ(w_i × r_i²/Δt_i)
        variance_log_per_second = np.sum(weights * normalized_squared_returns)

        return variance_log_per_second

    def _get_variance_for_AS_model(self) -> Optional[float]:    
        """
        Get variance σ² in absolute units for use in A-S formulas.
        
        This is the main function in the strategy.
        
        Conversion:
            σ²_absolute = σ²_log × S²
        
        Where:
            σ²_log: dimensionless variance per second (from log returns)
            S²: current price squared ($/BTC)²
            σ²_absolute: absolute variance ($/BTC)² per second
        
        Args:
            current_mid_price: Current mid price S in $/BTC
            
        Returns:
            σ²_absolute in ($/BTC)²/second for direct use in:
                r(s,q,t) = s - q·γ·σ²·(T-t)
        """
        # Check cache
        current_time = time.time()
        if (current_time - self._last_calculation_time) < self._cache_duration:
            if self._last_sigma_sq_absolute is not None:
                return self._last_sigma_sq_absolute
        
        # Calculate log return variance (dimensionless per second)
        sigma_sq_log = self._calculate_log_return_variance_ewma()

        if sigma_sq_log is None:
            return None

        # Units: ($/BTC)²/second = (1/second) × ($/BTC)²
        last_mid_price = self.price_history[-1][1]
        sigma_sq_absolute = sigma_sq_log * (last_mid_price ** 2)

        # Cache the result
        self._last_sigma_sq_log = sigma_sq_log
        self._last_sigma_sq_absolute = sigma_sq_absolute
        self._last_calculation_time = current_time

        return sigma_sq_absolute

class VolflowEstimator:
    """Volatility and order flow estimator service."""

    def __init__(self, config):
        """Initialize service"""
        self.config = config 
        self.nats = NATSClient(config.infrastructure.nats.url)
        self.running = False 
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

            # Subscribe to required data streams per verification
            await self.nats.subscribe("raw.depth.v1", self.on_depth)

            self._running = True 
            logger.info("VolflowEstimator ready")
        
        except Exception as e:
            logger.error("Failed to start VolflowEstimator", error=str(e))
            raise
    
    async def on_depth(self, msg) -> None:
        """Handle depth messages."""
        try:
            self._measurements += 1

            # Parse message
            data = json.loads(msg.data.decode())
            depth = DepthSnapshot(**data)

            # Update calculator
            self.calculator.update_prices(depth)
        
        except json.JSONDecodeError as e:
            logger.error("Failed to decode depth message", error=str(e))
            self._errors += 1
        except Exception as e:
            logger.error("Error processing depth message", error=str(e))
            self._errors += 1
    
    async def stop(self) -> None:
        """Stop service"""
        logger.info("Stopping VolflowEstimator")
        self._running = False 
        await self.nats.close()
        
        await self.nats.close()
        logger.info(
            "VolflowEstimator stopped",
            measurements=self._measurements,
            estimates_published=self._estimates_published,
            errors=self.errors,
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
    """Main entry point"""
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

