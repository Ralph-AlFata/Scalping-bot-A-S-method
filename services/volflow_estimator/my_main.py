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