# Phase 2: Data Ingestion & Features - Implementation Summary

**Status**: ✅ COMPLETE

**Date Completed**: October 30, 2024

**Branch**: `feature/phase_2`

---

## Overview

Phase 2 implements the complete data ingestion pipeline and feature calculation layer for the Avellaneda-Stoikov market maker. This phase bridges raw market data from Binance with the mathematical features required for Phase 3 (strategy engine implementation).

## What Was Implemented

### 1. Market Data Gateway (marketdata_gw) - Port 8001

**Service**: `services/marketdata_gw/main.py`

#### Functionality
- **Binance WebSocket Integration**: Dual stream architecture
  - Depth stream: `symbol@depth20@100ms` (order book snapshots, 20 levels, 100ms updates)
  - Trades stream: `symbol@aggTrade` (real-time aggregated trades)

- **Message Publishing**:
  - Publishes `DepthSnapshot` messages to NATS topic `raw.depth.v1`
  - Publishes `TradeMessage` messages to NATS topic `raw.trades.v1`

- **Resilience**:
  - Auto-reconnect with exponential backoff (1s → 30s max)
  - Per-stream error handling and recovery
  - Concurrent stream management

#### Key Classes
```python
BinanceWebSocketManager
├── _stream_depth()          # Handles depth stream with reconnect
├── _stream_trades()         # Handles trades stream with reconnect
├── _process_depth_message() # Parses Binance depth format
└── _process_trade_message() # Parses Binance trade format

MarketDataGateway
├── on_depth()   # Callback to publish depth to NATS
└── on_trade()   # Callback to publish trades to NATS
```

#### Configuration (from config.yaml)
```yaml
binance:
  testnet: true                    # Start with testnet!
  ws_url: wss://stream.binancefuture.com

system:
  symbol: BTCUSDT                  # Default trading pair
```

---

### 2. Features Service (features_svc) - Port 8002

**Service**: `services/features_svc/main.py`

#### Functionality
Calculates essential market microstructure features from order book and trade data:

1. **Order Flow Imbalance (OFI)**
   - Formula: `OFI = sum(bid_volumes) - sum(ask_volumes)`
   - Rolling window: Last 50 ticks
   - Z-score normalization with threshold detection
   - Signal: "BUY" if z-score > 1.5, "SELL" if z-score < -1.5

2. **Micro-Price**
   - Formula: `micro_price = (ask × bid_qty + bid × ask_qty) / (bid_qty + ask_qty)`
   - Volume-weighted mid-price (smoother than simple mid)
   - EMA smoothing with configurable alpha (default: 0.5)

3. **Queue Imbalance**
   - Counts order queue depth at each level
   - Ratio: `bid_queue / ask_queue`
   - Depth: 10 levels (configurable)

4. **Spread Calculation**
   - Basis points: `(ask - bid) / mid_price × 10000`

#### Message Output
Publishes `FeatureMessage` to NATS topic `features.v1` containing:
```python
{
    "symbol": "BTCUSDT",
    "timestamp_ms": 1699564800000,
    "mid_price": 42000.5,
    "micro_price": 42000.3,        # EMA-smoothed
    "spread_bps": 2.4,
    "best_bid": 42000.0,
    "best_ask": 42001.0,
    "best_bid_qty": 1.5,
    "best_ask_qty": 2.0,
    "ofi": {                        # Order Flow Imbalance
        "value": 150.5,
        "z_score": 1.8,
        "direction": "BUY"
    },
    "queue_imbalance": {            # Queue Imbalance
        "bid_queue": 50.0,
        "ask_queue": 30.0,
        "imbalance_ratio": 1.67
    }
}
```

#### Key Classes
```python
FeatureCalculator
├── calculate_features()          # Main entry point
├── _calculate_ofi()              # OFI with rolling window
├── _calculate_micro_price()      # Volume-weighted mid
├── _calculate_queue_imbalance()  # Queue depth ratio
└── _calculate_z_score()          # Normalization

FeaturesService
├── on_depth_message()   # Process depth and publish features
└── on_trade_message()   # Store trade data for OFI
```

#### Configuration
```yaml
strategy:
  features:
    ofi_window_ticks: 50           # 50-tick rolling window
    ofi_z_score_threshold: 1.5     # Signal threshold
    micro_price_alpha: 0.5         # EMA smoothing
    queue_imbalance_depth: 10      # Levels to analyze
```

---

### 3. Volatility & Order Flow Estimator (volflow_estimator) - Port 8003

**Service**: `services/volflow_estimator/main.py`

#### Functionality
Estimates key parameters for the Avellaneda-Stoikov strategy:

1. **Realized Volatility (σ)**
   - Calculation: Standard deviation of log returns
   - Window: 60 seconds (configurable)
   - Annualized: `stdev × sqrt(252 × 86400)` (trading days/year × seconds/day)
   - Bounds: [0.0005, 1.0]
   - Confidence: Increases from 0.1 → 0.95 with sample size

2. **Order Intensity (k parameter)**
   - Simplified approach: `k = (arrival_rate × 0.5) + 0.5`
   - Arrival rate: trades per second in 60-second window
   - Bounds: [0.1, 10.0]
   - Confidence: Based on number of trades observed

3. **VPIN (Volume-Synchronized Probability of Informed Trading)**
   - Formula: `VPIN = |buy_volume - sell_volume| / total_volume`
   - Detects informed/toxic flow
   - Classification:
     - NORMAL: VPIN < 0.4
     - ELEVATED: 0.4 ≤ VPIN < 0.7
     - TOXIC: VPIN ≥ 0.9

#### Message Output
Publishes `VolflowMessage` to NATS topic `volflow.v1` every 1 second:
```python
{
    "symbol": "BTCUSDT",
    "timestamp_ms": 1699564800000,
    "volatility": {
        "value": 0.45,         # Annualized volatility
        "confidence": 0.85     # Confidence in estimate
    },
    "order_intensity": {
        "k": 1.5,              # Poisson parameter
        "confidence": 0.9
    },
    "vpin": {
        "value": 0.65,
        "status": "ELEVATED"   # NORMAL/ELEVATED/TOXIC
    }
}
```

#### Key Classes
```python
VolflowCalculator
├── update_trade()               # Accumulate trade data
├── calculate_volflow()          # Throttled calculation (1s)
├── _calculate_volatility()      # Log-return std dev
├── _calculate_order_intensity() # Trade arrival rate
├── _calculate_vpin()            # Buy/sell imbalance
└── _cleanup_old_data()          # Memory management

VolflowEstimator
├── on_trade()   # Update calculator and publish
└── on_depth()   # Future: depth-based metrics
```

#### Configuration
```yaml
strategy:
  volflow:
    volatility_lookback_sec: 60        # 60-second window
    volatility_min: 0.0005             # Floor
    volatility_max: 1.0                # Ceiling

    order_intensity_window_sec: 60     # Trade counting window
    order_intensity_k_min: 0.1
    order_intensity_k_max: 10.0

    vpin_threshold_normal: 0.4         # NORMAL boundary
    vpin_threshold_elevated: 0.7       # ELEVATED boundary
    vpin_threshold_toxic: 0.9          # TOXIC boundary
```

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Binance Futures API                         │
│              (testnet or live, configurable)                    │
└──────────┬──────────────────────────────┬──────────────────────┘
           │                              │
    [depth@100ms]                   [aggTrade stream]
           │                              │
           └──────────┬───────────────────┘
                      │
         ┌───────────���▼──────────────┐
         │  marketdata_gw (8001)     │
         │  - Parse WebSocket JSON   │
         │  - Validate timestamps    │
         │  - Publish to NATS        │
         └────────────┬──────────────┘
                      │
        ┌─────────────┴─────────────┐
        │                           │
    raw.depth.v1           raw.trades.v1
        │                           │
        │         ┌─────────────────┘
        │         │
        ▼         ▼
   ┌────────────────────────┐
   │  features_svc (8002)   │
   │  - OFI (50 ticks)      │
   │  - Micro-price (EMA)   │
   │  - Queue imbalance     │
   │  - Spread (bps)        │
   └────────────┬───────────┘
                │
           features.v1
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼
    ┌──────────────────────────┐      ┌───────────────┐
    │ volflow_estimator (8003) │ ◄────┤ raw.trades.v1 │
    │ - Volatility (σ)         │      └───────────────┘
    │ - Order intensity (k)    │
    │ - VPIN                   │
    └────────────┬─────────────┘
                 │
            volflow.v1
                 │
                 ▼
    (Ready for Phase 3: as_engine)
```

---

## Service Dependencies & Ports

| Service | Port | Consumes | Produces | Status |
|---------|------|----------|----------|--------|
| marketdata_gw | 8001 | Binance WS | raw.depth.v1, raw.trades.v1 | ✅ Impl |
| features_svc | 8002 | raw.depth.v1, raw.trades.v1 | features.v1 | ✅ Impl |
| volflow_estimator | 8003 | raw.trades.v1, raw.depth.v1 | volflow.v1 | ✅ Impl |
| as_engine | 8004 | features.v1, volflow.v1 | quotes.v1 | ⏳ Phase 3 |
| order_router | 8005 | quotes.v1 | fill.v1, cancelled.v1 | ⏳ Phase 4 |
| inventory_svc | 8006 | fill.v1, cancelled.v1 | inventory.v1 | ⏳ Phase 4 |
| risk_manager | 8007 | inventory.v1, volflow.v1 | alert.v1 | ⏳ Phase 4 |
| metrics_svc | 8008 | All topics | Prometheus metrics | ✅ Phase 1 |
| sim_backtest | 8009 | Historical data | Backtest results | ⏳ Phase 5 |

---

## Running Phase 2

### Prerequisites
```bash
# Start infrastructure (NATS, Redis, TimescaleDB, Prometheus, Grafana)
docker-compose up -d

# Initialize database
python scripts/setup_db.py
```

### Start Individual Services
```bash
# Terminal 1: Market Data Gateway
python -m services.marketdata_gw.main

# Terminal 2: Features Service
python -m services.features_svc.main

# Terminal 3: Volatility & Order Flow Estimator
python -m services.volflow_estimator.main
```

### Or Start All Phase 2 Services
```bash
# Using launcher (future enhancement)
python launcher.py --services marketdata_gw,features_svc,volflow_estimator
```

### Monitor Message Flow
```bash
# Watch NATS topics in real-time
nats sub "raw.depth.v1" &
nats sub "raw.trades.v1" &
nats sub "features.v1" &
nats sub "volflow.v1" &

# Or use Prometheus/Grafana
open http://localhost:3000  # Grafana (admin/admin)
open http://localhost:9090  # Prometheus
```

---

## Testing & Validation

### Unit Tests
```bash
pytest tests/unit -v
```

Tests to add:
- [ ] FeatureCalculator.calculate_features() with mock depth
- [ ] OFI z-score calculation accuracy
- [ ] Micro-price volume-weighted formula
- [ ] Queue imbalance counting
- [ ] VolflowCalculator volatility annualization
- [ ] Order intensity k parameter bounds
- [ ] VPIN classification thresholds

### Integration Tests
```bash
# Requires docker-compose running
pytest tests/integration -v -s
```

Integration tests to add:
- [ ] marketdata_gw WebSocket → NATS pipeline
- [ ] features_svc depth processing → features publication
- [ ] volflow_estimator trade → volflow publication
- [ ] End-to-end: Binance → features → volflow
- [ ] Error recovery: WebSocket disconnect → reconnect
- [ ] Message ordering and latency

### Manual Testing Checklist
- [ ] Start docker-compose services
- [ ] Start Phase 2 services in separate terminals
- [ ] Verify NATS topics receive messages
- [ ] Check message rates match expectations:
  - `raw.depth.v1`: ~10 msgs/sec (100ms updates)
  - `raw.trades.v1`: Variable, depends on market activity
  - `features.v1`: Same as depth (~10 msgs/sec)
  - `volflow.v1`: ~1 msg/sec
- [ ] Inspect message payloads for correctness
- [ ] Verify OFI z-scores converge after warmup
- [ ] Check volatility estimates stabilize
- [ ] Monitor service logs for errors
- [ ] Verify Prometheus metrics are exported

---

## Key Metrics & Expected Behavior

### Message Rates (after warmup)
- **Depth messages**: ~10/sec (100ms Binance updates)
- **Trade messages**: 1-50/sec (market dependent)
- **Feature messages**: ~10/sec (published per depth)
- **Volflow messages**: ~1/sec (1-second throttling)

### Warmup Period
- **OFI z-score**: Needs 50 ticks (~5 seconds at 10/sec)
- **Volatility**: Needs 60 seconds of trades
- **Order intensity**: Needs 60 seconds of trades
- **VPIN**: Needs meaningful trade volume (~1 minute)

### Expected Ranges (BTCUSDT)
- **Spread**: 2-5 bps (tight market)
- **OFI z-score**: ±3 range
- **Volatility**: 0.3-0.8 annualized
- **Order intensity k**: 0.5-3.0 (depends on market)
- **VPIN**: 0.2-0.8 (mostly normal)

---

## Architecture Decisions

### 1. WebSocket Management
- **Why dual streams**: Separation of concerns (depth vs. trades)
- **Why concurrent**: Prevents one stream blocking the other
- **Why exponential backoff**: Gradual recovery from transient errors

### 2. Feature Calculation
- **Why rolling window for OFI**: Captures recent imbalance, responsive to changes
- **Why z-score normalization**: Makes OFI comparable across different market conditions
- **Why EMA for micro-price**: Smooth out tick-by-tick noise
- **Why volume-weighted**: More accurate fair-value estimate

### 3. Volatility Estimation
- **Why log returns**: Statistically rigorous, scale-invariant
- **Why annualized**: Matches Avellaneda-Stoikov paper convention
- **Why bounds**: Prevents numerical issues in strategy calculations

### 4. VPIN Calculation
- **Why volume-synchronized**: True informed trading detection
- **Why buy/sell classification from is_buyer_maker**: Efficient aggressiveness detection
- **Why status classification**: Enables risk management triggers (Phase 4)

---

## Next Steps: Phase 3 (Strategy Engine)

Phase 3 will implement the Avellaneda-Stoikov quote generation engine:

```python
# Pseudocode for Phase 3
class AsEngine:
    async def on_features(self, features: FeatureMessage):
        async def on_volflow(self, volflow: VolflowMessage):
            # 1. Get inventory from inventory_svc
            # 2. Calculate reservation price:
            #    r(t) = s(t) - q·γ·σ²·(T-t)
            # 3. Calculate optimal spread:
            #    δ* = (1/γ) · ln(1 + γ/k)
            # 4. Apply OFI skew adjustment
            # 5. Publish quotes.v1
```

Phase 3 deliverables:
- Quote generation based on AS model
- Inventory-adjusted pricing
- OFI-based skew adjustment
- Position tracking
- Quote refresh loop

---

## Known Limitations & Future Enhancements

### Current Limitations
1. **Volatility**: Simple log-return std dev (could use EWMA or Parkinson)
2. **Order Intensity**: Trade frequency-based (could use order book density)
3. **VPIN**: No bucketing (simpler full-window calculation)
4. **Micro-price**: Only uses top-of-book (could aggregate levels)

### Future Enhancements
1. **Multi-symbol support**: Currently hardcoded to BTCUSDT
2. **Historical backfill**: Pre-populate features from historical data
3. **Feature caching**: Redis caching for rapid retrieval
4. **Adaptive parameters**: Learn optimal feature windows from backtest data
5. **Advanced volatility**: Parkinson, Garman-Klass, Yang-Zhang
6. **Machine learning**: Predict volatility/intensity with neural nets

---

## Troubleshooting

### WebSocket Connection Errors
```
Error: "Trades stream error, reconnecting"
→ Check Binance status: https://status.binance.com
→ Verify network connectivity
→ Check testnet URL in config.yaml
```

### NATS Connection Issues
```
Error: "RuntimeError: Not connected to NATS"
→ Verify docker-compose is running: docker-compose ps
→ Check NATS health: docker logs mm-nats
→ Verify NATS_URL in config.yaml
```

### Feature Calculation Errors
```
Warning: "Failed to calculate features for depth message"
→ Check for invalid price data (bid=0 or ask=0)
→ Verify depth.bids and depth.asks are populated
→ Check timestamps are valid (not zero)
```

### Volatility Not Converging
```
VolatilityData(value=0.0005, confidence=0.1)
→ Needs 60 seconds of trade data
→ Check raw.trades.v1 topic is receiving messages
→ Verify trade prices are within reasonable range
```

---

## Code Statistics

- **marketdata_gw**: ~190 lines (Binance WebSocket + NATS publishing)
- **features_svc**: ~230 lines (OFI + micro-price + queue imbalance)
- **volflow_estimator**: ~260 lines (volatility + order intensity + VPIN)
- **Total Phase 2**: ~680 lines of implementation
- **Interfaces**: 11 Pydantic message types (defined in shared/schemas.py)
- **NATS Topics**: 2 raw (input) + 2 calculated (output)

---

## Commit Information

**Commit Hash**: 88782c9

**Message**: Implement Phase 2: Data Ingestion & Features

**Files Modified**:
- `services/marketdata_gw/main.py` (+180 -10)
- `services/features_svc/main.py` (+200 -50)
- `services/volflow_estimator/main.py` (+270 -60)

---

## References

- **Paper**: Avellaneda & Stoikov (2008) - "High Frequency Trading in a Limit Order Book"
- **Binance API**: https://binance-docs.github.io/apidocs/futures/
- **NATS**: https://nats.io/
- **Pydantic**: https://docs.pydantic.dev/

---

**Status**: ✅ Phase 2 Complete - Ready for Phase 3 (Strategy Engine)

**Next Milestone**: Phase 3 - Avellaneda-Stoikov Quote Generation Engine
