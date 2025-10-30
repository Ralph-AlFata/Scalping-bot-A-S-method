# Phase 2: Quick Start Guide

## One-Minute Overview

Phase 2 implements the data ingestion pipeline that streams market data from Binance and calculates essential trading features.

**Data Flow**:
```
Binance → marketdata_gw → features_svc & volflow_estimator → Ready for Phase 3
```

---

## Prerequisites

```bash
# Check you have these installed
python --version        # 3.11+
docker --version        # v20+
docker-compose --version
uv --version            # Python package manager

# Install dependencies
cd /path/to/Scalping-bot-A-S-method
uv sync
```

---

## Step 1: Start Infrastructure (One-Time)

```bash
# Terminal 1: Start all Docker containers
docker-compose up -d

# Wait 10 seconds for services to be healthy
sleep 10

# Verify all services are running
docker-compose ps

# You should see all 5 containers as "healthy":
# - mm-nats (NATS broker)
# - mm-redis (Cache)
# - mm-timescaledb (Database)
# - mm-prometheus (Metrics)
# - mm-grafana (Dashboard)
```

Check health:
```bash
# Test NATS
nats -s nats://localhost:4222 pub health "test" 2>&1 | grep -q "Msg sent"

# Test Redis
redis-cli ping

# Test TimescaleDB
psql -h localhost -U postgres -d marketmaker -c "SELECT 1"
```

---

## Step 2: Initialize Database (One-Time)

```bash
python scripts/setup_db.py
```

Output should show:
```
✓ Connected to marketmaker@localhost:5432
✓ TimescaleDB extension enabled
✓ Created ticks table
✓ Created ticks hypertable
✓ Created trades table
✓ Created trades hypertable
...
✅ Database setup complete!
```

---

## Step 3: Run Phase 2 Services

**Option A: Three Separate Terminals (Recommended for Development)**

```bash
# Terminal 2: Market Data Gateway
python -m services.marketdata_gw.main

# Expected output:
# 2024-10-30 10:00:00 - marketdata_gw - INFO - Initializing MarketDataGateway
# 2024-10-30 10:00:00 - marketdata_gw - INFO - Starting MarketDataGateway service
# 2024-10-30 10:00:01 - marketdata_gw - INFO - Connecting to depth stream
# 2024-10-30 10:00:01 - marketdata_gw - INFO - Connecting to trades stream
```

```bash
# Terminal 3: Features Service
python -m services.features_svc.main

# Expected output:
# 2024-10-30 10:00:00 - features_svc - INFO - Initializing FeaturesService
# 2024-10-30 10:00:00 - features_svc - INFO - Starting FeaturesService
# 2024-10-30 10:00:00 - features_svc - INFO - FeaturesService ready
```

```bash
# Terminal 4: Volatility & Order Flow Estimator
python -m services.volflow_estimator.main

# Expected output:
# 2024-10-30 10:00:00 - volflow_estimator - INFO - Initializing VolflowEstimator
# 2024-10-30 10:00:00 - volflow_estimator - INFO - Starting VolflowEstimator
# 2024-10-30 10:00:00 - volflow_estimator - INFO - VolflowEstimator ready
```

**Option B: Background Processes**

```bash
python -m services.marketdata_gw.main > /tmp/marketdata_gw.log 2>&1 &
python -m services.features_svc.main > /tmp/features_svc.log 2>&1 &
python -m services.volflow_estimator.main > /tmp/volflow_estimator.log 2>&1 &

# Tail logs
tail -f /tmp/marketdata_gw.log
tail -f /tmp/features_svc.log
tail -f /tmp/volflow_estimator.log
```

---

## Step 4: Monitor Message Flow

### Option A: NATS CLI

```bash
# Install NATS CLI (one-time)
go install github.com/nats-io/natscli/cmd/nats@latest

# Watch raw market data
nats sub "raw.depth.v1" &
nats sub "raw.trades.v1" &

# Watch calculated features
nats sub "features.v1" &
nats sub "volflow.v1" &
```

### Option B: Grafana Dashboard

```bash
# Open in browser
open http://localhost:3000

# Login: admin / admin
# View: Dashboards → Select any available dashboard
# Monitor: Message rate, latency, service health
```

### Option C: Prometheus Metrics

```bash
# Open in browser
open http://localhost:9090

# Example queries:
# - Message rate: rate(messages_received_total[1m])
# - Latency: histogram_quantile(0.99, message_latency_ms)
# - Service health: up{job="marketdata_gw"}
```

---

## Step 5: Verify Data Flow

### Check Message Counts

```bash
# Terminal 5: Monitor message activity
while true; do
  echo "=== $(date) ==="
  nats context info 2>/dev/null | grep -A5 "Server URL"
  sleep 5
done
```

### Expected Rates (after 30 seconds warmup)

- **raw.depth.v1**: ~10 messages/second (Binance 100ms updates)
- **raw.trades.v1**: 1-50 messages/second (depends on market)
- **features.v1**: ~10 messages/second (synchronized with depth)
- **volflow.v1**: ~1 message/second (published every 1 second)

### Sample Message Inspection

```bash
# Capture one depth message
nats sub "features.v1" --max-msgs=1 2>/dev/null | jq .

# Expected JSON structure:
{
  "symbol": "BTCUSDT",
  "timestamp_ms": 1699564800000,
  "mid_price": 42000.5,
  "micro_price": 42000.3,
  "spread_bps": 2.4,
  "best_bid": 42000.0,
  "best_ask": 42001.0,
  "best_bid_qty": 1.5,
  "best_ask_qty": 2.0,
  "ofi": {
    "value": 150.5,
    "z_score": 1.8,
    "direction": "BUY"
  },
  "queue_imbalance": {
    "bid_queue": 20.0,
    "ask_queue": 20.0,
    "imbalance_ratio": 1.0
  }
}
```

---

## Step 6: Verify Feature Quality

### OFI Convergence

After ~5 seconds (50 ticks), OFI z-score should start varying:

```python
# Example progression:
# t=0s:   z_score=0.0 (insufficient data)
# t=2s:   z_score=0.5 (20 ticks)
# t=5s:   z_score=±1.5 (50 ticks, converged)
# t=60s:  z_score=±2.0 (rolling window active)
```

### Volatility Convergence

Volatility needs 60 seconds to converge:

```python
# Expected progression:
# t=0-10s:   confidence=0.1, vol=0.0005 (min default)
# t=30s:     confidence=0.3, vol=0.3-0.5
# t=60s:     confidence=0.95, vol stable
```

### Typical Values (BTCUSDT Normal Market)

| Metric | Low | Normal | High |
|--------|-----|--------|------|
| Spread | 1 bps | 2-5 bps | 10+ bps |
| OFI z-score | ±0.5 | ±1.5 | ±3.0 |
| Volatility | 0.2 | 0.45 | 0.8+ |
| Order Intensity k | 0.5 | 1.5 | 3.0+ |
| VPIN | 0.2 | 0.5 | 0.8+ |

---

## Troubleshooting

### Services won't start

```bash
# Check NATS is healthy
docker-compose logs mm-nats | tail -20

# Check Redis is accessible
redis-cli ping

# Check config.yaml exists and is valid
cat config.yaml | grep -A5 "infrastructure:"

# Check imports are correct
python -c "from shared.nats_client import NATSClient; print('OK')"
```

### No messages appearing in NATS

```bash
# Check marketdata_gw is connected to Binance
tail -f /tmp/marketdata_gw.log | grep -i "connecting"

# Check Binance status
curl -s https://status.binance.com | jq .

# Test WebSocket manually
python -c "
import asyncio
import websockets
async def test():
    async with websockets.connect('wss://stream.binancefuture.com/ws/btcusdt@depth20@100ms') as ws:
        print('Connected!')
        msg = await ws.recv()
        print(f'Received: {len(msg)} bytes')
asyncio.run(test())
"
```

### High latency or missing messages

```bash
# Check service resource usage
docker stats

# Check NATS buffer
docker-compose logs mm-nats | grep -i "buffer"

# Reduce log level to INFO (less overhead)
# Edit config.yaml: log_level: INFO
```

### Wrong data in features

```bash
# Check depth message format
nats sub "raw.depth.v1" --max-msgs=1 2>/dev/null | jq .

# Check features_svc is processing depth
tail -f /tmp/features_svc.log | grep -i "published\|error"

# Validate OFI calculation manually
# OFI should be sum(bid_qty) - sum(ask_qty)
# Should be positive when buying pressure, negative when selling
```

---

## Next Steps

After Phase 2 is running:

1. **Monitor for 2-5 minutes** to ensure stability
2. **Check Prometheus/Grafana** for metrics
3. **Review log files** for any warnings
4. **Verify volatility converges** after 60 seconds
5. **Prepare for Phase 3**: Avellaneda-Stoikov quote generation engine

Phase 3 will consume `features.v1` and `volflow.v1` messages to generate optimal bid/ask quotes.

---

## Stop Services

```bash
# Graceful shutdown (Ctrl+C in each terminal)
# Services will log "Stopping [ServiceName]"

# Or kill all background processes
killall python

# Stop Docker containers
docker-compose down

# Optional: Remove volumes (clean slate)
docker-compose down -v
```

---

## Docker Troubleshooting

```bash
# Restart all containers
docker-compose restart

# Check container logs
docker-compose logs mm-nats
docker-compose logs mm-redis
docker-compose logs mm-timescaledb

# Rebuild containers (if needed)
docker-compose down
docker-compose up -d --build

# Full reset
docker-compose down -v
docker system prune -a
docker-compose up -d
python scripts/setup_db.py
```

---

## FAQ

**Q: Should I use testnet or live?**
A: Always start with testnet (BINANCE_TESTNET=true in .env). Use live only after successful backtesting.

**Q: Can I run multiple symbols?**
A: Phase 2 supports one symbol at a time (set in config.yaml: system.symbol). Multi-symbol support coming in Phase 3.

**Q: What if market is closed?**
A: Crypto markets are 24/7. If you get zero messages, check Binance status and network connectivity.

**Q: How do I know if volatility is correct?**
A: Compare with realized volatility from external sources (Yahoo Finance, etc.). Usually 0.3-0.8 for crypto.

**Q: Is the data being persisted?**
A: Raw data is persisted to TimescaleDB (ticks and trades tables). Set retention in config.yaml.

---

## Support

For issues or questions:

1. Check **[PHASE_2_SUMMARY.md](PHASE_2_SUMMARY.md)** for detailed documentation
2. Review **logs** in terminal windows
3. Check **[README.md](README.md)** for architecture overview
4. Inspect **[config.yaml](config.yaml)** for all parameters

---

**Ready to run Phase 2?** Start with Step 1 above! 🚀