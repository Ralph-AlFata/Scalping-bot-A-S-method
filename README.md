# Avellaneda-Stoikov Market Maker for Binance USDT-M Futures

A production-grade quantitative trading system implementing the Avellaneda-Stoikov high-frequency market-making strategy on Binance futures with a modern async microservices architecture.

## Overview

This system builds an optimal market-making bot based on the research paper:
> **High Frequency Trading in a Limit Order Book** - Marco Avellaneda & Sasha F. Stoikov (2008)

The strategy addresses two key sources of dealer risk:
1. **Inventory Risk**: Uncertainty in the asset's fair value
2. **Order Flow Risk**: Market impact and adverse selection

The bot dynamically adjusts bid/ask quotes based on:
- Real-time order flow imbalance (OFI)
- Volatility estimation
- Current inventory position
- Market microstructure

## Project Status

**Phase 1**: Foundation & Infrastructure ✅
- [x] Docker microservices architecture
- [x] NATS message broker
- [x] Redis cache layer
- [x] TimescaleDB time-series database
- [x] Prometheus monitoring
- [x] Structured logging
- [x] 9 service scaffolds

**Phase 2**: Data Ingestion & Features ✅
- [x] Binance WebSocket integration
- [x] OFI calculation (50-tick rolling window, z-score normalized)
- [x] Micro-price computation (volume-weighted with EMA)
- [x] Queue imbalance estimation (bid/ask queue ratio)
- [x] 3 complete data ingestion services

**Phase 3 (Current)**: Avellaneda-Stoikov Strategy ✅
- [x] Reservation price calculation (inventory-adjusted fair value)
- [x] Optimal spread formula (based on order intensity)
- [x] OFI-based skew adjustment (directional signals)
- [x] Quote generation and publishing
- [x] 27 passing unit tests
- [x] Complete documentation

**Phase 4**: Trading & Risk Management (Next)
- [ ] Order placement via Binance REST API
- [ ] Position tracking and P&L
- [ ] Stop-loss and inventory limits
- [ ] Funding rate guards
- [ ] Risk monitoring and alerts

**Phase 5**: Advanced Features & Backtesting
- [ ] Backtesting engine with historical data
- [ ] Parameter optimization
- [ ] Real market testing (testnet)
- [ ] Live deployment (with safeguards)

## System Architecture

### High-Level Data Flow

```
Binance WebSocket
        ↓
marketdata_gw (raw market data ingestion)
        ↓
    NATS Topics (pub/sub messaging)
        ├→ features_svc (OFI, micro-price)
        ├→ volflow_estimator (volatility, order intensity)
        └→ redis cache & timescale archive
        ↓
as_engine (quote generation)
        ↓
order_router (place/cancel orders)
        ↓
inventory_svc (P&L tracking)
        ↓
risk_manager (stop-loss, limits)
        ↓
metrics_svc (Prometheus export)
        ↓
Prometheus → Grafana (monitoring)
```

### Microservices (9 Total)

| Service | Port | Role | Phase |
|---------|------|------|-------|
| `marketdata_gw` | 8001 | Binance WebSocket ingestion | 2 |
| `features_svc` | 8002 | OFI, micro-price, queue imbalance | 2 |
| `volflow_estimator` | 8003 | Volatility (σ), order intensity (k) | 3 |
| `as_engine` | 8004 | Quote generation (core strategy) | 3 |
| `order_router` | 8005 | Order placement & cancellation | 4 |
| `inventory_svc` | 8006 | Position tracking & P&L | 4 |
| `risk_manager` | 8007 | Stop-loss, inventory limits | 4 |
| `metrics_svc` | 8008 | Prometheus metrics export | 1 |
| `sim_backtest` | 8009 | Historical simulation | 5 |

### Infrastructure

| Component | Purpose | Port |
|-----------|---------|------|
| **NATS** | Message broker with JetStream | 4222 |
| **Redis** | Cache layer (hot data) | 6379 |
| **TimescaleDB** | Time-series database | 5432 |
| **Prometheus** | Metrics collection | 9090 |
| **Grafana** | Visualization dashboard | 3000 |

## Installation & Setup

### Prerequisites

- **Docker & Docker Compose** (v1.29+)
- **Python** 3.11+
- **uv** (Python package manager)
- **Binance Account** (API key + secret for testnet or live)

### 1. Clone Repository

```bash
git clone https://github.com/yourusername/market-maker-as.git
cd market-maker-as
```

### 2. Install Python Dependencies

Using `uv` (recommended):

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
pip install -e .[dev]
```

### 3. Configure Environment

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` with your Binance API credentials:

```bash
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here
BINANCE_TESTNET=true  # Start with testnet!
```

### 4. Start Infrastructure

Launch all Docker containers:

```bash
docker-compose up -d
```

Verify all services are healthy:

```bash
docker-compose ps
```

You should see all 5 services as "healthy":
- `mm-nats` (NATS broker)
- `mm-redis` (Cache)
- `mm-timescaledb` (Database)
- `mm-prometheus` (Metrics)
- `mm-grafana` (Visualization)

### 5. Initialize Database

Create TimescaleDB schema and hypertables:

```bash
python scripts/setup_db.py \
  --host localhost \
  --port 5432 \
  --user postgres \
  --password marketmaker_dev \
  --db marketmaker
```

You should see:
```
✓ Connected to marketmaker@localhost:5432
✓ TimescaleDB extension enabled
✓ Created ticks table
✓ Created ticks hypertable
...
✅ Database setup complete!
```

## Running Services

### Development Mode (Single Terminal with Logs)

```bash
# Terminal 1: Run all services in parallel with logs
python -m services.marketdata_gw.main &
python -m services.features_svc.main &
python -m services.volflow_estimator.main &
python -m services.as_engine.main &
python -m services.order_router.main &
python -m services.inventory_svc.main &
python -m services.risk_manager.main &
python -m services.metrics_svc.main &
python -m services.sim_backtest.main &
```

Or with `tmux`/`screen` for better organization.

### Production Mode (Systemd, Docker, K8s)

Phase 2+ will include containerized service definitions.

## Configuration

### Main Configuration File

Edit `config.yaml` for strategy parameters:

```yaml
strategy:
  avellaneda_stoikov:
    gamma: 0.10              # Risk aversion (↑ = risk averse)
    time_horizon: 5.0        # T-t remaining (seconds)
    alpha_skew: 0.40         # OFI signal strength (0-1)

risk:
  max_inventory_btc: 0.02    # Max position size
  stop_loss_pct: -0.01       # -1% stop loss
```

### Environment Variables

Override YAML config with environment variables using `${VAR_NAME}` syntax:

```bash
# In .env
GAMMA=0.15
MAX_INVENTORY_BTC=0.05
DB_PASSWORD=my_secure_password
```

These override the corresponding `config.yaml` values.

## Message Topics (NATS)

Services communicate via NATS pub/sub topics following this schema:

### Market Data (Ingestion)

- **`raw.depth.v1`** - Limit order book snapshot
- **`raw.trades.v1`** - Aggregate trade stream

### Features (Calculated)

- **`features.v1`** - OFI, micro-price, queue imbalance
- **`volflow.v1`** - Volatility, order intensity, VPIN

### Strategy (Quotes & Execution)

- **`quotes.v1`** - Optimal bid/ask from AS engine
- **`fill.v1`** - Order fill notification
- **`cancelled.v1`** - Order cancellation

### Position & Monitoring

- **`inventory.v1`** - Position, P&L, inventory utilization
- **`alert.v1`** - Risk alerts (stop-loss, limits exceeded)
- **`health.v1`** - Service health checks
- **`metrics.v1`** - Performance metrics

### Message Formats

See `shared/schemas.py` for Pydantic model definitions. Example:

```python
from shared.schemas import QuoteMessage

quote = QuoteMessage(
    symbol="BTCUSDT",
    timestamp_ms=1699564800000,
    bid={"price": 41999.5, "quantity": 0.5},
    ask={"price": 42000.5, "quantity": 0.5},
    reservation_price=42000.0,
    mid_price=42000.0,
    spread_bps=2.4,
    inventory=0.1,
    gamma=0.1,
    volatility=0.45,
)
```

## Monitoring & Dashboards

### Grafana

Open http://localhost:3000 (default: admin/admin)

Current dashboards (Phase 2+):
- Quote Performance
- Position & P&L
- Risk Metrics
- Infrastructure Health

### Prometheus

Query historical metrics at http://localhost:9090

Example queries:
```promql
# Messages per second from each service
rate(messages_received_total[1m])

# Order execution latency (p99)
histogram_quantile(0.99, order_latency_ms)

# Current position
position_quantity{symbol="BTCUSDT"}

# P&L over time
pnl_usd{symbol="BTCUSDT", type="total"}
```

## Testing

### Unit Tests

Fast tests for schemas, utilities:

```bash
pytest tests/unit -v
```

### Integration Tests

Full stack tests (requires docker-compose running):

```bash
pytest tests/integration -v -s
```

### All Tests with Coverage

```bash
pytest --cov=shared --cov=services --cov-report=html
```

Coverage reports in `htmlcov/index.html`

## Project Structure

```
market-maker-as/
├── docker-compose.yml          # Infrastructure definition
├── pyproject.toml              # Python dependencies & config
├── config.yaml                 # Strategy parameters
├── .env.example                # Environment template
├── .gitignore                  # Git ignore rules
│
├── shared/                     # Shared utilities
│   ├── schemas.py              # All Pydantic message types
│   ├── config.py               # Config loader with env override
│   ├── logger.py               # Structured logging
│   ├── nats_client.py          # Async NATS wrapper
│   ├── redis_client.py         # Async Redis wrapper
│   └── db_client.py            # Async TimescaleDB wrapper
│
├── services/                   # 9 Microservices
│   ├── marketdata_gw/main.py
│   ├── features_svc/main.py
│   ├── volflow_estimator/main.py
│   ├── as_engine/main.py
│   ├── order_router/main.py
│   ├── inventory_svc/main.py
│   ├── risk_manager/main.py
│   ├── metrics_svc/main.py
│   └── sim_backtest/main.py
│
├── monitoring/
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/
│
├── scripts/
│   └── setup_db.py             # Database initialization
│
├── tests/
│   ├── conftest.py             # Pytest fixtures
│   ├── unit/test_schemas.py
│   └── integration/test_infrastructure.py
│
└── README.md
```

## The Avellaneda-Stoikov Model

### Core Equations

**Reservation Price** (fair value adjusted for inventory):
```
r(t) = s(t) - q·γ·σ²·(T-t)
```

Where:
- `s(t)` = mid-market price
- `q` = current inventory (shares held)
- `γ` = risk aversion coefficient
- `σ` = volatility
- `T-t` = remaining time

**Optimal Spread** (distance from reservation price to quotes):
```
δ* = (1/γ) · ln(1 + γ/k) ≈ 1/k   (for small γ)
```

Where:
- `k` = order arrival intensity (Poisson parameter)
- Larger spread for lower order intensity
- Tighter spread in high-frequency markets

**Bid and Ask Quotes**:
```
p_bid = r(t) - δ*
p_ask = r(t) + δ*
```

### Key Insights

1. **Inventory Adjustment**: Negative inventory → bid above mid; positive → ask below mid
2. **Time Urgency**: As T approaches, γ·σ²·(T-t) → 0, quotes converge to mid
3. **Risk Aversion**: Higher γ → wider spreads (more conservative)
4. **Liquidity**: Higher order intensity (k) → tighter spreads (compete more)

## Mathematical Background

The system estimates:

1. **Volatility (σ)**
   - Realized volatility from trade microstructure
   - Historical lookback window (default: 60s)

2. **Order Intensity (k)**
   - Exponential decay: λ(δ) = A·exp(-k·δ)
   - From order book density and price impact
   - Estimated via econophysics results

3. **Order Flow Imbalance (OFI)**
   - V_bid(t) - V_ask(t) from order book
   - Signals adverse selection risk
   - Used for skew adjustment

## Development Guidelines

### Code Style

- **Format**: Black (100-char line length)
- **Linting**: Flake8, Pylint
- **Type Hints**: Full mypy compliance
- **Async**: All I/O with asyncio

### Adding a New Service

1. Create `services/my_service/` directory
2. Implement `main.py` following service template
3. Add NATS subscriptions and publishes
4. Update `docker-compose.yml` if needed
5. Add Prometheus metrics
6. Write tests in `tests/integration/`

### Adding a New Message Type

1. Define Pydantic model in `shared/schemas.py`
2. Update NATS topic list in README
3. Implement publisher in relevant service
4. Implement subscriber in consuming services
5. Test with `pytest tests/unit/test_schemas.py`

## Performance Targets (Phase 3+)

- **Quote Update Latency**: < 100ms (NATS → quote generation → publish)
- **Order Execution Latency**: < 500ms (quote → Binance REST → fill notification)
- **Volatility Estimation**: Updated every 1s
- **Order Book Processing**: 100 updates/sec per symbol
- **Memory Usage**: < 500MB per service
- **CPU Utilization**: < 30% per service in normal conditions

## Troubleshooting

### Services won't start

1. Check Docker containers are healthy:
   ```bash
   docker-compose ps
   docker-compose logs
   ```

2. Check NATS connectivity:
   ```bash
   docker-compose exec nats nc -zv nats 4222
   ```

3. Check Redis connectivity:
   ```bash
   docker-compose exec redis redis-cli ping
   ```

4. Check TimescaleDB:
   ```bash
   docker-compose exec timescaledb psql -U postgres -d marketmaker -c "SELECT 1"
   ```

### Database not initialized

```bash
# Verify hypertables exist
python scripts/setup_db.py

# Drop and recreate (careful!)
python scripts/setup_db.py --drop
```

### High latency

1. Check service logs for errors:
   ```bash
   python -m services.as_engine.main
   ```

2. Monitor Prometheus metrics
3. Check NATS message throughput
4. Profile with `py-spy` for CPU bottlenecks

## Contributing

1. Fork repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Make changes following code style guidelines
4. Add tests for new functionality
5. Run full test suite: `pytest --cov`
6. Submit pull request

## References

- **Paper**: Avellaneda & Stoikov (2008) - "High Frequency Trading in a Limit Order Book"
- **Binance API**: https://binance-docs.github.io/apidocs/futures/
- **NATS**: https://nats.io/
- **TimescaleDB**: https://www.timescale.com/

## License

MIT License - see LICENSE file

## Disclaimer

**⚠️ Warning**: This is educational software for research and testing purposes only.

- **Live Trading Risk**: Paper trading is enabled by default. Use at your own risk.
- **Market Risk**: High-frequency trading incurs execution risk, slippage, and market impact.
- **Technical Risk**: Bugs, network failures, or market anomalies could cause losses.
- **Regulatory**: Check local regulations before deploying in production.

Start with **testnet only** (BINANCE_TESTNET=true) and thoroughly backtest before any live deployment.

## Roadmap

### Short Term (Weeks 1-2) ✅
- [x] Phase 1: Infrastructure foundation
- [x] Phase 2: Data ingestion & features
- [x] Phase 3: Strategy implementation (AS math)

### Medium Term (Months 2-3) - NEXT
- [ ] Phase 4: Trading & risk management
  - [ ] Order routing to Binance
  - [ ] Position tracking and P&L
  - [ ] Risk limits and stop-loss
- [ ] Testnet validation and tuning
- [ ] Documentation and examples

### Long Term (Months 4+)
- [ ] Phase 5: Advanced features & optimization
- [ ] Backtesting engine with historical data
- [ ] Parameter optimization (genetic algorithms, Bayesian)
- [ ] Live deployment with safeguards
- [ ] Multi-symbol support (ETHUSDT, LTCUSDT, etc.)
- [ ] Advanced features (adaptive spreads, regime detection)
- [ ] Options & derivatives support

## Documentation

- **[Phase 1 Summary](PHASE_1_SUMMARY.md)** - Infrastructure & foundation
- **[Phase 2 Summary](PHASE_2_SUMMARY.md)** - Data ingestion & feature calculation
- **[Phase 2 Execution Report](PHASE_2_EXECUTION_REPORT.md)** - Detailed Phase 2 implementation
- **[Phase 3 Implementation](PHASE_3_IMPLEMENTATION.md)** - Avellaneda-Stoikov strategy details
- **[Quick Start Guide](QUICKSTART.md)** - Get up and running in 10 minutes

## Support

For issues, questions, or contributions:

1. **GitHub Issues**: Report bugs and request features
2. **Discussions**: Ask questions in GitHub Discussions
3. **Email**: contact@example.com

---

**Last Updated**: October 30, 2024
**Status**: Phase 3 Complete ✅ | Phase 4 In Planning