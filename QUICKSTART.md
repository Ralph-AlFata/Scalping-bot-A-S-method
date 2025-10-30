# Quick Start Guide - Phase 1 Complete ✅

Get the Avellaneda-Stoikov market maker running locally in 5 minutes.

## Prerequisites

- Docker & Docker Compose installed
- Python 3.11+
- uv package manager (or pip)

## 5-Minute Setup

### 1. Install Dependencies
```bash
uv sync
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your Binance testnet credentials (optional for Phase 1)
```

### 3. Start Infrastructure
```bash
docker-compose up -d
```

Wait for all services to be healthy:
```bash
docker-compose ps
# All should show "healthy"
```

### 4. Initialize Database
```bash
python scripts/setup_db.py
```

Expected output:
```
✓ Connected to marketmaker@localhost:5432
✓ TimescaleDB extension enabled
✓ Created ticks table
...
✅ Database setup complete!
```

### 5. Verify Everything Works

**Option A: Run Tests**
```bash
# Unit tests (fast)
pytest tests/unit -v

# Integration tests (requires docker-compose running)
pytest tests/integration -v -s
```

**Option B: Check Services Manually**

Open in browser:
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **NATS**: http://localhost:8222

Run a simple test:
```bash
python -c "
import asyncio
from shared.nats_client import NATSClient

async def test():
    client = NATSClient('nats://localhost:4222')
    await client.connect()
    print('✓ NATS connected')
    await client.close()

asyncio.run(test())
"
```

## What's Running?

| Service | What It Does | Status |
|---------|-------------|--------|
| NATS | Message broker | ✅ Operational |
| Redis | Cache layer | ✅ Operational |
| TimescaleDB | Time-series database | ✅ Operational |
| Prometheus | Metrics collection | ✅ Operational |
| Grafana | Dashboards | ✅ Operational |
| 9 Microservices | Trading logic | 🔧 Scaffolds (Phase 2+) |

## Next Steps

### Phase 2: Data Ingestion (Weeks 3-4)

Once Phase 1 is complete, Phase 2 will add:
1. **Binance WebSocket Integration** → marketdata_gw
2. **Feature Calculation** → features_svc
3. **Order Flow Metrics** → volflow_estimator

### Phase 3: Strategy (Weeks 5-6)

Then Phase 3 implements:
1. **Avellaneda-Stoikov Math** → as_engine
2. **Quote Generation** → optimal bid/ask quotes
3. **Inventory Adjustment** → position-aware pricing

### Phase 4: Trading (Weeks 7-8)

Finally Phase 4 adds:
1. **Order Placement** → order_router
2. **Position Tracking** → inventory_svc
3. **Risk Management** → risk_manager

## Development Workflow

### Start a Service Manually

```bash
# Run a single service with logs
python -m services.features_svc.main

# Or in tmux for all services
tmux new-session -d -s trader
tmux send-keys -t trader "python -m services.marketdata_gw.main" Enter
tmux send-keys -t trader "python -m services.features_svc.main" Enter
# etc...
```

### Monitor Logs

```bash
# Docker container logs
docker-compose logs -f nats
docker-compose logs -f timescaledb

# Service logs
tail -f logs/*.log
```

### Run Tests

```bash
# Quick unit tests
pytest tests/unit -v

# Full integration test suite (slow)
pytest tests/integration -v -s

# With coverage
pytest --cov=shared --cov=services
```

## Project Structure (Phase 1)

```
✅ COMPLETE (Phase 1)
├── docker-compose.yml          Infrastructure
├── pyproject.toml              Dependencies
├── config.yaml                 Configuration
├── shared/                     Utilities
│   ├── schemas.py              Message types (10+ Pydantic models)
│   ├── nats_client.py          NATS wrapper
│   ├── redis_client.py         Redis wrapper
│   ├── db_client.py            Database wrapper
│   ├── config.py               Config loader
│   └── logger.py               Logging
├── services/                   9 Microservices (scaffolds)
├── monitoring/                 Prometheus + Grafana
├── scripts/setup_db.py         Database initialization
├── tests/                      Unit & integration tests
└── README.md                   Full documentation

🔄 IN PROGRESS (Phase 2)
├── Binance WebSocket integration
├── Feature calculation (OFI, micro-price)
└── Order flow metrics

⏳ UPCOMING (Phase 3+)
├── AS strategy implementation
├── Order placement & execution
├── Risk management
└── Backtesting engine
```

## Troubleshooting

### Docker containers not healthy?

```bash
# Check logs
docker-compose logs

# Restart specific service
docker-compose restart nats

# Full reset
docker-compose down
docker-compose up -d
```

### Python import errors?

```bash
# Make sure dependencies are installed
uv sync

# Or with pip
pip install -e .
pip install -e .[dev]
```

### Tests failing?

```bash
# Check docker-compose is running
docker-compose ps

# Run tests with more verbose output
pytest tests/integration -v -s --tb=short

# Check database is initialized
python scripts/setup_db.py
```

## Key Files to Understand

1. **[config.yaml](config.yaml)** - Strategy parameters and risk limits
2. **[shared/schemas.py](shared/schemas.py)** - All message types between services
3. **[docker-compose.yml](docker-compose.yml)** - Infrastructure definition
4. **[pyproject.toml](pyproject.toml)** - Dependencies and build config
5. **[README.md](README.md)** - Full technical documentation

## Important Concepts

### Message Flow

All services communicate via NATS topics:
```
raw.depth.v1 + raw.trades.v1
        ↓
features.v1 + volflow.v1
        ↓
quotes.v1
        ↓
fill.v1 + inventory.v1
        ↓
risk alerts
```

### Configuration

- YAML file: `config.yaml` (persistent)
- Environment variables: `.env` (overrides YAML)
- Usage: `from shared.config import load_config`

### Logging

```python
from shared.logger import setup_logging

logger = setup_logging("my_service", level="DEBUG")
logger.info("Message", extra_field="value")
```

## Production Readiness

Phase 1 is production-ready for **infrastructure only**:
- ✅ Multi-service async architecture
- ✅ Message broker (NATS) with JetStream
- ✅ Time-series database (TimescaleDB)
- ✅ Monitoring (Prometheus + Grafana)
- ✅ Structured logging
- ✅ Health checks
- ✅ Error handling
- ✅ Configuration management

**NOT ready for trading yet** (requires Phase 2-4):
- No Binance connection
- No quote generation
- No order execution

## Resources

- **Full Docs**: [README.md](README.md)
- **Paper**: [Avellaneda & Stoikov 2008](Paper%20Method.pdf)
- **Binance API**: https://binance-docs.github.io/apidocs/futures/
- **NATS**: https://nats.io/
- **TimescaleDB**: https://www.timescale.com/

## Questions?

1. Check [README.md](README.md) for comprehensive documentation
2. Look at [tests](tests/) for code examples
3. Review [config.yaml](config.yaml) for all parameters
4. Read service code in [services/](services/)

---

**Status**: Phase 1 ✅ Complete
**Next Phase**: Phase 2 (Data Ingestion)
**Estimated Time**: 2-3 weeks for full system

Good luck! 🚀