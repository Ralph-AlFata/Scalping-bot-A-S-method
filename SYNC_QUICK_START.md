# Sync Clock System - Quick Start Guide

## What Was Built

Three new components have been added to your system:

1. **`shared/sync_clock.py`** - The central synchronization service
2. **`shared/sync_client.py`** - Client library for services to use
3. **`services/sync_clock_service/main.py`** - Entry point to run the sync service

Plus comprehensive documentation:
- **`SYNC_INTEGRATION_GUIDE.md`** - Full integration documentation
- **`SYNC_SERVICE_INTEGRATION_EXAMPLE.py`** - Code examples for each service

## Problem Solved

Previously, when you ran only some services (e.g., `marketdata_gw`, `features_svc`, `volflow_estimator`):
- Each service used its own local clock
- OFI calculation (features_svc) might see depth from 100ms
- Volatility calculation (volflow_estimator) might see trade from 99ms
- **Result: Misaligned data, unreliable calculations**

Now with sync clock:
- All enabled services snap to the same 100ms boundaries
- OFI and volatility calculated from synchronized data
- Full flexibility to run 3 services or all 9 services
- Graceful: disabled services are automatically skipped

## 5-Minute Setup

### Step 1: Configure Which Services to Run

Edit `config.yaml`:

```yaml
services:
  marketdata_gw:
    enabled: true       # ✓ Will be synchronized
  features_svc:
    enabled: true       # ✓ Will be synchronized
  volflow_estimator:
    enabled: true       # ✓ Will be synchronized
  as_engine:
    enabled: false      # ✗ Skip (not running)
  order_router:
    enabled: false      # ✗ Skip
  # ... rest disabled
```

### Step 2: Start the Sync Clock Service

Open a terminal:

```bash
python services/sync_clock_service/main.py
```

Output should show:
```
Sync Clock Service Starting
Connected to NATS
Expected services: {'marketdata_gw', 'features_svc', 'volflow_estimator'}
Sync interval: 100ms
```

### Step 3: Integrate Sync Client into Your Services

For each service you want to synchronize (`marketdata_gw`, `features_svc`, `volflow_estimator`):

**Add these 5 changes to the service file:**

#### Change 1: Add import at top
```python
from shared.sync_client import SyncClient
```

#### Change 2: Initialize in __init__
```python
class MyService:
    def __init__(self, config, nats_client):
        # ... existing code ...
        self.sync_client = SyncClient(config, nats_client, "marketdata_gw")  # your service name
```

#### Change 3: Start sync client in start()
```python
async def start(self) -> None:
    await self.nats.connect()
    await self.sync_client.start()  # ADD THIS LINE
    # ... rest of existing code ...
```

#### Change 4: Wait for sync before processing data

When you process incoming data:

```python
async def on_depth(self, msg) -> None:
    try:
        depth = DepthSnapshot.model_validate_json(msg.data)

        # ADD THIS LINE before processing
        sync_point_ms = await self.sync_client.wait_for_sync_point(depth.timestamp_ms)

        # Process with synchronized timestamp
        result = process(depth)
        await publish(result)
```

#### Change 5: Stop sync client in stop()
```python
async def stop(self) -> None:
    await self.sync_client.stop()  # ADD THIS LINE
    # ... rest of existing code ...
```

### Step 4: Start Your Services

```bash
# Terminal 1 (already running)
python services/sync_clock_service/main.py

# Terminal 2
python services/marketdata_gw/main.py

# Terminal 3
python services/features_svc/main.py

# Terminal 4
python services/volflow_estimator/main.py
```

Or use the launcher:

```bash
python launcher.py --services marketdata_gw,features_svc,volflow_estimator
```

## What Happens Now

```
Time: 100ms
├─ marketdata_gw receives depth → calls wait_for_sync_point(100)
├─ features_svc receives depth → calls wait_for_sync_point(100)
└─ volflow_estimator receives trade → calls wait_for_sync_point(100)

Sync Clock Service:
├─ Sees all 3 services ready
├─ Publishes sync.clock.v1 with sync_point_ms=100
└─ All services proceed with timestamp 100ms

Time: 200ms
├─ marketdata_gw receives next depth → calls wait_for_sync_point(200)
├─ features_svc receives next depth → calls wait_for_sync_point(200)
└─ volflow_estimator receives next trade → calls wait_for_sync_point(200)

Sync Clock Service:
└─ All at 200ms → publishes sync point
```

## Verification

### Check Logs

Look for these lines in service logs:

```bash
# Service startup
grep "Sync client started" logs/marketmaker.log

# Registration
grep "Service registered" logs/marketmaker.log

# Sync points
grep "Sync point published" logs/marketmaker.log
grep "Received sync point" logs/marketmaker.log
```

### Check NATS Messages (Optional)

```bash
# If you have nats-cli installed
nats sub "sync.*.v1" -s nats://localhost:4222
```

You should see messages flowing:
- `sync.register.v1` - Service registration
- `sync.ready.v1` - Service ready signals
- `sync.clock.v1` - Sync points from the clock service
- `sync.heartbeat.v1` - Service heartbeats

## Detailed Integration Instructions

See `SYNC_INTEGRATION_GUIDE.md` for:
- Complete architecture overview
- Message format specifications
- Configuration options
- Troubleshooting guide
- Advanced usage patterns

## Code Examples

See `SYNC_SERVICE_INTEGRATION_EXAMPLE.py` for:
- Before/after code for each service
- Exactly where to add lines
- How to handle timestamps
- Error handling patterns

## Key Concepts

### Enabled vs Disabled Services

```yaml
services:
  marketdata_gw:
    enabled: true       # This service participates in sync
  as_engine:
    enabled: false      # This service is skipped (not running)
```

The sync clock automatically:
1. Reads `enabled: true/false` for each service from config
2. Waits only for enabled services
3. Skips disabled services

### Sync Interval

```yaml
strategy:
  quoting:
    update_freq_ms: 100  # Sync every 100ms (10Hz)
```

All services snap to 100ms boundaries:
- Sync point: 0ms, 100ms, 200ms, 300ms...

To sync more frequently (1000ms = 1s):
```yaml
strategy:
  quoting:
    update_freq_ms: 1000
```

### Graceful Degradation

If a service is slow or missing:
1. Sync clock waits up to 1 second
2. If service doesn't respond, it's marked stale
3. Sync continues without it
4. Service can resume when ready

## FAQ

**Q: Do I need to modify AS engine or order_router?**
A: No, not for audit. Set `enabled: false` for services you're not running.

**Q: What if I want to add sync to AS engine later?**
A: Same 5-step process. Integration is non-breaking and additive.

**Q: Can I enable/disable services at runtime?**
A: Currently, you must set in config and restart. Reload support is future work.

**Q: What happens if sync clock service crashes?**
A: Services will timeout waiting for sync and gracefully degrade to local timestamps. Restart sync clock service and services will re-register.

**Q: Is synchronization real-time or virtual?**
A: Currently real-time (wall-clock). Virtual time mode for backtesting is future work.

**Q: How much latency does sync add?**
A: Minimal - typical wait time is 0-10ms depending on service processing speed.

**Q: Can I run without sync clock?**
A: Yes, services work independently. But you lose synchronization benefits. Set `enabled: false` for all services that you're not running to avoid orphaned waiters.

## Next Steps

1. **Choose services to audit:**
   ```yaml
   # config.yaml
   services:
     marketdata_gw: enabled: true
     features_svc: enabled: true
     volflow_estimator: enabled: true
   ```

2. **Start sync clock:**
   ```bash
   python services/sync_clock_service/main.py
   ```

3. **Integrate sync client into your 3 services** (5 changes each):
   - Add import
   - Initialize in __init__
   - Start in start()
   - Call wait_for_sync_point() before processing
   - Stop in stop()

4. **Run your services:**
   ```bash
   python services/marketdata_gw/main.py
   python services/features_svc/main.py
   python services/volflow_estimator/main.py
   ```

5. **Monitor sync in logs:**
   ```bash
   tail -f logs/marketmaker.log | grep -i sync
   ```

## Support

If you need help:
1. Read `SYNC_INTEGRATION_GUIDE.md` for detailed documentation
2. Look at `SYNC_SERVICE_INTEGRATION_EXAMPLE.py` for code examples
3. Check service logs for sync errors
4. Use `sync_client.get_status()` in code for debugging
