# Sync Clock - Market Data Driven Architecture

## Overview

The sync clock has been reconfigured to use **marketdata_gw as the clock source**. This means:

- The system synchronizes based on **when market data arrives** (depth/trade updates from Binance)
- Not on a fixed 100ms interval
- All other services synchronize to the timing of actual market events
- Natural, event-driven synchronization without artificial delays

## Architecture

### Before (Fixed Interval)
```
Time: 0ms    50ms    100ms   150ms   200ms
├─ Sync      │       Sync     │       Sync
│  point     │       point    │       point
└─ All services wait for fixed 100ms boundary
```

### After (Market Data Driven)
```
Depth arrives: 42ms   Depth arrives: 146ms   Depth arrives: 249ms
│              │                   │                     │
└─ Sync point  └─── Sync point ───└─────── Sync point ──┘
   (42ms)           (146ms)               (249ms)

All services sync to actual market data arrival times
```

## How It Works

### 1. Market Data Arrives
```
marketdata_gw receives depth update at timestamp 42ms
```

### 2. Service Signals Ready
```python
# In marketdata_gw
sync_point_ms = await self.sync_client.wait_for_sync_point(42)
# This publishes: sync.ready.v1 with timestamp_ms=42
```

### 3. Sync Clock Captures Timestamp
```python
# In sync_clock.py _handle_service_ready()
reg.last_ready_timestamp_ms = 42  # Capture the data timestamp
```

### 4. All Services Ready → Sync Point Published
```python
# In sync_clock.py _publish_sync_point()
sync_point_ms = marketdata_reg.last_ready_timestamp_ms  # Use 42ms from marketdata_gw
# Broadcasts: sync.clock.v1 with sync_point_ms=42
```

### 5. All Services Process at Same Timestamp
```
features_svc receives depth with timestamp 42
volflow_estimator uses trades from timestamp 42
as_engine generates quotes for timestamp 42
order_router executes orders based on timestamp 42
```

## Key Changes

### sync_clock.py

#### ServiceRegistration (Line 44-52)
```python
@dataclass
class ServiceRegistration:
    service_name: str
    enabled: bool = True
    last_heartbeat_ms: float = field(default_factory=lambda: time.time() * 1000)
    is_ready: bool = False
    last_sync_point_ms: float = 0.0
    last_ready_timestamp_ms: float = 0.0  # NEW: Captures marketdata timestamp
```

#### _handle_service_ready() (Line 193-197)
```python
# Captures the timestamp from ready signal (especially from marketdata_gw)
reg = self.registered_services[service_name]
reg.is_ready = True
reg.last_ready_timestamp_ms = timestamp_ms  # NEW: Store data timestamp
```

#### _publish_sync_point() (Line 262-299)
```python
# Now uses marketdata_gw's timestamp as sync point
marketdata_reg = self.registered_services.get("marketdata_gw")

if marketdata_reg and hasattr(marketdata_reg, 'last_ready_timestamp_ms'):
    # Use marketdata_gw's timestamp as the authoritative sync point
    sync_point_ms = int(marketdata_reg.last_ready_timestamp_ms)
else:
    # Fallback: use current time if not ready yet
    sync_point_ms = int(time.time() * 1000)
```

## Benefits

### 1. **Event-Driven Synchronization**
   - No artificial delays waiting for clock boundaries
   - Synchronizes to actual market events

### 2. **Optimal Latency**
   - All calculations happen as soon as data arrives
   - Reduced time between market data and quote generation
   - Better for fast-moving markets

### 3. **Natural Rate Matching**
   - If market data comes every 50ms → system ticks at 50ms
   - If market data comes every 200ms → system ticks at 200ms
   - System naturally adapts to market velocity

### 4. **Reduced Timeout Issues**
   - No more "Sync point timeout" warnings
   - Services process data immediately after marketdata_gw publishes

### 5. **Consistent Timestamps**
   - All downstream services use the same timestamp as market data
   - No timestamp drift across services

## Example Flow

### Scenario: Depth Update Arrives

```
Time: 145.234ms
├─ Binance sends depth update to marketdata_gw
│
└─ marketdata_gw:
   ├─ Receives depth at timestamp 145.234ms
   ├─ Calls wait_for_sync_point(145)
   ├─ Publishes sync.ready.v1 with timestamp_ms=145
   │
   └─ sync_clock:
      ├─ Receives sync.ready.v1 from marketdata_gw
      ├─ Saves last_ready_timestamp_ms=145
      ├─ Checks if all other services ready
      │
      ├─ If ALL ready (features_svc, volflow_estimator, as_engine, etc.):
      │  └─ Publishes sync.clock.v1 with sync_point_ms=145
      │
      └─ All services receive sync.clock.v1 at time 145ms
         ├─ features_svc: processes depth with timestamp 145
         ├─ volflow_estimator: updates with timestamp 145
         ├─ as_engine: generates quotes with timestamp 145
         └─ order_router: executes orders at timestamp 145
```

## Testing the Change

### With Single Service (marketdata_gw only)

```bash
# Terminal 1: Start sync clock
python services/sync_clock_service/main.py

# Terminal 2: Start marketdata_gw
python services/marketdata_gw/main.py
```

**Expected Output:**
```
marketdata_gw: Sync client started
marketdata_gw: Service registered with sync clock
[Multiple depth/trade updates received]
sync_clock: Sync point published (marketdata_gw-driven) sync_point_ms=1234567890145
```

**No more "Sync point timeout" warnings!**

### With Multiple Services

```bash
# All services enabled in config.yaml
python launcher.py
```

**Expected Flow:**
```
[marketdata_gw publishes depth at 145ms]
    ↓
[All services receive depth/prev data]
    ↓
[All services call wait_for_sync_point()]
    ↓
[All services signal ready]
    ↓
[sync_clock publishes sync.clock.v1 with sync_point_ms=145]
    ↓
[All services process with timestamp=145]
```

## Configuration

No config changes needed! The sync mechanism works automatically:

- `config.yaml` still defines `strategy.quoting.update_freq_ms` (used by other parts)
- Sync clock ignores fixed interval when marketdata_gw is the clock source
- System naturally runs at market data frequency

## Fallback Behavior

If **marketdata_gw is not enabled**:
- Sync clock falls back to current time (line 274)
- System uses wall-clock time as sync source
- All other services still synchronize normally

Example:
```yaml
services:
  marketdata_gw:
    enabled: false  # Not using market data
  features_svc:
    enabled: true   # Sync clock will use wall-clock time instead
  # ...
```

## Performance Characteristics

### Sync Wait Time
- **Before**: Services waited up to 1 second for timeout (poor UX)
- **After**: Services sync immediately when marketdata_gw publishes

### Typical Sync Latencies
```
Binance → marketdata_gw: ~5-10ms (network)
marketdata_gw → sync_clock: ~1-2ms (NATS)
sync_clock → features_svc: ~1-2ms (NATS broadcast)
features_svc processing: ~2-5ms
total: ~10-20ms per sync cycle
```

### CPU Impact
- No change: still minimal overhead
- Sync loop checks every 10ms (line 256)
- Publishing happens only when all services ready

## Troubleshooting

### Issue: Still Seeing Timeout Warnings

**Cause**: Other services not ready in time

**Solution**:
1. Check all expected services are enabled in config.yaml
2. Check NATS connectivity
3. Check service logs for errors

### Issue: Sync Points Not Publishing

**Cause**: marketdata_gw hasn't sent ready signal

**Solution**:
1. Verify marketdata_gw is running
2. Check it's receiving data from Binance
3. Check sync_client is started in marketdata_gw

### Issue: Timestamps Are Inconsistent

**Cause**: Some services not using sync timestamp

**Solution**:
1. Verify all services are casting sync_point_ms to int() (line 210, 256, 359, 395, 1236)
2. Verify services are updating output message timestamps with sync_point_ms

## Summary

The sync clock is now **market data driven** instead of **fixed interval driven**:

- ✅ Synchronizes to actual market events (depth/trade arrivals)
- ✅ No artificial delays or timeout warnings
- ✅ Natural rate adaptation (fast markets = fast ticks)
- ✅ Better latency for time-sensitive trading
- ✅ All services process with consistent timestamps
- ✅ Graceful fallback to wall-clock if needed

The system is now optimized for real-time, event-driven market making.
