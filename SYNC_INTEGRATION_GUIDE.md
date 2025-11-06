# Clock Synchronization Integration Guide

## Overview

The clock synchronization system ensures all running services are synchronized around a common logical clock, preventing race conditions and data misalignment even when running only a subset of services.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Sync Clock Service                            │
│  (Maintains authoritative time, orchestrates sync points)      │
└─────────────────────────────────────────────────────────────────┘
         ↑                    ↑                    ↑
         │                    │                    │
    sync.register.v1     sync.ready.v1       sync.heartbeat.v1
         │                    │                    │
         ↓                    ↓                    ↓
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ marketdata_gw    │ │ features_svc     │ │ volflow_estimator│
│ (SyncClient)     │ │ (SyncClient)     │ │ (SyncClient)     │
└──────────────────┘ └──────────────────┘ └──────────────────┘
```

## How It Works

1. **Service Registration**: Each service creates a `SyncClient` and calls `start()`
   - This registers the service with the sync clock
   - Only services listed as `enabled: true` in `config.yaml` are synchronized

2. **Data Processing**: When a service has new data:
   - Service calls `sync_client.wait_for_sync_point(data_timestamp_ms)`
   - This signals "I'm ready with new data"

3. **Synchronization Point**: When ALL enabled services are ready:
   - Sync clock publishes a synchronized timestamp via `sync.clock.v1`
   - All services receive the sync point and proceed

4. **Graceful Degradation**: Missing/disabled services are skipped
   - If running only `marketdata_gw`, `features_svc`, and `volflow_estimator`
   - AS engine is NOT synchronized (not running)
   - Sync proceeds only when these 3 services are ready

## Configuration

### Enable/Disable Services

In `config.yaml`:

```yaml
services:
  marketdata_gw:
    enabled: true      # Include in synchronization
  features_svc:
    enabled: true
  volflow_estimator:
    enabled: true
  as_engine:
    enabled: false     # Will be skipped in sync
  order_router:
    enabled: false
  inventory_svc:
    enabled: false
  risk_manager:
    enabled: false
  metrics_svc:
    enabled: false
```

### Sync Timing

```yaml
strategy:
  quoting:
    update_freq_ms: 100    # Sync interval (100ms boundaries)
```

This means all services synchronize every 100ms (10Hz).

## Integration Steps

For each service you want to synchronize, follow these steps:

### 1. Import the SyncClient

```python
from shared.sync_client import SyncClient
```

### 2. Initialize SyncClient in __init__

```python
class MyService:
    def __init__(self, config):
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self.sync_client = SyncClient(config, self.nats, service_name="my_service")
        # ... rest of init
```

### 3. Start SyncClient in start()

```python
async def start(self) -> None:
    logger.info("Starting MyService")
    try:
        await self.nats.connect()
        await self.sync_client.start()  # Add this line

        # Subscribe to inputs...
        # ...rest of start
```

### 4. Call wait_for_sync_point() When Processing Data

**Before:**
```python
async def on_depth(self, msg) -> None:
    """Handle depth snapshot."""
    try:
        depth = DepthSnapshot.model_validate_json(msg.data)
        # Process immediately
        result = self.process_depth(depth)
        await self.publish_result(result)
```

**After:**
```python
async def on_depth(self, msg) -> None:
    """Handle depth snapshot."""
    try:
        depth = DepthSnapshot.model_validate_json(msg.data)

        # Wait for sync point with other services
        sync_point_ms = await self.sync_client.wait_for_sync_point(
            depth.timestamp_ms
        )

        # Now all services are synchronized
        result = self.process_depth(depth, sync_point_ms)
        await self.publish_result(result)
```

### 5. Stop SyncClient in stop()

```python
async def stop(self) -> None:
    """Stop service."""
    logger.info("Stopping MyService")
    await self.sync_client.stop()  # Add this line
    await self.nats.close()
```

## Usage Examples

### Running Only 3 Services (Audit)

```bash
# Configuration in config.yaml
services:
  marketdata_gw:
    enabled: true       # ✓ Synchronized
  features_svc:
    enabled: true       # ✓ Synchronized
  volflow_estimator:
    enabled: true       # ✓ Synchronized
  as_engine:
    enabled: false      # ✗ Skipped
  order_router:
    enabled: false      # ✗ Skipped
  # ... rest disabled

# Start the services
python launcher.py --services marketdata_gw,features_svc,volflow_estimator

# Sync clock automatically:
# - Registers: marketdata_gw, features_svc, volflow_estimator
# - Waits for all 3 to be ready
# - Publishes sync point
# - Repeats every 100ms
```

### Running Full System

```bash
# Configuration in config.yaml
services:
  marketdata_gw:
    enabled: true       # ✓
  features_svc:
    enabled: true       # ✓
  volflow_estimator:
    enabled: true       # ✓
  as_engine:
    enabled: true       # ✓
  order_router:
    enabled: true       # ✓
  inventory_svc:
    enabled: true       # ✓
  risk_manager:
    enabled: true       # ✓
  metrics_svc:
    enabled: true       # ✓

# Start all services
python launcher.py

# Sync clock:
# - Registers all 8 services
# - Waits for ALL to be ready before sync point
# - Full system synchronization every 100ms
```

### Running with Virtual Time (Replay)

Future enhancement: The sync clock supports virtual time mode for deterministic replay:

```python
# In sync_clock_service.py
sync_service.sync_mode = SyncMode.VIRTUAL_TIME
sync_service.current_sync_point_ms = starting_timestamp_ms

# Then advance time manually or from a data replay source
# All services see the same virtual timestamp
```

## Key Properties

### SyncClient Methods

```python
# Main synchronization method
sync_point_ms = await sync_client.wait_for_sync_point(data_timestamp_ms)
# Returns: Synchronized timestamp for all services

# Optional: Register callback for sync events
await sync_client.set_sync_point_callback(my_async_callback)

# Get status
status = sync_client.get_status()
# Returns: dict with registration, ready state, counters
```

### Message Types

**sync.register.v1** - Service registration
```python
{
    "service_name": "marketdata_gw"
}
```

**sync.ready.v1** - Service ready signal
```python
{
    "service_name": "marketdata_gw",
    "timestamp_ms": 1699564800000
}
```

**sync.clock.v1** - Sync point from clock service
```python
{
    "sync_point_ms": 1699564800000,
    "sync_counter": 42,
    "mode": "real_time",
    "num_services": 3,
    "timestamp_ms": 1699564800000
}
```

**sync.heartbeat.v1** - Periodic heartbeat
```python
{
    "service_name": "marketdata_gw"
}
```

## Troubleshooting

### Services Not Synchronizing

1. **Check if sync clock is running:**
   ```bash
   # The sync clock service should be started before other services
   python services/sync_clock/main.py
   ```

2. **Check service configuration:**
   ```yaml
   # Ensure the service is enabled in config.yaml
   services:
     your_service:
       enabled: true  # Must be true
   ```

3. **Check logs:**
   ```bash
   grep "Sync client started for" logs/marketmaker.log
   grep "Service registered" logs/marketmaker.log
   ```

### Slow Synchronization / Timeout

If a service is too slow to process data:

1. **Increase sync interval:**
   ```yaml
   strategy:
     quoting:
       update_freq_ms: 200  # 200ms instead of 100ms
   ```

2. **Disable slow service:**
   ```yaml
   services:
     slow_service:
       enabled: false  # Skip in sync
   ```

3. **Check service health:**
   - Look for service logs showing processing delays
   - Check if service is receiving data properly

## Data Alignment Benefits

With clock synchronization:

1. **OFI and Volatility Aligned**: Features calculated on same depth snapshot
2. **No Race Conditions**: All services see same logical timestamp
3. **Deterministic Behavior**: Same data → same calculations
4. **Flexible Deployment**: Run partial systems without sync issues
5. **Audit Trail**: Synchronized timestamps in all messages

## Without Synchronization (Previous State)

```
Time: 100ms
├─ marketdata_gw receives depth
├─ features_svc processes OLD depth (99ms)
└─ volflow_estimator processes NEW trade (100ms)
   → OFI and volatility misaligned!

Time: 101ms
├─ features_svc NOW processes current depth
├─ as_engine generates quote with MIXED data
└─ Race condition: features from 100ms, volflow from 99ms
```

## With Synchronization (New State)

```
Time: 100ms
├─ marketdata_gw: "I have new depth at 100ms" → signal ready
├─ features_svc: (still processing)
└─ volflow_estimator: (still processing)

Time: 101ms
├─ features_svc: "I processed depth at 100ms" → signal ready
├─ volflow_estimator: (still processing)
└─ marketdata_gw: (waits for next data)

Time: 102ms
├─ volflow_estimator: "I processed trade at 100ms" → signal ready
├─ Sync clock: "All services at 100ms, publishing sync point"
└─ All services: Proceed with 100ms timestamp
   → OFI and volatility from SAME point in time!
```

## Advanced Usage

### Monitor Sync Health

```python
# In a monitoring service
status = sync_client.get_status()
print(f"Sync counter: {status['sync_counter']}")
print(f"Registered: {status['is_registered']}")
print(f"Current sync point: {status['current_sync_point_ms']}")
```

### React to Sync Points

```python
# Optional callback for custom behavior
async def on_sync_point(event):
    print(f"Sync point {event.sync_counter}: {event.sync_point_ms}ms")
    print(f"Services in sync: {event.num_services}")

sync_client.set_sync_point_callback(on_sync_point)
```

### Disable Sync for a Service

If a service doesn't need synchronization:

```yaml
services:
  my_service:
    enabled: false  # Won't participate in sync
```

The service can still run independently; it just won't be synchronized.

## Summary

The clock synchronization component provides:

- ✓ Flexible configuration for partial/full system runs
- ✓ Automatic alignment of data across services
- ✓ Graceful degradation for missing services
- ✓ Deterministic, auditable behavior
- ✓ No changes to core service logic
- ✓ Easy integration via `SyncClient`

Simply add the `SyncClient` to each service and call `wait_for_sync_point()` before processing data.
