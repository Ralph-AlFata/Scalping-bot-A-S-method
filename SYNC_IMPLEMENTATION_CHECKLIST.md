# Sync Clock Implementation Checklist

This is a step-by-step checklist to integrate the sync clock into your services.

## System Setup

- [ ] Sync clock service files exist:
  - `shared/sync_clock.py` - Central sync service
  - `shared/sync_client.py` - Client library
  - `services/sync_clock_service/main.py` - Entry point

- [ ] Documentation files exist:
  - `SYNC_INTEGRATION_GUIDE.md` - Full guide
  - `SYNC_QUICK_START.md` - Quick start
  - `SYNC_SERVICE_INTEGRATION_EXAMPLE.py` - Code examples

## Configuration Setup

- [ ] Edit `config.yaml`:
  - [ ] Set `services.marketdata_gw.enabled: true`
  - [ ] Set `services.features_svc.enabled: true`
  - [ ] Set `services.volflow_estimator.enabled: true`
  - [ ] Set other services to `enabled: false` (for now)
  - [ ] Verify `strategy.quoting.update_freq_ms: 100` (sync interval)

## Service 1: marketdata_gw

### Integration Steps

- [ ] Open `services/marketdata_gw/main.py`

- [ ] Add import (near top with other imports):
  ```python
  from shared.sync_client import SyncClient
  ```

- [ ] Find `BinanceWebSocketManager.__init__()` method
  - [ ] Add after `self.on_trade_callback = on_trade_callback`:
    ```python
    self.sync_client = SyncClient(config, nats_client, "marketdata_gw")
    ```

- [ ] Find `BinanceWebSocketManager.start()` method
  - [ ] Add after line where it logs "Starting Binance WebSocket streams":
    ```python
    await self.sync_client.start()
    ```

- [ ] Find the method that publishes depth events (likely `_stream_depth()` or similar)
  - [ ] Before publishing depth, add:
    ```python
    sync_point_ms = await self.sync_client.wait_for_sync_point(depth.timestamp_ms)
    ```
  - [ ] Update the message to use sync_point_ms (or add as separate field)

- [ ] Find the method that publishes trade events (likely `_stream_trades()` or similar)
  - [ ] Before publishing trade, add:
    ```python
    sync_point_ms = await self.sync_client.wait_for_sync_point(trade.timestamp_ms)
    ```

- [ ] Find `BinanceWebSocketManager.stop()` method
  - [ ] Add before `self._running = False`:
    ```python
    await self.sync_client.stop()
    ```

### Testing

- [ ] Start sync clock service:
  ```bash
  python services/sync_clock_service/main.py
  ```

- [ ] Start marketdata_gw:
  ```bash
  python services/marketdata_gw/main.py
  ```

- [ ] Check logs for:
  - [ ] "Sync client started for marketdata_gw"
  - [ ] "Service registered with sync clock"
  - [ ] "Received sync point" (should appear frequently)

## Service 2: features_svc

### Integration Steps

- [ ] Open `services/features_svc/main.py`

- [ ] Add import (near top with other imports):
  ```python
  from shared.sync_client import SyncClient
  ```

- [ ] Find the main service class (likely `FeaturesService` or similar)
  - [ ] In `__init__()`, add after creating nats_client:
    ```python
    self.sync_client = SyncClient(config, nats_client, "features_svc")
    ```

- [ ] Find `start()` method
  - [ ] Add after `await self.nats.connect()`:
    ```python
    await self.sync_client.start()
    ```

- [ ] Find the method that handles depth snapshots (callback for `raw.depth.v1`)
  - [ ] Add after parsing the depth message:
    ```python
    sync_point_ms = await self.sync_client.wait_for_sync_point(depth.timestamp_ms)
    ```
  - [ ] Update feature message timestamp (optional but recommended):
    ```python
    if features:
        features.timestamp_ms = sync_point_ms
    ```

- [ ] Find `stop()` method
  - [ ] Add before closing nats:
    ```python
    await self.sync_client.stop()
    ```

### Testing

- [ ] Have sync clock and marketdata_gw running (from Service 1)

- [ ] Start features_svc:
  ```bash
  python services/features_svc/main.py
  ```

- [ ] Check logs for:
  - [ ] "Sync client started for features_svc"
  - [ ] "Service registered with sync clock"
  - [ ] "Received sync point"

- [ ] Watch sync clock logs for:
  - [ ] "Service registered" showing marketdata_gw and features_svc
  - [ ] "Sync point published" (should now happen more frequently as it waits for both services)

## Service 3: volflow_estimator

### Integration Steps

- [ ] Open `services/volflow_estimator/main.py`

- [ ] Add import (near top with other imports):
  ```python
  from shared.sync_client import SyncClient
  ```

- [ ] Find the main service class (likely contains `VolflowCalculator`)
  - [ ] In `__init__()`, add after creating nats_client:
    ```python
    self.sync_client = SyncClient(config, nats_client, "volflow_estimator")
    ```

- [ ] Find `start()` method
  - [ ] Add after `await self.nats.connect()`:
    ```python
    await self.sync_client.start()
    ```

- [ ] Find the method that handles trade messages (callback for `raw.trades.v1`)
  - [ ] Add after parsing the trade message:
    ```python
    sync_point_ms = await self.sync_client.wait_for_sync_point(trade.timestamp_ms)
    ```
  - [ ] Update volflow message timestamp (optional but recommended):
    ```python
    if volflow:
        volflow.timestamp_ms = sync_point_ms
    ```

- [ ] Find `stop()` method
  - [ ] Add before closing nats:
    ```python
    await self.sync_client.stop()
    ```

### Testing

- [ ] Have sync clock, marketdata_gw, and features_svc running

- [ ] Start volflow_estimator:
  ```bash
  python services/volflow_estimator/main.py
  ```

- [ ] Check logs for:
  - [ ] "Sync client started for volflow_estimator"
  - [ ] "Service registered with sync clock"
  - [ ] "Received sync point"

- [ ] Watch sync clock logs for:
  - [ ] "Service registered" showing all 3 services
  - [ ] "Sync point published" with "num_services: 3"

## System Verification

Once all 3 services are integrated and running:

### Sync Clock Service Logs

Look for pattern (repeating every ~100ms):
```
Sync point published - sync_point_ms=1699564800000, sync_counter=42
```

### Service Logs

Each service should show:
```
Service registered with sync clock
Received sync point - sync_point_ms=1699564800000
```

### Message Flow

Check NATS messages (if you have nats-cli):
```bash
nats sub "sync.*.v1" -s nats://localhost:4222
```

You should see:
- `sync.register.v1` messages from each service (on startup)
- `sync.ready.v1` messages from services (when they have data)
- `sync.clock.v1` messages from sync clock (broadcasts sync points)
- `sync.heartbeat.v1` messages (periodic liveness signals)

## Common Issues & Fixes

### Issue: "Service not registered"

**Symptom:** Sync clock not showing service as registered

**Fix:**
1. Check service is enabled in `config.yaml`:
   ```yaml
   services:
     your_service:
       enabled: true
   ```
2. Check service is calling `await self.sync_client.start()`
3. Check NATS connection is working

### Issue: "Sync points not flowing"

**Symptom:** Sync clock not publishing sync.clock.v1

**Fix:**
1. Check all expected services are connected
2. Check services are calling `wait_for_sync_point()`
3. Look for service logs showing "received data"
4. Check sync clock logs for errors

### Issue: "Services waiting for sync indefinitely"

**Symptom:** Logs show "waiting for sync point" but nothing happens

**Fix:**
1. Check all 3 services are running
2. Check all 3 services have `enabled: true` in config
3. Check sync clock is running
4. Try restarting sync clock service

### Issue: "NATS connection refused"

**Symptom:** Services can't connect to NATS

**Fix:**
1. Make sure NATS is running:
   ```bash
   docker-compose up -d nats
   ```
2. Check NATS URL in `config.yaml`:
   ```yaml
   infrastructure:
     nats:
       url: nats://localhost:4222
   ```

## Performance Check

The sync system should be very lightweight. Typical timings:

- [ ] Sync clock publishes every 100ms
- [ ] Services wait 0-10ms for sync point
- [ ] No messages or data are delayed
- [ ] CPU usage minimal (< 1% per service)

To measure:
1. Run the system for 1 minute
2. Count sync points in logs: `grep -c "Sync point published" logs/marketmaker.log`
3. Expected: ~600 sync points (1 per 100ms × 60 seconds)

## Rollout Plan

When moving to other services (as_engine, order_router, etc.):

1. **As Engine:**
   - [ ] Set `services.as_engine.enabled: true` in config
   - [ ] Add SyncClient (same 5 steps)
   - [ ] Call `wait_for_sync_point()` before generating quotes
   - [ ] Verify sync includes all 4 services

2. **Other Services:**
   - [ ] Repeat above for each additional service
   - [ ] Update config.yaml to enable
   - [ ] Add 5 integration steps
   - [ ] Verify in sync clock logs

## Final Checklist

- [ ] All 3 services integrated with SyncClient
- [ ] config.yaml shows correct `enabled` status
- [ ] Sync clock service running
- [ ] All 3 services registered with sync clock
- [ ] Sync points publishing every 100ms
- [ ] Services receiving sync points
- [ ] Data timestamps synchronized
- [ ] Logs showing no errors or warnings
- [ ] System running stable for 5+ minutes
- [ ] No data quality issues in calculations
- [ ] Ready for next phase (as_engine integration)

## Success Criteria

When everything is working:

1. **Sync Clock Logs:**
   ```
   Expected services: {'marketdata_gw', 'features_svc', 'volflow_estimator'}
   Service registered - service_name=marketdata_gw
   Service registered - service_name=features_svc
   Service registered - service_name=volflow_estimator
   Sync point published - sync_point_ms=..., sync_counter=1
   Sync point published - sync_point_ms=..., sync_counter=2
   Sync point published - sync_point_ms=..., sync_counter=3
   ...
   ```

2. **Service Logs (each):**
   ```
   Sync client started for [service_name]
   Service [service_name] registered with sync clock
   Received sync point - sync_point_ms=..., sync_counter=1
   Received sync point - sync_point_ms=..., sync_counter=2
   ...
   ```

3. **Data Processing:**
   - marketdata_gw publishes depth/trades
   - features_svc publishes features at same timestamp
   - volflow_estimator publishes volflow at same timestamp
   - All use same sync_point_ms

4. **System Health:**
   - No hanging or slow processing
   - No NATS message buildup
   - CPU usage stable
   - Memory usage stable

You're done when all above criteria are met! ✅
