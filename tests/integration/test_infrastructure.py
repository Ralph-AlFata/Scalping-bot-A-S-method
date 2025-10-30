"""
Integration tests for infrastructure components.
Tests NATS, Redis, and TimescaleDB connectivity.
"""

import json
import pytest
from datetime import datetime


@pytest.mark.integration
@pytest.mark.asyncio
async def test_nats_connect(nats_client):
    """Test NATS connection."""
    assert nats_client.is_connected()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_nats_ping(nats_client):
    """Test NATS ping."""
    is_alive = await nats_client.ping()
    assert is_alive


@pytest.mark.integration
@pytest.mark.asyncio
async def test_nats_publish_subscribe(nats_client):
    """Test NATS pub/sub."""
    received_messages = []

    async def callback(msg):
        received_messages.append(msg.data.decode())

    # Subscribe to topic
    await nats_client.subscribe("test.topic.v1", callback)

    # Publish message
    test_message = {"test": "data"}
    await nats_client.publish("test.topic.v1", test_message)

    # Give it a moment to deliver
    import asyncio
    await asyncio.sleep(0.1)

    # Verify message received
    assert len(received_messages) > 0
    received_data = json.loads(received_messages[0])
    assert received_data["test"] == "data"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_connect(redis_client):
    """Test Redis connection."""
    assert redis_client.is_connected()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_set_get(redis_client):
    """Test Redis set/get operations."""
    key = "test_key"
    value = "test_value"

    # Set value
    await redis_client.set(key, value)

    # Get value
    result = await redis_client.get(key)
    assert result.decode() == value


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_json(redis_client):
    """Test Redis JSON operations."""
    key = "test_json"
    data = {"price": 42000.0, "quantity": 1.5}

    # Set JSON
    await redis_client.set(key, data)

    # Get JSON
    result = await redis_client.get_json(key)
    assert result["price"] == 42000.0
    assert result["quantity"] == 1.5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_ttl(redis_client):
    """Test Redis TTL functionality."""
    key = "expiring_key"
    value = "temporary"

    # Set with TTL
    await redis_client.set(key, value, ttl=10)

    # Check TTL
    ttl = await redis_client.ttl(key)
    assert ttl > 0
    assert ttl <= 10


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_counter(redis_client):
    """Test Redis counter functionality."""
    key = "counter"

    # Increment multiple times
    val1 = await redis_client.incr(key)
    val2 = await redis_client.incr(key)
    val3 = await redis_client.incr(key, 5)

    assert val1 == 1
    assert val2 == 2
    assert val3 == 7


@pytest.mark.integration
@pytest.mark.asyncio
async def test_db_connect(db_client):
    """Test TimescaleDB connection."""
    assert db_client.is_connected()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_db_query(db_client):
    """Test TimescaleDB query."""
    result = await db_client.fetchval("SELECT 1;")
    assert result == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_db_insert_and_fetch(db_client):
    """Test TimescaleDB insert and fetch."""
    # Insert test data into ticks table
    await db_client.execute(
        """
        INSERT INTO ticks (
            timestamp, symbol, bid_price, ask_price,
            bid_volume, ask_volume, mid_price, spread_bps
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        datetime.utcnow(),
        "BTCUSDT",
        41999.0,
        42001.0,
        1.5,
        2.0,
        42000.0,
        4.76,
    )

    # Fetch data
    records = await db_client.fetch(
        "SELECT * FROM ticks WHERE symbol = $1 LIMIT 1",
        "BTCUSDT",
    )

    assert len(records) > 0
    assert records[0]["symbol"] == "BTCUSDT"
    assert float(records[0]["bid_price"]) == 41999.0