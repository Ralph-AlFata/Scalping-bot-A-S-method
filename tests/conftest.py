"""
Pytest configuration and fixtures for testing.
Provides fixtures for NATS, Redis, and TimescaleDB clients.
"""

import os
import asyncio
import pytest
from typing import AsyncGenerator, Generator

from shared.nats_client import NATSClient
from shared.redis_client import RedisClient
from shared.db_client import DBClient
from shared.config import load_config
from shared.logger import setup_logging


# ==================== Configuration ====================
@pytest.fixture(scope="session")
def config():
    """Load configuration."""
    return load_config()


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create asyncio event loop for session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# ==================== NATS Fixtures ====================
@pytest.fixture
async def nats_client(config) -> AsyncGenerator[NATSClient, None]:
    """Create and connect NATS client."""
    client = NATSClient(config.nats.url, name="test-client")
    await client.connect()
    yield client
    await client.close()


# ==================== Redis Fixtures ====================
@pytest.fixture
async def redis_client(config) -> AsyncGenerator[RedisClient, None]:
    """Create and connect Redis client."""
    client = RedisClient(
        host=config.redis.host,
        port=config.redis.port,
        db=config.redis.db,
    )
    await client.connect()
    # Flush database before each test
    await client.flush_db()
    yield client
    await client.close()


# ==================== Database Fixtures ====================
@pytest.fixture
async def db_client(config) -> AsyncGenerator[DBClient, None]:
    """Create and connect TimescaleDB client."""
    client = DBClient(
        host=config.timescaledb.host,
        port=config.timescaledb.port,
        user=config.timescaledb.user,
        password=config.timescaledb.password,
        database=config.timescaledb.database,
    )
    await client.connect()
    yield client
    await client.close()


# ==================== Logging Fixture ====================
@pytest.fixture(scope="session", autouse=True)
def setup_test_logging():
    """Set up test logging."""
    setup_logging("tests", level="DEBUG", log_format="console")


# ==================== Integration Test Marker ====================
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow"
    )