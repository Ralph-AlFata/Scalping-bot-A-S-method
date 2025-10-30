"""
Async Redis client wrapper for cache operations.
Provides get/set/delete operations with automatic serialization.
"""

import json
from typing import Any, Optional

import redis.asyncio as redis
from pydantic import BaseModel

from shared.logger import get_logger

logger = get_logger(__name__)


class RedisClient:
    """Async Redis client wrapper."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        ssl: bool = False,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        max_connections: int = 50,
    ):
        """
        Initialize Redis client.

        Args:
            host: Redis host.
            port: Redis port.
            db: Database number.
            password: Redis password.
            ssl: Use SSL connection.
            socket_timeout: Socket timeout in seconds.
            socket_connect_timeout: Connection timeout in seconds.
            max_connections: Max connection pool size.
        """
        self.host = host
        self.port = port
        self.db = db

        self.connection_params = {
            "host": host,
            "port": port,
            "db": db,
            "password": password,
            "ssl": ssl,
            "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_connect_timeout,
            "decode_responses": False,  # Handle bytes ourselves
        }

        self.pool: Optional[redis.ConnectionPool] = None
        self.client: Optional[redis.Redis] = None
        self._connected = False

    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            self.pool = redis.ConnectionPool(max_connections=50, **self.connection_params)
            self.client = redis.Redis(connection_pool=self.pool)

            # Test connection
            await self.client.ping()
            self._connected = True

            logger.info(
                "Connected to Redis",
                host=self.host,
                port=self.port,
                db=self.db,
            )

        except Exception as e:
            logger.error("Failed to connect to Redis", error=str(e))
            raise

    async def close(self) -> None:
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            self._connected = False
            logger.info("Closed Redis connection")

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set a key-value pair.

        Args:
            key: Redis key.
            value: Value (Pydantic model, dict, or scalar).
            ttl: Time-to-live in seconds.

        Returns:
            True if successful.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            # Serialize value
            if isinstance(value, BaseModel):
                data = value.model_dump_json().encode()
            elif isinstance(value, dict):
                data = json.dumps(value).encode()
            elif isinstance(value, (str, bytes)):
                data = value if isinstance(value, bytes) else value.encode()
            else:
                data = str(value).encode()

            # Set with optional TTL
            if ttl:
                await self.client.setex(key, ttl, data)
            else:
                await self.client.set(key, data)

            logger.debug("Redis SET", key=key, ttl=ttl)
            return True

        except Exception as e:
            logger.error("Redis SET error", key=key, error=str(e))
            raise

    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key.

        Args:
            key: Redis key.

        Returns:
            Value as bytes, or None if not found.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            value = await self.client.get(key)
            if value is not None:
                logger.debug("Redis GET hit", key=key)
            else:
                logger.debug("Redis GET miss", key=key)
            return value

        except Exception as e:
            logger.error("Redis GET error", key=key, error=str(e))
            raise

    async def get_json(self, key: str) -> Optional[dict]:
        """
        Get a JSON value and deserialize it.

        Args:
            key: Redis key.

        Returns:
            Deserialized dict, or None if not found.
        """
        value = await self.get(key)
        if value is None:
            return None

        try:
            return json.loads(value.decode())
        except (json.JSONDecodeError, AttributeError):
            logger.warning("Failed to deserialize JSON", key=key)
            return None

    async def delete(self, *keys: str) -> int:
        """
        Delete one or more keys.

        Args:
            *keys: Keys to delete.

        Returns:
            Number of keys deleted.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            count = await self.client.delete(*keys)
            logger.debug("Redis DELETE", keys=keys, deleted=count)
            return count

        except Exception as e:
            logger.error("Redis DELETE error", keys=keys, error=str(e))
            raise

    async def exists(self, *keys: str) -> int:
        """
        Check if keys exist.

        Args:
            *keys: Keys to check.

        Returns:
            Number of keys that exist.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            count = await self.client.exists(*keys)
            return count

        except Exception as e:
            logger.error("Redis EXISTS error", keys=keys, error=str(e))
            raise

    async def incr(self, key: str, amount: int = 1) -> int:
        """
        Increment a numeric value.

        Args:
            key: Redis key.
            amount: Increment amount.

        Returns:
            New value.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            value = await self.client.incrby(key, amount)
            logger.debug("Redis INCR", key=key, value=value)
            return value

        except Exception as e:
            logger.error("Redis INCR error", key=key, error=str(e))
            raise

    async def lpush(self, key: str, *values: Any) -> int:
        """
        Push values to a list (left side).

        Args:
            key: Redis key.
            *values: Values to push.

        Returns:
            Length of list after push.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            # Serialize values
            serialized = []
            for v in values:
                if isinstance(v, BaseModel):
                    serialized.append(v.model_dump_json())
                elif isinstance(v, dict):
                    serialized.append(json.dumps(v))
                else:
                    serialized.append(str(v))

            length = await self.client.lpush(key, *serialized)
            logger.debug("Redis LPUSH", key=key, length=length)
            return length

        except Exception as e:
            logger.error("Redis LPUSH error", key=key, error=str(e))
            raise

    async def lrange(self, key: str, start: int = 0, end: int = -1) -> list:
        """
        Get range from a list.

        Args:
            key: Redis key.
            start: Start index.
            end: End index (-1 for all).

        Returns:
            List of values (as bytes).

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            values = await self.client.lrange(key, start, end)
            return values

        except Exception as e:
            logger.error("Redis LRANGE error", key=key, error=str(e))
            raise

    async def ttl(self, key: str) -> int:
        """
        Get time-to-live for a key.

        Args:
            key: Redis key.

        Returns:
            TTL in seconds (-1 if no expiry, -2 if not found).

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            ttl = await self.client.ttl(key)
            return ttl

        except Exception as e:
            logger.error("Redis TTL error", key=key, error=str(e))
            raise

    async def flush_db(self) -> bool:
        """
        Flush all keys in current database.

        Returns:
            True if successful.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Redis")

        try:
            await self.client.flushdb()
            logger.warning("Flushed Redis database", db=self.db)
            return True

        except Exception as e:
            logger.error("Redis FLUSHDB error", error=str(e))
            raise

    def is_connected(self) -> bool:
        """Check if connected to Redis."""
        return self._connected and self.client is not None
