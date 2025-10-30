"""
Async PostgreSQL/TimescaleDB client wrapper.
Provides connection pooling and common operations.
"""

from typing import Any, List, Optional, Tuple

import asyncpg
from asyncpg import Pool, Connection

from shared.logger import get_logger

logger = get_logger(__name__)


class DBClient:
    """Async TimescaleDB client wrapper."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "password",
        database: str = "marketmaker",
        min_size: int = 2,
        max_size: int = 10,
        ssl: bool = False,
    ):
        """
        Initialize database client.

        Args:
            host: Database host.
            port: Database port.
            user: Database user.
            password: Database password.
            database: Database name.
            min_size: Minimum pool size.
            max_size: Maximum pool size.
            ssl: Use SSL connection.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.min_size = min_size
        self.max_size = max_size
        self.ssl = ssl

        self.pool: Optional[Pool] = None
        self._connected = False

    async def connect(self) -> None:
        """Create connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                min_size=self.min_size,
                max_size=self.max_size,
                ssl=self.ssl,
            )

            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")

            self._connected = True

            logger.info(
                "Connected to TimescaleDB",
                host=self.host,
                port=self.port,
                database=self.database,
            )

        except Exception as e:
            logger.error("Failed to connect to TimescaleDB", error=str(e))
            raise

    async def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self._connected = False
            logger.info("Closed TimescaleDB connection")

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        """
        Execute a query (INSERT, UPDATE, DELETE).

        Args:
            query: SQL query.
            *args: Query parameters.

        Returns:
            Result status message.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *args)
            logger.debug("Query executed", query=query[:50])
            return result

        except Exception as e:
            logger.error("Query execution error", query=query[:50], error=str(e))
            raise

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> List[asyncpg.Record]:
        """
        Fetch multiple rows.

        Args:
            query: SQL query.
            *args: Query parameters.

        Returns:
            List of records.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query, *args)
            logger.debug("Query fetched", query=query[:50], count=len(records))
            return records

        except Exception as e:
            logger.error("Query fetch error", query=query[:50], error=str(e))
            raise

    async def fetchrow(
        self,
        query: str,
        *args: Any,
    ) -> Optional[asyncpg.Record]:
        """
        Fetch single row.

        Args:
            query: SQL query.
            *args: Query parameters.

        Returns:
            Single record or None.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                record = await conn.fetchrow(query, *args)
            return record

        except Exception as e:
            logger.error("Query fetchrow error", query=query[:50], error=str(e))
            raise

    async def fetchval(
        self,
        query: str,
        *args: Any,
    ) -> Any:
        """
        Fetch single value.

        Args:
            query: SQL query.
            *args: Query parameters.

        Returns:
            Single value or None.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                value = await conn.fetchval(query, *args)
            return value

        except Exception as e:
            logger.error("Query fetchval error", query=query[:50], error=str(e))
            raise

    async def executemany(
        self,
        query: str,
        args: List[Tuple[Any, ...]],
    ) -> None:
        """
        Execute multiple statements efficiently.

        Args:
            query: SQL query with placeholders.
            args: List of parameter tuples.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    for row_args in args:
                        await conn.execute(query, *row_args)

            logger.debug("Batch query executed", count=len(args))

        except Exception as e:
            logger.error("Batch query error", error=str(e))
            raise

    async def transaction(self) -> Connection:
        """
        Get connection for transaction.

        Usage:
            async with db.transaction() as conn:
                await conn.execute("...")

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.pool:
            raise RuntimeError("Not connected to database")

        return self.pool.acquire()

    def is_connected(self) -> bool:
        """Check if connected to database."""
        return self._connected and self.pool is not None
