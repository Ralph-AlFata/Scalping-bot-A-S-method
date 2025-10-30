"""
Initialize TimescaleDB with hypertables and schema.
Run this after docker-compose up to set up the database.

Usage:
    python scripts/setup_db.py --host localhost --port 5432 --user postgres --password marketmaker_dev --db marketmaker
"""

import asyncio
import argparse
from pathlib import Path

import asyncpg


async def setup_database(
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "marketmaker_dev",
    database: str = "marketmaker",
) -> None:
    """Create TimescaleDB hypertables and schema."""

    # Connect to database
    conn = await asyncpg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )

    try:
        print(f"Connected to {database}@{host}:{port}")

        # Enable TimescaleDB extension
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        print("✓ TimescaleDB extension enabled")

        # Create ticks table (OHLCV data)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                bid_price DECIMAL NOT NULL,
                ask_price DECIMAL NOT NULL,
                bid_volume DECIMAL NOT NULL,
                ask_volume DECIMAL NOT NULL,
                mid_price DECIMAL NOT NULL,
                spread_bps DECIMAL NOT NULL
            );
        """)
        print("✓ Created ticks table")

        # Create hypertable for ticks
        await conn.execute(
            "SELECT create_hypertable('ticks', 'timestamp', if_not_exists => true);"
        )
        print("✓ Created ticks hypertable")

        # Create index on ticks
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticks_symbol_timestamp
            ON ticks (symbol, timestamp DESC);
        """)
        print("✓ Created ticks index")

        # Create trades table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                trade_id BIGINT NOT NULL,
                price DECIMAL NOT NULL,
                quantity DECIMAL NOT NULL,
                is_buyer_maker BOOLEAN NOT NULL
            );
        """)
        print("✓ Created trades table")

        # Create hypertable for trades
        await conn.execute(
            "SELECT create_hypertable('trades', 'timestamp', if_not_exists => true);"
        )
        print("✓ Created trades hypertable")

        # Create index on trades
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_symbol_timestamp
            ON trades (symbol, timestamp DESC);
        """)
        print("✓ Created trades index")

        # Create position snapshots table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS position_snapshots (
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                position DECIMAL NOT NULL,
                entry_price DECIMAL,
                realized_pnl DECIMAL NOT NULL,
                unrealized_pnl DECIMAL NOT NULL,
                total_pnl DECIMAL NOT NULL
            );
        """)
        print("✓ Created position_snapshots table")

        # Create hypertable for position snapshots
        await conn.execute(
            "SELECT create_hypertable('position_snapshots', 'timestamp', if_not_exists => true);"
        )
        print("✓ Created position_snapshots hypertable")

        # Create order events table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS order_events (
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                order_id TEXT NOT NULL,
                side TEXT NOT NULL,
                price DECIMAL NOT NULL,
                quantity DECIMAL NOT NULL,
                status TEXT NOT NULL,
                fill_quantity DECIMAL DEFAULT 0,
                commission DECIMAL DEFAULT 0
            );
        """)
        print("✓ Created order_events table")

        # Create hypertable for order events
        await conn.execute(
            "SELECT create_hypertable('order_events', 'timestamp', if_not_exists => true);"
        )
        print("✓ Created order_events hypertable")

        # Create index on order events
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_order_events_symbol_timestamp
            ON order_events (symbol, timestamp DESC);
        """)
        print("✓ Created order_events index")

        # Set retention policies (TimescaleDB native)
        await conn.execute("""
            SELECT add_retention_policy('ticks', INTERVAL '30 days', if_not_exists => true);
        """)
        print("✓ Set ticks retention to 30 days")

        await conn.execute("""
            SELECT add_retention_policy('trades', INTERVAL '7 days', if_not_exists => true);
        """)
        print("✓ Set trades retention to 7 days")

        await conn.execute("""
            SELECT add_retention_policy('order_events', INTERVAL '90 days', if_not_exists => true);
        """)
        print("✓ Set order_events retention to 90 days")

        print("\n✅ Database setup complete!")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        await conn.close()


def main() -> None:
    """Parse arguments and run setup."""
    parser = argparse.ArgumentParser(
        description="Initialize TimescaleDB for market maker"
    )
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument(
        "--password", default="marketmaker_dev", help="Database password"
    )
    parser.add_argument("--db", default="marketmaker", help="Database name")

    args = parser.parse_args()

    asyncio.run(
        setup_database(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.db,
        )
    )


if __name__ == "__main__":
    main()