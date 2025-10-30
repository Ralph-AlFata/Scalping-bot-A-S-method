"""
Unit tests for Pydantic schemas.
Tests schema validation and serialization.
"""

import pytest
from pydantic import ValidationError

from shared.schemas import (
    DepthSnapshot,
    TradeMessage,
    FeatureMessage,
    QuoteMessage,
    OrderSide,
    PositionSide,
)


@pytest.mark.unit
def test_depth_snapshot_valid():
    """Test valid DepthSnapshot."""
    depth = DepthSnapshot(
        symbol="BTCUSDT",
        timestamp_ms=1699564800000,
        exchange_timestamp_ms=1699564800000,
        bids=[[42000.0, 0.5], [41999.0, 1.0]],
        asks=[[42001.0, 0.5], [42002.0, 1.0]],
    )

    assert depth.symbol == "BTCUSDT"
    assert depth.type == "depth"
    assert len(depth.bids) == 2
    assert len(depth.asks) == 2


@pytest.mark.unit
def test_depth_snapshot_serialization():
    """Test DepthSnapshot serialization."""
    depth = DepthSnapshot(
        symbol="BTCUSDT",
        timestamp_ms=1699564800000,
        exchange_timestamp_ms=1699564800000,
        bids=[[42000.0, 0.5]],
        asks=[[42001.0, 0.5]],
    )

    json_str = depth.model_dump_json()
    assert "BTCUSDT" in json_str
    assert "depth" in json_str


@pytest.mark.unit
def test_trade_message_valid():
    """Test valid TradeMessage."""
    trade = TradeMessage(
        symbol="BTCUSDT",
        timestamp_ms=1699564800000,
        trade_id=123456,
        price=42000.0,
        quantity=0.5,
        is_buyer_maker=False,
    )

    assert trade.symbol == "BTCUSDT"
    assert trade.type == "trade"
    assert trade.price == 42000.0


@pytest.mark.unit
def test_trade_message_invalid_price():
    """Test TradeMessage with invalid price."""
    with pytest.raises(ValidationError):
        TradeMessage(
            symbol="BTCUSDT",
            timestamp_ms=1699564800000,
            trade_id=123456,
            price=-100.0,  # Invalid: negative price
            quantity=0.5,
            is_buyer_maker=False,
        )


@pytest.mark.unit
def test_order_side_enum():
    """Test OrderSide enum."""
    assert OrderSide.BUY == "BUY"
    assert OrderSide.SELL == "SELL"


@pytest.mark.unit
def test_position_side_enum():
    """Test PositionSide enum."""
    assert PositionSide.LONG == "LONG"
    assert PositionSide.SHORT == "SHORT"
    assert PositionSide.FLAT == "FLAT"


@pytest.mark.unit
def test_quote_message_complete():
    """Test complete QuoteMessage."""
    from shared.schemas import OrderSpec

    quote = QuoteMessage(
        symbol="BTCUSDT",
        timestamp_ms=1699564800000,
        bid=OrderSpec(price=41999.5, quantity=0.5),
        ask=OrderSpec(price=42000.5, quantity=0.5),
        reservation_price=42000.0,
        mid_price=42000.0,
        spread_bps=2.4,
        inventory=0.1,
        gamma=0.1,
        volatility=0.45,
    )

    assert quote.type == "quotes"
    assert quote.bid.price == 41999.5
    assert quote.ask.price == 42000.5
    assert quote.spread_bps == 2.4