"""
Pydantic schemas for all message types in the market-making system.
These define the contract between services via NATS.
"""

from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
from datetime import datetime

from pydantic import BaseModel, Field, validator


# ==================== Enums ====================
class OrderSide(str, Enum):
    """Order side (BUY or SELL)."""
    BUY = "BUY"
    SELL = "SELL"


class PositionSide(str, Enum):
    """Position side (LONG, SHORT, or FLAT)."""
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class OrderStatus(str, Enum):
    """Order status."""
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class AlertSeverity(str, Enum):
    """Alert severity level."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class VPINStatus(str, Enum):
    """VPIN (Volume-Synchronized Probability of Informed Trading) status."""
    NORMAL = "NORMAL"
    ELEVATED = "ELEVATED"
    TOXIC = "TOXIC"


# ==================== Market Data Messages ====================
class DepthSnapshot(BaseModel):
    """Limit order book snapshot (depth data)."""

    type: str = Field(default="depth", frozen=True)
    symbol: str = Field(..., description="Trading pair (e.g., BTCUSDT)")
    timestamp_ms: int = Field(..., description="Message timestamp in milliseconds")
    exchange_timestamp_ms: int = Field(..., description="Binance exchange timestamp")
    bids: List[Tuple[float, float]] = Field(
        ..., description="[(price, quantity), ...] - best bids first"
    )
    asks: List[Tuple[float, float]] = Field(
        ..., description="[(price, quantity), ...] - best asks first"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "type": "depth",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "exchange_timestamp_ms": 1699564800000,
                "bids": [[42000.0, 0.5], [41999.0, 1.0]],
                "asks": [[42001.0, 0.5], [42002.0, 1.0]]
            }
        }


class TradeMessage(BaseModel):
    """Market trade (aggressive order fill)."""

    type: str = Field(default="trade", frozen=True)
    symbol: str = Field(..., description="Trading pair")
    timestamp_ms: int = Field(..., description="Trade timestamp in milliseconds")
    trade_id: int = Field(..., description="Binance trade ID")
    price: float = Field(..., gt=0, description="Trade price")
    quantity: float = Field(..., gt=0, description="Trade quantity in base asset")
    is_buyer_maker: bool = Field(..., description="True if buyer is maker (sell order)")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "trade",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "trade_id": 123456,
                "price": 42000.0,
                "quantity": 0.5,
                "is_buyer_maker": False
            }
        }


# ==================== Feature Messages ====================
class OFIData(BaseModel):
    """Order Flow Imbalance (OFI) data."""

    value: float = Field(..., description="Raw OFI value")
    z_score: float = Field(..., description="Standardized OFI (z-score)")
    direction: Optional[str] = Field(None, description="'BUY' or 'SELL' if significant")

    class Config:
        json_schema_extra = {
            "example": {
                "value": 150.5,
                "z_score": 1.8,
                "direction": "BUY"
            }
        }


class QueueImbalanceData(BaseModel):
    """Queue imbalance at each depth level."""

    bid_queue: float = Field(..., description="Bid side queue depth (ticks)")
    ask_queue: float = Field(..., description="Ask side queue depth (ticks)")
    imbalance_ratio: float = Field(..., description="bid_queue / ask_queue")

    class Config:
        json_schema_extra = {
            "example": {
                "bid_queue": 50.0,
                "ask_queue": 30.0,
                "imbalance_ratio": 1.67
            }
        }


class FeatureMessage(BaseModel):
    """Computed market features (input for strategy)."""

    type: str = Field(default="features", frozen=True)
    symbol: str
    timestamp_ms: int
    mid_price: float = Field(..., gt=0, description="(best_bid + best_ask) / 2")
    micro_price: float = Field(..., gt=0, description="Weighted mid-price using volumes")
    spread_bps: float = Field(..., ge=0, description="Spread in basis points")
    best_bid: float = Field(..., gt=0)
    best_ask: float = Field(..., gt=0)
    best_bid_qty: float = Field(..., gt=0)
    best_ask_qty: float = Field(..., gt=0)
    ofi: OFIData
    queue_imbalance: QueueImbalanceData

    class Config:
        json_schema_extra = {
            "example": {
                "type": "features",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "mid_price": 42000.5,
                "micro_price": 42000.3,
                "spread_bps": 2.4,
                "best_bid": 42000.0,
                "best_ask": 42001.0,
                "best_bid_qty": 1.5,
                "best_ask_qty": 2.0,
                "ofi": {"value": 150.5, "z_score": 1.8, "direction": "BUY"},
                "queue_imbalance": {"bid_queue": 50.0, "ask_queue": 30.0, "imbalance_ratio": 1.67}
            }
        }


# ==================== Volatility & Order Intensity ====================
class VolatilityData(BaseModel):
    """Volatility estimation."""

    value: float = Field(..., ge=0, description="Realized volatility (annualized)")
    confidence: float = Field(..., ge=0, le=1, description="Confidence in estimate (0-1)")

    class Config:
        json_schema_extra = {
            "example": {
                "value": 0.45,
                "confidence": 0.85
            }
        }


class OrderIntensityData(BaseModel):
    """Order arrival intensity (Poisson intensity parameter)."""

    k: float = Field(..., ge=0, description="Exponential decay parameter")
    confidence: float = Field(..., ge=0, le=1)

    class Config:
        json_schema_extra = {
            "example": {
                "k": 1.5,
                "confidence": 0.9
            }
        }


class VPINData(BaseModel):
    """Volume-Synchronized Probability of Informed Trading."""

    value: float = Field(..., ge=0, le=1, description="VPIN value (0-1)")
    status: VPINStatus = Field(..., description="Normal/Elevated/Toxic")

    class Config:
        json_schema_extra = {
            "example": {
                "value": 0.65,
                "status": "ELEVATED"
            }
        }


class VolflowMessage(BaseModel):
    """Volatility and order flow metrics."""

    type: str = Field(default="volflow", frozen=True)
    symbol: str
    timestamp_ms: int
    volatility: VolatilityData
    order_intensity: OrderIntensityData
    vpin: VPINData

    class Config:
        json_schema_extra = {
            "example": {
                "type": "volflow",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "volatility": {"value": 0.45, "confidence": 0.85},
                "order_intensity": {"k": 1.5, "confidence": 0.9},
                "vpin": {"value": 0.65, "status": "ELEVATED"}
            }
        }


# ==================== Quote Messages ====================
class OrderSpec(BaseModel):
    """Order specification (price + quantity)."""

    price: float = Field(..., gt=0)
    quantity: float = Field(..., gt=0)

    class Config:
        json_schema_extra = {
            "example": {
                "price": 42000.0,
                "quantity": 0.5
            }
        }


class QuoteMessage(BaseModel):
    """Optimal quotes generated by strategy."""

    type: str = Field(default="quotes", frozen=True)
    symbol: str
    timestamp_ms: int
    bid: OrderSpec = Field(..., description="Bid quote (our buy order)")
    ask: OrderSpec = Field(..., description="Ask quote (our sell order)")

    # Metadata for analysis
    reservation_price: float = Field(..., description="Fair value adjusted for inventory")
    mid_price: float = Field(..., description="Market mid-price")
    spread_bps: float = Field(..., ge=0, description="Quote spread in basis points")
    inventory: float = Field(..., description="Current inventory (qty)")
    gamma: float = Field(..., description="Risk aversion parameter used")
    volatility: float = Field(..., description="Volatility used in calculation")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "quotes",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "bid": {"price": 41999.5, "quantity": 0.5},
                "ask": {"price": 42000.5, "quantity": 0.5},
                "reservation_price": 42000.0,
                "mid_price": 42000.0,
                "spread_bps": 2.4,
                "inventory": 0.1,
                "gamma": 0.1,
                "volatility": 0.45
            }
        }


# ==================== Order Execution Messages ====================
class FillMessage(BaseModel):
    """Order fill notification."""

    type: str = Field(default="fill", frozen=True)
    symbol: str
    timestamp_ms: int
    order_id: str = Field(..., description="Our order ID")
    side: OrderSide
    price: float = Field(..., gt=0)
    quantity: float = Field(..., gt=0)
    commission: float = Field(..., ge=0, description="Trading fee")
    commission_asset: str = Field(default="USDT")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "fill",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "order_id": "order_123",
                "side": "BUY",
                "price": 42000.0,
                "quantity": 0.5,
                "commission": 5.0,
                "commission_asset": "USDT"
            }
        }


class OrderCancelledMessage(BaseModel):
    """Order cancellation notification."""

    type: str = Field(default="cancelled", frozen=True)
    symbol: str
    timestamp_ms: int
    order_id: str
    side: OrderSide

    class Config:
        json_schema_extra = {
            "example": {
                "type": "cancelled",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "order_id": "order_123",
                "side": "BUY"
            }
        }


# ==================== Inventory Messages ====================
class PnLData(BaseModel):
    """Profit and loss metrics."""

    realized_pnl: float = Field(..., description="Realized P&L from closed positions")
    unrealized_pnl: float = Field(..., description="Unrealized P&L from open position")
    total_pnl: float = Field(..., description="Total P&L (realized + unrealized)")

    class Config:
        json_schema_extra = {
            "example": {
                "realized_pnl": 125.50,
                "unrealized_pnl": -50.25,
                "total_pnl": 75.25
            }
        }


class InventoryMessage(BaseModel):
    """Current position and P&L."""

    type: str = Field(default="inventory", frozen=True)
    symbol: str
    timestamp_ms: int
    position: PositionSide
    quantity: float = Field(..., description="Position size (positive = long)")
    entry_price: Optional[float] = Field(None, description="Average entry price")
    mark_price: float = Field(..., description="Current mark price")
    pnl: PnLData

    # Risk metrics
    inventory_utilization: float = Field(..., description="Current / max inventory ratio (0-1)")
    position_notional_usd: float = Field(..., description="Position notional value")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "inventory",
                "symbol": "BTCUSDT",
                "timestamp_ms": 1699564800000,
                "position": "LONG",
                "quantity": 0.5,
                "entry_price": 41900.0,
                "mark_price": 42000.0,
                "pnl": {
                    "realized_pnl": 125.50,
                    "unrealized_pnl": 50.0,
                    "total_pnl": 175.50
                },
                "inventory_utilization": 0.25,
                "position_notional_usd": 21000.0
            }
        }


# ==================== Risk & Alert Messages ====================
class AlertMessage(BaseModel):
    """Risk alert or system notification."""

    type: str = Field(default="alert", frozen=True)
    timestamp_ms: int
    severity: AlertSeverity
    service: str = Field(..., description="Service that generated alert")
    message: str = Field(..., description="Alert description")
    context: Dict[str, Any] = Field(default_factory=dict, description="Additional context")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "alert",
                "timestamp_ms": 1699564800000,
                "severity": "WARNING",
                "service": "risk_manager",
                "message": "Position exceeds max inventory limit",
                "context": {
                    "current_inventory": 0.5,
                    "max_inventory": 0.02
                }
            }
        }


class HealthCheckMessage(BaseModel):
    """Service health check."""

    type: str = Field(default="health", frozen=True)
    service_name: str
    timestamp_ms: int
    healthy: bool
    uptime_sec: float = Field(..., description="Uptime in seconds")
    errors_count: int = Field(default=0)
    messages_processed: int = Field(default=0)
    last_message_timestamp_ms: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "type": "health",
                "service_name": "marketdata_gw",
                "timestamp_ms": 1699564800000,
                "healthy": True,
                "uptime_sec": 3600.5,
                "errors_count": 0,
                "messages_processed": 15000
            }
        }


# ==================== Metrics Messages ====================
class MetricsSnapshot(BaseModel):
    """Service performance metrics."""

    type: str = Field(default="metrics", frozen=True)
    service_name: str
    timestamp_ms: int

    # Processing metrics
    messages_received: int
    messages_published: int
    messages_failed: int
    avg_latency_ms: float = Field(..., ge=0)
    p99_latency_ms: float = Field(..., ge=0)

    # Resource metrics
    memory_mb: float = Field(..., ge=0)
    cpu_percent: float = Field(..., ge=0, le=100)
    active_connections: int = Field(..., ge=0)

    class Config:
        json_schema_extra = {
            "example": {
                "type": "metrics",
                "service_name": "features_svc",
                "timestamp_ms": 1699564800000,
                "messages_received": 15000,
                "messages_published": 15000,
                "messages_failed": 0,
                "avg_latency_ms": 5.2,
                "p99_latency_ms": 12.5,
                "memory_mb": 128.5,
                "cpu_percent": 45.2,
                "active_connections": 5
            }
        }


# ==================== Message Union Type ====================
AnyMessage = (
    DepthSnapshot
    | TradeMessage
    | FeatureMessage
    | VolflowMessage
    | QuoteMessage
    | FillMessage
    | OrderCancelledMessage
    | InventoryMessage
    | AlertMessage
    | HealthCheckMessage
    | MetricsSnapshot
)
