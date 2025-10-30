"""
Avellaneda-Stoikov Strategy Engine.
Generates optimal bid/ask quotes using the AS model.

Phase 1: Infrastructure scaffold.
Phase 3: Implement AS mathematics.

Core AS Mathematics:
1. Reservation Price: r(t) = s(t) - q·γ·σ²·(T-t)
   - Adjusts fair value for current inventory position
2. Optimal Spread: δ* = (1/γ) · ln(1 + γ/k)
   - Based on risk aversion and order intensity
3. Quote Placement: bid = r(t) - δ*, ask = r(t) + δ*
4. OFI Skew: Adjust quotes based on order flow imbalance
"""

import asyncio
import signal
import math
import time
from typing import Optional

from shared.nats_client import NATSClient
from shared.config import load_config
from shared.logger import setup_logging, get_logger
from shared.schemas import (
    FeatureMessage,
    VolflowMessage,
    InventoryMessage,
    QuoteMessage,
    OrderSpec,
)

logger: Optional[object] = None


class AvellanedaStoikovCalculator:
    """Implements AS mathematics for quote generation."""

    def __init__(self, config):
        """Initialize calculator with strategy parameters."""
        self.config = config
        # Convert config values to float (handle both string and numeric types)
        self.gamma = float(config.strategy.avellaneda_stoikov.gamma)
        self.time_horizon = float(
            config.strategy.avellaneda_stoikov.time_horizon
        )
        self.alpha_skew = float(config.strategy.avellaneda_stoikov.alpha_skew)

        self.min_spread_bps = float(config.strategy.quoting.min_spread_bps)
        self.max_spread_bps = float(config.strategy.quoting.max_spread_bps)
        self.quote_size_usd = float(config.strategy.quoting.quote_size_usd)

    def calculate_reservation_price(
        self,
        mid_price: float,
        inventory_qty: float,
        volatility: float,
        remaining_time: float,
    ) -> float:
        """
        Calculate reservation price adjusted for inventory.

        Formula: r(t) = s(t) - q·γ·σ²·(T-t)

        Args:
            mid_price: Current market mid-price
            inventory_qty: Current inventory (positive = long)
            volatility: Realized volatility (annualized)
            remaining_time: Time remaining (seconds)

        Returns:
            Reservation price (fair value adjusted for inventory)
        """
        if volatility <= 0 or remaining_time <= 0:
            return mid_price

        # Inventory adjustment term
        inventory_adjustment = (
            inventory_qty * self.gamma * (volatility ** 2) * remaining_time
        )

        reservation_price = mid_price - inventory_adjustment

        logger.debug(
            "Reservation price calculated",
            mid_price=mid_price,
            inventory_qty=inventory_qty,
            volatility=volatility,
            remaining_time=remaining_time,
            inventory_adjustment=inventory_adjustment,
            reservation_price=reservation_price,
        )

        return reservation_price

    def calculate_optimal_spread(self, order_intensity_k: float) -> float:
        """
        Calculate optimal spread using AS formula.

        Formula: δ* = (1/γ) · ln(1 + γ/k)

        Args:
            order_intensity_k: Poisson order intensity parameter

        Returns:
            Optimal half-spread (relative to mid-price)
        """
        if order_intensity_k <= 0:
            # Fallback to minimum spread if k is invalid
            return self.min_spread_bps / 10000.0

        try:
            # Optimal spread formula
            spread = (1.0 / self.gamma) * math.log(1.0 + self.gamma / order_intensity_k)

            # Convert to bps and apply bounds
            spread_bps = max(
                self.min_spread_bps,
                min(self.max_spread_bps, spread * 10000),
            )

            half_spread = spread_bps / 10000.0 / 2.0

            logger.debug(
                "Optimal spread calculated",
                order_intensity_k=order_intensity_k,
                spread_bps=spread_bps,
                half_spread=half_spread,
            )

            return half_spread
        except (ValueError, ZeroDivisionError) as e:
            logger.warning(
                "Error calculating optimal spread, using min spread",
                error=str(e),
                order_intensity_k=order_intensity_k,
            )
            return self.min_spread_bps / 10000.0 / 2.0

    def apply_ofi_skew(
        self,
        bid_price: float,
        ask_price: float,
        ofi_z_score: float,
        ofi_direction: Optional[str],
    ) -> tuple[float, float]:
        """
        Adjust bid/ask quotes based on OFI signal.

        Logic:
        - Positive OFI (BUY pressure): lower bid, raise ask (reduce participation)
        - Negative OFI (SELL pressure): raise bid, lower ask (increase participation)

        Args:
            bid_price: Current bid price
            ask_price: Current ask price
            ofi_z_score: Normalized OFI z-score
            ofi_direction: 'BUY' or 'SELL' if significant

        Returns:
            Tuple of (adjusted_bid, adjusted_ask)
        """
        if ofi_direction is None or self.alpha_skew <= 0:
            return bid_price, ask_price

        mid_price = (bid_price + ask_price) / 2.0

        # Normalize z-score impact (cap at ±2 for stability)
        z_capped = max(-2.0, min(2.0, ofi_z_score))

        if ofi_direction == "BUY":
            # Positive OFI: reduce bid participation, increase ask
            bid_adjustment = -self.alpha_skew * abs(z_capped) * mid_price
            ask_adjustment = self.alpha_skew * abs(z_capped) * mid_price
        else:  # SELL
            # Negative OFI: increase bid participation, reduce ask
            bid_adjustment = self.alpha_skew * abs(z_capped) * mid_price
            ask_adjustment = -self.alpha_skew * abs(z_capped) * mid_price

        adjusted_bid = bid_price + bid_adjustment
        adjusted_ask = ask_price + ask_adjustment

        logger.debug(
            "OFI skew applied",
            ofi_direction=ofi_direction,
            ofi_z_score=ofi_z_score,
            bid_adjustment=bid_adjustment,
            ask_adjustment=ask_adjustment,
            adjusted_bid=adjusted_bid,
            adjusted_ask=adjusted_ask,
        )

        return adjusted_bid, adjusted_ask

    def calculate_quote_size(self, mid_price: float) -> float:
        """
        Calculate quote size in base asset quantity.

        Args:
            mid_price: Current market mid-price

        Returns:
            Quantity to quote (in base asset, e.g., BTC)
        """
        if mid_price <= 0:
            return 0.0

        quantity = self.quote_size_usd / mid_price

        logger.debug(
            "Quote size calculated",
            quote_size_usd=self.quote_size_usd,
            mid_price=mid_price,
            quantity=quantity,
        )

        return quantity


class AvellanedaStoikovEngine:
    """Avellaneda-Stoikov quote generation engine."""

    def __init__(self, config):
        """Initialize service."""
        self.config = config
        self.nats = NATSClient(config.infrastructure.nats.url)
        self.calculator = AvellanedaStoikovCalculator(config)
        self._running = False

        # State tracking
        self._quotes_generated = 0
        self._quotes_published = 0
        self._errors = 0

        # Current market state
        self._latest_features: Optional[FeatureMessage] = None
        self._latest_volflow: Optional[VolflowMessage] = None
        self._latest_inventory: Optional[InventoryMessage] = None

        # Quote publishing throttle
        self._last_quote_time = 0.0
        self._quote_interval_ms = float(config.strategy.quoting.update_freq_ms)

    async def start(self) -> None:
        """Start service."""
        logger.info("Starting AvellanedaStoikovEngine")
        try:
            await self.nats.connect()

            # Subscribe to inputs
            await self.nats.subscribe("features.v1", self.on_features)
            await self.nats.subscribe("volflow.v1", self.on_volflow)
            await self.nats.subscribe("inventory.v1", self.on_inventory)

            self._running = True
            logger.info(
                "AvellanedaStoikovEngine ready",
                gamma=self.calculator.gamma,
                time_horizon=self.calculator.time_horizon,
                alpha_skew=self.calculator.alpha_skew,
            )

        except Exception as e:
            logger.error("Failed to start AS Engine", error=str(e))
            raise

    async def on_features(self, msg) -> None:
        """Handle features update and potentially generate quotes."""
        try:
            # Parse features message
            self._latest_features = FeatureMessage.model_validate_json(msg.data)
            logger.debug(
                "Features received",
                symbol=self._latest_features.symbol,
                micro_price=self._latest_features.micro_price,
                spread_bps=self._latest_features.spread_bps,
            )

            # Try to generate quotes if we have all required data
            await self._try_generate_quotes()

        except Exception as e:
            self._errors += 1
            logger.error("Error processing features message", error=str(e))

    async def on_volflow(self, msg) -> None:
        """Handle volflow update and potentially generate quotes."""
        try:
            # Parse volflow message
            self._latest_volflow = VolflowMessage.model_validate_json(msg.data)
            logger.debug(
                "Volflow received",
                symbol=self._latest_volflow.symbol,
                volatility=self._latest_volflow.volatility.value,
                order_intensity_k=self._latest_volflow.order_intensity.k,
                vpin_status=self._latest_volflow.vpin.status,
            )

            # Try to generate quotes if we have all required data
            await self._try_generate_quotes()

        except Exception as e:
            self._errors += 1
            logger.error("Error processing volflow message", error=str(e))

    async def on_inventory(self, msg) -> None:
        """Handle inventory update and potentially generate quotes."""
        try:
            # Parse inventory message
            self._latest_inventory = InventoryMessage.model_validate_json(msg.data)
            logger.debug(
                "Inventory received",
                symbol=self._latest_inventory.symbol,
                position=self._latest_inventory.position,
                quantity=self._latest_inventory.quantity,
                pnl=self._latest_inventory.pnl.total_pnl,
            )

            # Try to generate quotes if we have all required data
            await self._try_generate_quotes()

        except Exception as e:
            self._errors += 1
            logger.error("Error processing inventory message", error=str(e))

    async def _try_generate_quotes(self) -> None:
        """Generate and publish quotes if all state is available and throttle allows."""
        # Check if all required state is available
        if (
            self._latest_features is None
            or self._latest_volflow is None
            or self._latest_inventory is None
        ):
            logger.debug("Waiting for all state before generating quotes")
            return

        # Check throttling
        current_time_ms = time.time_ns() // 1_000_000
        if current_time_ms - self._last_quote_time < self._quote_interval_ms:
            return

        try:
            quote = await self._generate_quote()
            if quote:
                await self.nats.publish("quotes.v1", quote.model_dump_json().encode())
                self._quotes_published += 1
                self._last_quote_time = current_time_ms
                logger.info(
                    "Quote published",
                    bid_price=quote.bid.price,
                    ask_price=quote.ask.price,
                    spread_bps=quote.spread_bps,
                )
        except Exception as e:
            self._errors += 1
            logger.error("Error generating quote", error=str(e))

    async def _generate_quote(self) -> Optional[QuoteMessage]:
        """Generate a single quote using AS mathematics."""
        features = self._latest_features
        volflow = self._latest_volflow
        inventory = self._latest_inventory

        # Extract current state
        mid_price = features.micro_price
        inventory_qty = inventory.quantity
        volatility = volflow.volatility.value
        order_intensity_k = volflow.order_intensity.k

        # Ensure volatility has minimum confidence
        if volflow.volatility.confidence < 0.1:
            logger.warning(
                "Low confidence in volatility, using minimum spread",
                confidence=volflow.volatility.confidence,
            )

        # Calculate reservation price
        reservation_price = self.calculator.calculate_reservation_price(
            mid_price=mid_price,
            inventory_qty=inventory_qty,
            volatility=volatility,
            remaining_time=self.calculator.time_horizon,
        )

        # Calculate optimal spread (half-spread on each side)
        half_spread = self.calculator.calculate_optimal_spread(order_intensity_k)

        # Initial bid/ask
        bid_price = reservation_price - half_spread * reservation_price
        ask_price = reservation_price + half_spread * reservation_price

        # Apply OFI-based skew adjustment
        bid_price, ask_price = self.calculator.apply_ofi_skew(
            bid_price=bid_price,
            ask_price=ask_price,
            ofi_z_score=features.ofi.z_score,
            ofi_direction=features.ofi.direction,
        )

        # Ensure bid < ask
        if bid_price >= ask_price:
            logger.warning(
                "Invalid quote prices after adjustment, swapping",
                bid_price=bid_price,
                ask_price=ask_price,
            )
            bid_price, ask_price = ask_price, bid_price

        # Calculate quote size
        quote_size = self.calculator.calculate_quote_size(mid_price)

        # Calculate final spread in bps
        final_spread_bps = (ask_price - bid_price) / mid_price * 10000

        # Create quote message
        quote = QuoteMessage(
            symbol=features.symbol,
            timestamp_ms=features.timestamp_ms,
            bid=OrderSpec(price=bid_price, quantity=quote_size),
            ask=OrderSpec(price=ask_price, quantity=quote_size),
            reservation_price=reservation_price,
            mid_price=mid_price,
            spread_bps=final_spread_bps,
            inventory=inventory_qty,
            gamma=self.calculator.gamma,
            volatility=volatility,
        )

        self._quotes_generated += 1

        return quote

    async def stop(self) -> None:
        """Stop service."""
        logger.info("Stopping AvellanedaStoikovEngine")
        self._running = False
        await self.nats.close()
        logger.info(
            "AvellanedaStoikovEngine stopped",
            quotes_generated=self._quotes_generated,
            quotes_published=self._quotes_published,
            errors=self._errors,
        )

    async def run(self) -> None:
        """Main service loop."""
        try:
            await self.start()
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Service cancelled")
        except Exception as e:
            logger.error("Service error", error=str(e))
        finally:
            await self.stop()


async def main() -> None:
    """Main entry point."""
    global logger
    config = load_config()
    logger = setup_logging(
        "as_engine",
        level=config.system.log_level,
        log_format=config.system.log_format,
    )

    logger.info("Initializing AvellanedaStoikovEngine")

    service = AvellanedaStoikovEngine(config)

    def signal_handler(sig, frame):
        logger.warning("Received signal", signal=sig)
        asyncio.create_task(service.stop())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())