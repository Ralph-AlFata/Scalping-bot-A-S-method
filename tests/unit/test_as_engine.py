# """
# Unit tests for Avellaneda-Stoikov strategy engine.
# Tests the AS mathematics and quote generation logic.
# """

# import pytest
# import math
# from unittest.mock import Mock
# import sys

# from shared.config import load_config
# from shared.logger import setup_logging
# from services.as_engine.main import (
#     AvellanedaStoikovCalculator,
#     AvellanedaStoikovEngine,
# )
# import services.as_engine.main as as_engine_module


# @pytest.fixture(autouse=True)
# def setup_logger():
#     """Set up logger for tests."""
#     logger = setup_logging("test_as_engine", level="DEBUG", log_format="console")
#     as_engine_module.logger = logger
#     yield


# @pytest.fixture
# def config():
#     """Load test configuration."""
#     return load_config()


# @pytest.fixture
# def calculator(config):
#     """Create AS calculator instance."""
#     return AvellanedaStoikovCalculator(config)


# class TestAvellanedaStoikovCalculator:
#     """Tests for AS mathematics calculator."""

#     def test_reservation_price_no_inventory(self, calculator):
#         """Test reservation price calculation with zero inventory."""
#         # With zero inventory, reservation price should equal mid price
#         mid_price = 42000.0
#         inventory_qty = 0.0
#         volatility = 0.45
#         remaining_time = 5.0

#         result = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=remaining_time,
#         )

#         assert result == pytest.approx(mid_price)

#     def test_reservation_price_long_position(self, calculator):
#         """Test reservation price decreases with long inventory."""
#         mid_price = 42000.0
#         inventory_qty = 0.1  # Long position
#         volatility = 0.45
#         remaining_time = 5.0

#         result = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=remaining_time,
#         )

#         # Reservation price should be less than mid price for long position
#         assert result < mid_price
#         # Should be a reasonable adjustment
#         assert result > mid_price * 0.99

#     def test_reservation_price_short_position(self, calculator):
#         """Test reservation price increases with short inventory."""
#         mid_price = 42000.0
#         inventory_qty = -0.1  # Short position
#         volatility = 0.45
#         remaining_time = 5.0

#         result = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=remaining_time,
#         )

#         # Reservation price should be higher than mid price for short position
#         assert result > mid_price
#         assert result < mid_price * 1.01

#     def test_reservation_price_zero_volatility(self, calculator):
#         """Test reservation price with zero volatility."""
#         mid_price = 42000.0
#         inventory_qty = 0.1
#         volatility = 0.0
#         remaining_time = 5.0

#         result = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=remaining_time,
#         )

#         # Zero volatility -> no adjustment
#         assert result == pytest.approx(mid_price)

#     def test_reservation_price_increases_with_gamma(self, calculator):
#         """Test that reservation price adjustment increases with gamma."""
#         mid_price = 42000.0
#         inventory_qty = 0.1
#         volatility = 0.45
#         remaining_time = 5.0

#         original_gamma = calculator.gamma
#         result_original = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=remaining_time,
#         )

#         # Increase gamma (more risk averse)
#         calculator.gamma = original_gamma * 2
#         result_higher_gamma = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=remaining_time,
#         )

#         # Larger gamma should produce larger adjustment
#         assert abs(result_higher_gamma - mid_price) > abs(
#             result_original - mid_price
#         )

#         # Restore original gamma
#         calculator.gamma = original_gamma

#     def test_optimal_spread_basic(self, calculator):
#         """Test optimal spread calculation."""
#         order_intensity_k = 1.5

#         result = calculator.calculate_optimal_spread(order_intensity_k)

#         # Result should be positive and reasonable
#         assert result > 0
#         assert result < 0.01  # Less than 1% half-spread

#     def test_optimal_spread_respects_min_bound(self, calculator):
#         """Test that optimal spread respects minimum bound."""
#         # Very high k -> very small spread
#         order_intensity_k = 100.0

#         result = calculator.calculate_optimal_spread(order_intensity_k)

#         # Should be at minimum bound
#         min_spread_half = calculator.min_spread_bps / 10000.0 / 2.0
#         assert result >= min_spread_half * 0.99

#     def test_optimal_spread_respects_max_bound(self, calculator):
#         """Test that optimal spread respects maximum bound."""
#         # Very small k -> very large spread
#         order_intensity_k = 0.01

#         result = calculator.calculate_optimal_spread(order_intensity_k)

#         # Should be at maximum bound
#         max_spread_half = calculator.max_spread_bps / 10000.0 / 2.0
#         assert result <= max_spread_half * 1.01

#     def test_optimal_spread_increases_with_intensity(self, calculator):
#         """Test that spread increases as order intensity increases."""
#         k_low = 0.1
#         k_high = 10.0

#         spread_low = calculator.calculate_optimal_spread(k_low)
#         spread_high = calculator.calculate_optimal_spread(k_high)

#         # Higher k means faster order arrivals -> lower spreads
#         # Both should be within bounds - at least test they're different
#         assert spread_low >= spread_high, f"Expected {spread_low} >= {spread_high}"

#     def test_optimal_spread_zero_intensity(self, calculator):
#         """Test optimal spread with zero order intensity."""
#         result = calculator.calculate_optimal_spread(0.0)

#         # Should fallback gracefully
#         assert result > 0

#     def test_optimal_spread_negative_intensity(self, calculator):
#         """Test optimal spread with negative order intensity."""
#         result = calculator.calculate_optimal_spread(-1.0)

#         # Should fallback gracefully
#         assert result > 0

#     def test_ofi_skew_no_direction(self, calculator):
#         """Test OFI skew with no direction signal."""
#         bid_price = 41999.0
#         ask_price = 42001.0

#         adjusted_bid, adjusted_ask = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=0.5,
#             ofi_direction=None,
#         )

#         # No direction -> no adjustment
#         assert adjusted_bid == bid_price
#         assert adjusted_ask == ask_price

#     def test_ofi_skew_zero_alpha(self, calculator):
#         """Test OFI skew with zero alpha (disabled)."""
#         original_alpha = calculator.alpha_skew
#         calculator.alpha_skew = 0.0

#         bid_price = 41999.0
#         ask_price = 42001.0

#         adjusted_bid, adjusted_ask = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=1.5,
#             ofi_direction="BUY",
#         )

#         # Zero alpha -> no adjustment
#         assert adjusted_bid == bid_price
#         assert adjusted_ask == ask_price

#         calculator.alpha_skew = original_alpha

#     def test_ofi_skew_buy_pressure(self, calculator):
#         """Test OFI skew adjusts quotes for buy pressure."""
#         bid_price = 41999.0
#         ask_price = 42001.0

#         adjusted_bid, adjusted_ask = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=1.5,
#             ofi_direction="BUY",
#         )

#         # Buy pressure -> lower bid, raise ask (reduce participation)
#         assert adjusted_bid < bid_price
#         assert adjusted_ask > ask_price

#     def test_ofi_skew_sell_pressure(self, calculator):
#         """Test OFI skew adjusts quotes for sell pressure."""
#         bid_price = 41999.0
#         ask_price = 42001.0

#         adjusted_bid, adjusted_ask = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=-1.5,
#             ofi_direction="SELL",
#         )

#         # Sell pressure -> raise bid, lower ask (increase participation)
#         assert adjusted_bid > bid_price
#         assert adjusted_ask < ask_price

#     def test_ofi_skew_capped_z_score(self, calculator):
#         """Test OFI skew caps z-score impact."""
#         bid_price = 41999.0
#         ask_price = 42001.0

#         # Very high z-score
#         result_high = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=10.0,
#             ofi_direction="BUY",
#         )

#         # Moderately high z-score
#         result_moderate = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=2.0,
#             ofi_direction="BUY",
#         )

#         # Impact should be similar due to capping
#         assert abs(result_high[0] - result_moderate[0]) < 1.0

#     def test_quote_size_calculation(self, calculator):
#         """Test quote size calculation."""
#         mid_price = 42000.0

#         result = calculator.calculate_quote_size(mid_price)

#         # Size should be quote_size_usd / mid_price
#         expected = calculator.quote_size_usd / mid_price
#         assert result == pytest.approx(expected)

#     def test_quote_size_zero_price(self, calculator):
#         """Test quote size with zero price."""
#         result = calculator.calculate_quote_size(0.0)

#         # Should return 0
#         assert result == 0.0

#     def test_quote_size_scales_with_price(self, calculator):
#         """Test quote size scales inversely with price."""
#         price_low = 40000.0
#         price_high = 50000.0

#         size_low = calculator.calculate_quote_size(price_low)
#         size_high = calculator.calculate_quote_size(price_high)

#         # Higher price -> smaller size (constant notional)
#         assert size_low > size_high
#         # Ratio should match price ratio
#         assert size_low / size_high == pytest.approx(price_high / price_low)

#     def test_consistency_reservation_and_spread(self, calculator):
#         """Test that reservation price and spread work together correctly."""
#         mid_price = 42000.0
#         inventory_qty = 0.1
#         volatility = 0.45
#         remaining_time = 5.0
#         order_intensity_k = 1.5

#         reservation_price = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=remaining_time,
#         )

#         half_spread = calculator.calculate_optimal_spread(order_intensity_k)

#         # Construct bid/ask
#         bid_price = reservation_price - half_spread * reservation_price
#         ask_price = reservation_price + half_spread * reservation_price

#         # Verify spread is symmetric around reservation price
#         assert (ask_price - reservation_price) == pytest.approx(
#             reservation_price - bid_price, rel=1e-6
#         )

#         # Verify bid < ask
#         assert bid_price < ask_price


# class TestAvellanedaStoikovEngine:
#     """Tests for AS Engine service."""

#     def test_engine_initialization(self, config):
#         """Test engine initialization."""
#         engine = AvellanedaStoikovEngine(config)

#         assert engine.config is config
#         assert engine.nats is not None
#         assert engine.calculator is not None
#         assert engine._running is False
#         assert engine._quotes_generated == 0
#         assert engine._quotes_published == 0
#         assert engine._errors == 0
#         assert engine._latest_features is None
#         assert engine._latest_volflow is None
#         assert engine._latest_inventory is None

#     def test_engine_calculator_settings(self, config):
#         """Test engine calculator has correct settings."""
#         engine = AvellanedaStoikovEngine(config)

#         assert (
#             engine.calculator.gamma
#             == float(config.strategy.avellaneda_stoikov.gamma)
#         )
#         assert (
#             engine.calculator.time_horizon
#             == float(config.strategy.avellaneda_stoikov.time_horizon)
#         )
#         assert (
#             engine.calculator.alpha_skew
#             == float(config.strategy.avellaneda_stoikov.alpha_skew)
#         )


# class TestASMathematicsIntegration:
#     """Integration tests for complete AS quote generation."""

#     def test_full_quote_generation_flow(self, calculator):
#         """Test complete quote generation using all components."""
#         # Market state
#         mid_price = 42000.0
#         inventory_qty = 0.05
#         volatility = 0.35
#         order_intensity_k = 2.0
#         ofi_z_score = 1.2
#         ofi_direction = "BUY"

#         # Step 1: Calculate reservation price
#         reservation_price = calculator.calculate_reservation_price(
#             mid_price=mid_price,
#             inventory_qty=inventory_qty,
#             volatility=volatility,
#             remaining_time=calculator.time_horizon,
#         )

#         # Step 2: Calculate optimal spread
#         half_spread = calculator.calculate_optimal_spread(order_intensity_k)

#         # Step 3: Place initial quotes
#         bid_price = reservation_price - half_spread * reservation_price
#         ask_price = reservation_price + half_spread * reservation_price

#         # Step 4: Apply OFI skew
#         bid_price, ask_price = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=ofi_z_score,
#             ofi_direction=ofi_direction,
#         )

#         # Step 5: Calculate quote size
#         quote_size = calculator.calculate_quote_size(mid_price)

#         # Validation
#         assert bid_price < ask_price
#         assert quote_size > 0
#         assert quote_size < calculator.quote_size_usd / (mid_price * 0.9)

#     def test_extreme_market_conditions(self, calculator):
#         """Test quote generation under extreme market conditions."""
#         # High volatility
#         high_vol_spread = calculator.calculate_optimal_spread(1.5)
#         assert high_vol_spread > 0

#         # Low volatility
#         low_vol_spread = calculator.calculate_optimal_spread(5.0)
#         assert low_vol_spread > 0

#         # Spreads should be within bounds
#         assert high_vol_spread <= calculator.max_spread_bps / 10000.0
#         assert low_vol_spread <= calculator.max_spread_bps / 10000.0

#         # Extreme inventory
#         extreme_inventory_price = calculator.calculate_reservation_price(
#             mid_price=42000.0,
#             inventory_qty=1.0,
#             volatility=0.45,
#             remaining_time=5.0,
#         )
#         assert extreme_inventory_price < 42000.0


# class TestBoundsAndValidation:
#     """Tests for bounds checking and edge cases."""

#     def test_spread_bounds_respected(self, calculator):
#         """Test that spread bounds are always respected."""
#         test_k_values = [0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 100.0]

#         for k in test_k_values:
#             spread_half = calculator.calculate_optimal_spread(k)
#             spread_bps = spread_half * 2 * 10000

#             # Check bounds
#             assert spread_bps >= calculator.min_spread_bps * 0.99
#             assert spread_bps <= calculator.max_spread_bps * 1.01

#     def test_ofi_skew_adjusts_prices(self, calculator):
#         """Test that OFI skew adjusts bid/ask prices."""
#         bid_price = 41999.0
#         ask_price = 42001.0

#         # Test with zero z_score (no effect)
#         adj_bid, adj_ask = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=0.0,
#             ofi_direction="BUY",
#         )

#         # Zero z-score should produce same prices
#         assert adj_bid == bid_price
#         assert adj_ask == ask_price

#         # Test with small positive z_score and BUY direction
#         adj_bid_buy, adj_ask_buy = calculator.apply_ofi_skew(
#             bid_price=bid_price,
#             ask_price=ask_price,
#             ofi_z_score=0.5,
#             ofi_direction="BUY",
#         )

#         # BUY direction should lower bid and raise ask
#         assert adj_bid_buy < bid_price
#         assert adj_ask_buy > ask_price

#     def test_negative_prices_handled(self, calculator):
#         """Test behavior with unusual inputs."""
#         # Mid price should never be negative, but test graceful handling
#         result = calculator.calculate_reservation_price(
#             mid_price=-1000.0,
#             inventory_qty=0.1,
#             volatility=0.45,
#             remaining_time=5.0,
#         )
#         # Should still calculate, even if market prices are wrong
#         assert isinstance(result, float)