"""VenueGrid round-trip tests."""

import math
import pytest
from dlmm_bot.grid import VenueGrid

class TestVenueGrid:
    @pytest.fixture
    def grid(self):
        return VenueGrid(
            ref_price=150.0,
            bin_step_bps=2,
            base_decimals=6,
            quote_decimals=9,
        )

    def test_price_from_bin_known(self, grid):
        """price_at_bin(0) must equal ref_price."""
        assert grid.price_from_bin(0) == 150.0

    def test_price_from_bin_positive(self, grid):
        """price_at_bin(n) > ref for n > 0."""
        p = grid.price_from_bin(50)
        expected = 150.0 * (1 + 2 / 1e4) ** 50
        assert abs(p - expected) < 1e-9

    def test_price_from_bin_negative(self, grid):
        """price_at_bin(n) < ref for n < 0."""
        p = grid.price_from_bin(-30)
        expected = 150.0 * (1 + 2 / 1e4) ** -30
        assert abs(p - expected) < 1e-9

    def test_round_trip_small_bin(self, grid):
        """bin_from_price(price_from_bin(n)) == n for small bin ids."""
        for n in [-10, 0, 10, 50]:
            p = grid.price_from_bin(n)
            assert grid.bin_from_price(p) == n

    def test_round_trip_large_bin(self, grid):
        """Round-trip holds for larger bin indices."""
        for n in [-200, 200, 500, -500]:
            p = grid.price_from_bin(n)
            assert grid.bin_from_price(p) == n

    def test_round_trip_via_price(self, grid):
        """Arbitrary price → bin → price is close to original."""
        for price in [100.0, 150.0, 200.0, 50.0, 300.0]:
            bin_id = grid.bin_from_price(price)
            recovered = grid.price_from_bin(bin_id)
            # Within one bin-width of original price
            expected_error = grid._step() - 1  # relative step
            assert abs(recovered - price) / price < expected_error + 1e-9

    def test_bin_zero_at_ref(self, grid):
        """bin_from_price(ref_price) == 0."""
        assert grid.bin_from_price(150.0) == 0

    def test_decimal_conversion_round_trip_base(self, grid):
        """Raw → decimal → raw for base token."""
        raw = 1_000_000_000
        dec = grid.to_decimal(raw, "base")
        back = grid.to_raw(dec, "base")
        assert back == raw

    def test_decimal_conversion_round_trip_quote(self, grid):
        """Raw → decimal → raw for quote token."""
        raw = 500_000_000_000
        dec = grid.to_decimal(raw, "quote")
        back = grid.to_raw(dec, "quote")
        assert back == raw

    def test_invalid_price(self, grid):
        """Negative or zero price raises."""
        with pytest.raises(ValueError):
            grid.bin_from_price(-1.0)
        with pytest.raises(ValueError):
            grid.bin_from_price(0.0)
