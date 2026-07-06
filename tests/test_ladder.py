"""Ladder placement tests: center shift, spread clamping, skew behavior."""

import pytest
from dlmm_bot.grid import VenueGrid
from dlmm_bot.ladder import build_ladder, LadderConfig, LadderLevel

@pytest.fixture
def grid():
    return VenueGrid(
        ref_price=150.0,
        bin_step_bps=2,
        base_decimals=6,
        quote_decimals=9,
    )

class TestCenterShift:
    """Ladder center shifts with AS reservation price."""

    def test_no_tilt_center_equals_active(self, grid):
        """When r == S, center == active_bin."""
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0)
        bids = [l for l in levels if l.side == "bid"]
        asks = [l for l in levels if l.side == "ask"]
        # Mid bin between innermost bid and ask should equal active_bin
        mid = (bids[0].bin_id + asks[-1].bin_id) / 2
        assert mid == 100.0

    def test_positive_tilt_shifts_center_up(self, grid):
        """r > S tilts center higher (long inventory → lower quotes)."""
        levels = build_ladder(grid, active_bin=100, r=151.0, S=150.0, half_spread=0.01, skew=0.0)
        bids = [l for l in levels if l.side == "bid"]
        mid = (bids[0].bin_id + bids[0].bin_id) / 2  # not needed, check bids moved
        # All bids should be at higher bin ids than no-tilt case
        levels_no_tilt = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0)
        bids_no_tilt = [l for l in levels_no_tilt if l.side == "bid"]
        assert all(b.bin_id > bnt.bin_id for b, bnt in zip(bids, bids_no_tilt))

    def test_negative_tilt_shifts_center_down(self, grid):
        """r < S tilts center lower."""
        levels = build_ladder(grid, active_bin=100, r=149.0, S=150.0, half_spread=0.01, skew=0.0)
        levels_no_tilt = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0)
        bids = [l for l in levels if l.side == "bid"]
        bids_no_tilt = [l for l in levels_no_tilt if l.side == "bid"]
        assert all(b.bin_id < bnt.bin_id for b, bnt in zip(bids, bids_no_tilt))

class TestCenterShiftMagnitude:
    """The tilt is a PRICE distance; the shift must be in log-price bins.

    Guards against dividing (r − S) by log(step) directly, which inflates
    the shift by a factor of ~S.
    """

    def test_one_dollar_tilt_on_150(self, grid):
        """r=151 on S=150 with 2 bps bins ≈ log(151/150)/log(1.0002) ≈ 33 bins."""
        levels = build_ladder(grid, active_bin=100, r=151.0, S=150.0, half_spread=0.01, skew=0.0)
        levels_no_tilt = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0)
        shift = levels[0].bin_id - levels_no_tilt[0].bin_id
        assert shift == 33

    def test_shift_independent_of_price_level(self):
        """The same relative tilt gives the same bin shift at any price."""
        import math

        def shift_for(price):
            g = VenueGrid(ref_price=price, bin_step_bps=20,
                          base_decimals=6, quote_decimals=9)
            tilted = build_ladder(g, active_bin=0, r=price * 1.01, S=price,
                                  half_spread=price * 0.0001, skew=0.0)
            flat = build_ladder(g, active_bin=0, r=price, S=price,
                                half_spread=price * 0.0001, skew=0.0)
            return tilted[0].bin_id - flat[0].bin_id

        assert shift_for(1.0) == shift_for(150.0) == shift_for(50000.0)
        # 1% tilt at 20 bps bins ≈ log(1.01)/log(1.002) ≈ 5 bins
        assert shift_for(150.0) == 5

    def test_offset_scales_with_relative_spread(self):
        """half_spread is a price distance: same fraction → same offset bins."""
        cfg = LadderConfig(inner_offset=1)
        for price in (1.0, 150.0, 50000.0):
            g = VenueGrid(ref_price=price, bin_step_bps=20,
                          base_decimals=6, quote_decimals=9)
            # 0.1% half-spread ≈ log(1.001)/log(1.002) ≈ 0.5 → clamped to 1;
            # 1% ≈ 5 bins
            wide = build_ladder(g, active_bin=0, r=price, S=price,
                                half_spread=price * 0.01, skew=0.0, cfg=cfg)
            asks = [l for l in wide if l.side == "ask"]
            innermost_ask = min(a.bin_id for a in asks)
            assert innermost_ask == 5, f"at price {price}"

    def test_rejects_nonpositive_prices(self, grid):
        with pytest.raises(ValueError):
            build_ladder(grid, active_bin=0, r=0.0, S=150.0, half_spread=0.01, skew=0.0)
        with pytest.raises(ValueError):
            build_ladder(grid, active_bin=0, r=150.0, S=-1.0, half_spread=0.01, skew=0.0)


class TestSpreadClamping:
    """Inner offset respects minimum and half-spread."""

    def test_min_inner_offset(self, grid):
        """When half_spread is tiny, inner_offset is clamped to cfg.inner_offset."""
        cfg = LadderConfig(inner_offset=3, capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.0001, skew=0.0, cfg=cfg)
        bids = [l for l in levels if l.side == "bid"]
        # Innermost bid should be at least inner_offset bins from center
        assert bids[0].bin_id <= 100 - 3

    def test_half_spread_widens_offset(self, grid):
        """Larger half_spread increases inner offset."""
        cfg = LadderConfig(inner_offset=1, capital=1000.0, level_weight=0.2)
        levels_tight = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0, cfg=cfg)
        levels_wide = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.05, skew=0.0, cfg=cfg)
        bids_tight = [l for l in levels_tight if l.side == "bid"]
        bids_wide = [l for l in levels_wide if l.side == "bid"]
        assert bids_wide[0].bin_id < bids_tight[0].bin_id

class TestSkew:
    """Per-level sizes reflect inventory skew."""

    def test_zero_skew_equal_sizes(self, grid):
        """skew=0 → ask and bid sizes are equal."""
        cfg = LadderConfig(capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0, cfg=cfg)
        bids = [l for l in levels if l.side == "bid"]
        asks = [l for l in levels if l.side == "ask"]
        for b, a in zip(bids, asks):
            assert abs(b.size - a.size) < 1e-9

    def test_positive_skew_favors_asks(self, grid):
        """skew > 0 → ask sizes larger than bid sizes."""
        cfg = LadderConfig(capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.3, cfg=cfg)
        bids = [l for l in levels if l.side == "bid"]
        asks = [l for l in levels if l.side == "ask"]
        for b, a in zip(bids, asks):
            assert a.size > b.size

    def test_negative_skew_favors_bids(self, grid):
        """skew < 0 → bid sizes larger than ask sizes."""
        cfg = LadderConfig(capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=-0.3, cfg=cfg)
        bids = [l for l in levels if l.side == "bid"]
        asks = [l for l in levels if l.side == "ask"]
        for b, a in zip(bids, asks):
            assert b.size > a.size

    def test_extreme_skew_all_to_one_side(self, grid):
        """skew=+0.99 → nearly all capital on asks."""
        cfg = LadderConfig(capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.99, cfg=cfg)
        bids = [l for l in levels if l.side == "bid"]
        asks = [l for l in levels if l.side == "ask"]
        for b, a in zip(bids, asks):
            assert a.size > 50 * b.size

class TestLadderStructure:
    """Basic structure and sorting."""

    def test_level_count(self, grid):
        """2 * levels levels in the ladder."""
        cfg = LadderConfig(levels=5, capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0, cfg=cfg)
        assert len(levels) == 10

    def test_sorted_by_bin_id(self, grid):
        """Levels sorted by bin_id ascending."""
        cfg = LadderConfig(levels=5, capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0, cfg=cfg)
        bin_ids = [l.bin_id for l in levels]
        assert bin_ids == sorted(bin_ids)

    def test_bid_before_ask_per_level(self, grid):
        """Bids are at lower bin ids than asks."""
        cfg = LadderConfig(levels=5, capital=1000.0, level_weight=0.2)
        levels = build_ladder(grid, active_bin=100, r=150.0, S=150.0, half_spread=0.01, skew=0.0, cfg=cfg)
        bids = [l for l in levels if l.side == "bid"]
        asks = [l for l in levels if l.side == "ask"]
        for b, a in zip(bids, asks):
            assert b.bin_id < a.bin_id
