"""Ping-pong soak simulator (tools/pingpong_soak.py): in-place flips, shifts, slow hedge."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import pingpong_soak  # noqa: E402

from dlmm_bot.backtest import BinEvent  # noqa: E402
from dlmm_bot.grid import VenueGrid  # noqa: E402

GRID = VenueGrid(ref_price=100.0, bin_step_bps=10, base_decimals=9, quote_decimals=6)


def _walk(path: list[int], dt: float = 60.0) -> list[BinEvent]:
    """Active-bin path -> one BinEvent per step."""
    return [
        BinEvent(ts=i * dt, pool="p", active_bin=b, prev_active_bin=a,
                 direction="up" if b > a else "down", trade_size_usd=0.0, fee_bps=10.0)
        for i, (a, b) in enumerate(zip(path, path[1:]))
    ]


def test_oscillation_flips_in_place_for_fees_only():
    r = pingpong_soak.simulate(_walk([0, 3, 0, -3] * 5 + [0]), GRID,
                               width=5, shift=2, tau=3600, gas_cost=0.0)
    assert r["n_shifts"] == 0 and r["n_flips"] > 0
    # Same-bin round trips and a price that ends where it started: no
    # conversion PnL, the LP leg is exactly its fees.
    assert r["conversion_vs_hold"] == pytest.approx(0.0, abs=1e-9)
    assert r["lp_total"] == pytest.approx(r["lp_fee"]) and r["lp_fee"] > 0


def test_trend_accumulates_without_swapping_and_hedge_cuts_drawdown():
    r = pingpong_soak.simulate(_walk(list(range(0, -60, -1))), GRID,
                               width=5, shift=2, tau=900, gas_cost=0.0)
    assert r["n_shifts"] > 0
    assert r["final_base_share"] == 1.0  # bought all the way down, never swapped back
    assert r["conversion_vs_hold"] < 0   # adverse selection bites
    assert r["final_short"] > 5.0        # hedge followed the accumulated base up from the seed
    assert r["max_dd_hedged"] < r["max_dd_lp"]



def test_one_bin_gap_does_not_reshift():
    # At 6 the untouched bids merge into 1..5 next to the active bin (one
    # real shift). Swaps that keep price at 6 find the range already there;
    # re-depositing the identical range would only pay gas.
    events = _walk(list(range(0, 7)))
    events += [BinEvent(ts=1000.0 + i, pool="p", active_bin=6, prev_active_bin=6,
                        direction="down", trade_size_usd=0.0, fee_bps=10.0)
               for i in range(5)]
    r = pingpong_soak.simulate(events, GRID, width=5, shift=1, tau=3600, gas_cost=0.0)
    assert r["n_shifts"] == 1


def test_in_bin_swaps_pay_our_share_of_the_active_bin():
    events = _walk([0, 2])  # bin 2 (ours) becomes active, holding base
    events.append(BinEvent(ts=200.0, pool="p", active_bin=2, prev_active_bin=2,
                           direction="down", trade_size_usd=10_000.0, fee_bps=10.0))
    kw = dict(width=5, shift=2, tau=3600, gas_cost=0.0)
    off = pingpong_soak.simulate(events, GRID, **kw)
    on = pingpong_soak.simulate(events, GRID, pool_bin_quote=9_900.0, **kw)
    ours = 1.0 * GRID.price_from_bin(2)  # 5 base over 5 ask bins, marked at bin 2
    assert on["in_bin_fee"] == pytest.approx(10.0 * ours / (9_900.0 + ours))
    assert on["lp_total"] - off["lp_total"] == pytest.approx(on["in_bin_fee"])


def test_sampled_depth_uses_latest_sample_for_the_active_bin(tmp_path):
    def row(ts, active, quote_raw):
        # 6-decimal quote only: bin value = quote_raw / 1e6
        return {"ts": ts, "pool": "p", "active_bin": active,
                "bins": [{"bin_id": b, "price": 100.0, "x_raw": "0", "y_raw": str(quote_raw * (b - active + 2))}
                         for b in (active - 1, active, active + 1)]}
    path = tmp_path / "depth.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in [
        row(100.0, 0, 1_000_000), {"ts": 150.0, "pool": "p", "error": "RpcReadError"},
        row(200.0, 0, 3_000_000)]) + "\n")
    depth = pingpong_soak.depth_at(str(path), 9, 6)
    at = lambda ts, b: depth(BinEvent(ts=ts, pool="p", active_bin=b, prev_active_bin=b,
                                      direction="down", trade_size_usd=0.0, fee_bps=0.0))
    assert at(150.0, 0) == pytest.approx(2.0)    # latest sample at/before ts (bin 0 = 2 × 1.0)
    assert at(250.0, 1) == pytest.approx(9.0)    # later sample, bin 1 = 3 × 3.0
    assert at(250.0, 50) == pytest.approx(6.0)   # unsampled bin: the sample's median
    assert at(10.0, 0) == pytest.approx(2.0)     # before any sample: the first one
