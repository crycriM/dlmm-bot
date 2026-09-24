"""Oracle minute bars: paging, no lookahead, synthetic cross, stale → gate shut."""

import pytest

from dlmm_bot.backtest import BinEvent, DLMMBacktester
from dlmm_bot.config import DLMMConfig
from dlmm_bot.grid import VenueGrid
from dlmm_bot.oracle import OracleBars, fetch_pool_bars, synthetic_cross


def _gecko(bars_newest_first):
    """Fake GeckoTerminal: honours before_timestamp and limit=1000 paging."""
    calls = []

    def fetch(url):
        before = int(url.rsplit("before_timestamp=", 1)[1])
        calls.append(before)
        rows = [b for b in bars_newest_first if b[0] < before][:1000]
        return {"data": {"attributes": {"ohlcv_list": rows}}}

    return fetch, calls


def test_fetch_pages_and_drops_the_forming_bar():
    # 2,500 minute bars, newest first; `end` falls inside the last bar.
    rows = [[t, 1.0, 1.0, 1.0, float(t), 5.0] for t in range(2499 * 60, -60, -60)]
    fetch, calls = _gecko(rows)
    end = 2499 * 60 + 30  # the 2499*60 bar is still forming
    bars = fetch_pool_bars("P", 0, end, fetch=fetch, sleep=lambda _s: None)
    assert [b[0] for b in bars] == [t * 60.0 for t in range(2499)]
    assert len(calls) == 3


def test_history_sees_only_closed_bars():
    oracle = OracleBars(bars=[(t * 60.0, 1, 1, 1, float(t)) for t in range(10)], window=3)
    # The bar opened at 240 closes at 300: invisible at 299, visible at 300.
    assert oracle.history(299.0) == [(120.0, 1.0), (180.0, 2.0), (240.0, 3.0)]
    assert oracle.history(300.0)[-1] == (300.0, 4.0)


def test_synthetic_cross_ratios_and_alignment():
    a = [(0.0, 2.0, 3.0, 1.0, 2.0), (60.0, 2.0, 2.0, 2.0, 4.0)]
    b = [(0.0, 1.0, 2.0, 0.5, 1.0), (120.0, 1.0, 1.0, 1.0, 1.0)]
    assert synthetic_cross(a, b) == [(0.0, 2.0, 6.0, 0.5, 2.0)]


def test_stale_oracle_closes_the_gate_in_the_backtest():
    grid = VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9)
    cfg = DLMMConfig(gamma=1.0, kappa=20.0, bin_step_bps=20, ref_price=150.0,
                     levels=5, inner_offset=1, capital=1000.0, level_weight=0.2)
    events = [
        BinEvent(ts=10_000.0 + 10 * i, pool="t", active_bin=100 + (i % 2), prev_active_bin=100 + ((i + 1) % 2),
                 direction="up" if i % 2 else "down", trade_size_usd=500, fee_bps=10.0, tvl_usd=100000)
        for i in range(300)
    ]
    # Bars end an hour before the capture starts: every cycle sees a stale feed.
    old = OracleBars(bars=[(t * 60.0, 1, 1, 1, 1.0 + (t % 3) * 1e-3) for t in range(100)])
    bt = DLMMBacktester(cfg, grid, events=events, oracle=old)
    bt.run()
    assert old.regime_history(events[0].ts) == []
    assert "quote" not in {d.value for d in bt.decisions}
    assert bt.n_fills == 0


@pytest.mark.parametrize("ts", [0.0, 59.0])
def test_empty_or_unclosed_is_stale(ts):
    assert OracleBars().stale(ts)
    assert OracleBars(bars=[(0.0, 1, 1, 1, 1.0)]).regime_history(ts) == []


def test_quiet_minutes_become_flat_bars():
    from dlmm_bot.oracle import fill_gaps
    bars = [(0.0, 1, 2, 1, 1.5), (180.0, 2, 2, 2, 2.0)]
    filled = fill_gaps(bars, end=299.0)  # the 240 bar is still forming
    assert [b[0] for b in filled] == [0.0, 60.0, 120.0, 180.0]
    assert filled[1] == (60.0, 1.5, 1.5, 1.5, 1.5)
    assert fill_gaps(bars, end=300.0)[-1] == (240.0, 2.0, 2.0, 2.0, 2.0)
