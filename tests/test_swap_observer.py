"""SwapObserver: dedupe, fill rule, and parity with the DLMMBacktester."""

from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

from dlmm_bot.backtest import BinEvent, DLMMBacktester
from dlmm_bot.config import DLMMConfig
from dlmm_bot.event_log import EventLog, load_events
from dlmm_bot.grid import VenueGrid
from dlmm_bot.swap_observer import SwapObserver, SwapStreamRunner


@pytest.fixture
def grid():
    return VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9)


@pytest.fixture
def log(grid, tmp_path):
    l = EventLog(str(tmp_path / "obs.jsonl"), run_id="r", config_hash="h")
    yield l
    l.close()


def _fills(log):
    return [e for e in load_events(log.path) if e["event_type"] == "bin_fill"]


class TestDedupe:
    def test_same_signature_is_deduplicated(self, grid, log):
        ob = SwapObserver(log, grid, "p")
        s1 = ob.on_swap({"tx_signature": "SIG", "prev_active_bin": 100,
                         "new_active_bin": 101}, ts=1.0)
        s2 = ob.on_swap({"tx_signature": "SIG", "prev_active_bin": 100,
                         "new_active_bin": 101}, ts=2.0)
        assert s1 is not None and s2 is None
        trades = [e for e in load_events(log.path) if e["event_type"] == "observed_trade"]
        assert len(trades) == 1

    def test_no_signature_always_emits(self, grid, log):
        ob = SwapObserver(log, grid, "p")
        assert ob.on_swap({"prev_active_bin": 100, "new_active_bin": 101}, ts=1.0) is not None
        assert ob.on_swap({"prev_active_bin": 100, "new_active_bin": 102}, ts=2.0) is not None


class TestFills:
    def test_up_cross_sells_only_crossed_bids(self, grid, log):
        ob = SwapObserver(log, grid, "p")
        ob.set_last_mid(grid.price_from_bin(100), 1.0)
        ob.register_ladder({101: 3.0, 99: 2.0}, position_id="PID")
        ob.on_swap({"tx_signature": "S", "prev_active_bin": 100, "new_active_bin": 103,
                    "direction": "up", "fee_bps": 25.0}, ts=2.0)
        # up 100->103 crosses [100,101,102]; only bin 101 has resting liquidity
        assert [(f["bin_id"], f["side_filled"], f["amount_base"]) for f in _fills(log)] \
            == [(101, "sell", 3.0)]
        assert _fills(log)[0]["position_id"] == "PID"
        assert ob.resting[101] == 0.0  # consumed after fill

    def test_down_cross_buys_all_crossed_bids(self, grid, log):
        ob = SwapObserver(log, grid, "p")
        ob.register_ladder({101: 3.0, 99: 2.0})
        ob.on_swap({"tx_signature": "S", "prev_active_bin": 101, "new_active_bin": 98,
                    "direction": "down", "fee_bps": 25.0}, ts=2.0)
        # down 101->98 crosses [99,100,101]; resting at 99 (2.0) and 101 (3.0)
        got = sorted((f["bin_id"], f["side_filled"], f["amount_base"]) for f in _fills(log))
        assert got == [(99, "buy", 2.0), (101, "buy", 3.0)]

    def test_no_fill_without_resting(self, grid, log):
        ob = SwapObserver(log, grid, "p")
        ob.on_swap({"tx_signature": "S", "prev_active_bin": 100, "new_active_bin": 103,
                    "direction": "up", "fee_bps": 25.0}, ts=2.0)
        assert _fills(log) == []
        trade = [e for e in load_events(log.path) if e["event_type"] == "observed_trade"][0]
        assert trade["crossed_ours"] is False

    def test_register_and_clear_ladder(self, grid, log):
        ob = SwapObserver(log, grid, "p")
        ob.register_ladder({101: 1.0}, position_id="A")
        assert ob.resting == {101: 1.0}
        ob.clear()
        assert ob.resting == {}


class TestBacktesterParity:
    """The observer must apply the exact same fill rule as DLMMBacktester so
    live and backtest accounting cannot diverge."""

    def test_fill_rule_matches_backtester(self, grid):
        # Backtester side: drive one up-cross through a ladder, capture the fills
        bt = DLMMBacktester(DLMMConfig(), grid, [])
        bt.ladder_state.active = True
        bt.ladder_state.center_bin = 100
        bt.ladder_state.bin_levels = {100: 1.0, 101: 3.0, 99: 2.0}

        captured: list = []
        bt.pnl.on_fill = lambda fill: captured.append(fill)  # capture w/o accruing

        ev = BinEvent(ts=10.0, pool="p", active_bin=103, prev_active_bin=99,
                      direction="up", trade_size_usd=100.0, fee_bps=25.0,
                      tvl_usd=50000.0)
        bt._process_event(ev)
        bt_fills = sorted(
            (grid.bin_from_price(f.price), f.side, round(f.size, 9))
            for f in captured
        )

        # Observer side: identical move + ladder
        with tempfile.TemporaryDirectory() as d:
            log = EventLog(os.path.join(d, "x.jsonl"))
            ob = SwapObserver(log, grid, "p")
            ob.set_last_mid(grid.price_from_bin(99), 10.0)
            ob.register_ladder({100: 1.0, 101: 3.0, 99: 2.0}, position_id="P")
            ob.on_swap({"tx_signature": "S", "prev_active_bin": 99,
                        "new_active_bin": 103, "direction": "up", "fee_bps": 25.0},
                       ts=10.0)
            log.close()
            obs_fills = sorted(
                (f["bin_id"], f["side_filled"], round(f["amount_base"], 9))
                for f in _fills(log)
            )

        # up 99->103 crosses [99,100,101,102]; resting on 99,100,101 all sell
        assert bt_fills == obs_fills
        assert {b for b, _, _ in obs_fills} == {99, 100, 101}
        assert bt.n_fills == 3


def test_rich_bid_liquidity_keeps_quote_and_base_units(grid, log):
    observer = SwapObserver(log, grid, "p")
    price = grid.price_from_bin(99)
    observer.register_ladder({
        99: {
            "side": "bid",
            "amount_base": 0.0,
            "amount_quote": 200.0,
            "amount_quote_raw": 200_000_000_000,
        },
    }, position_id="P")
    observer.on_swap({
        "tx_signature": "S",
        "slot": 7,
        "block_time": 8,
        "prev_active_bin": 100,
        "new_active_bin": 98,
        "direction": "down",
        "bins_crossed": [{"bin_id": 99, "amount_x_raw": 123}],
    }, ts=2.0)
    fill = _fills(log)[0]
    assert fill["amount_quote"] == pytest.approx(200.0)
    assert fill["amount_base"] == pytest.approx(200.0 / price)
    assert fill["amount_quote_raw"] == 200_000_000_000
    trade = [e for e in load_events(log.path) if e["event_type"] == "observed_trade"][0]
    assert trade["bins_crossed"] == [
        {"bin_id": 99, "amount_x_raw": 123, "bin_price": price},
        {"bin_id": 100, "bin_price": grid.price_from_bin(100)},
    ]


@pytest.mark.asyncio
async def test_stream_runner_backfills_before_subscribe(grid, log):
    class Source:
        async def backfill(self, pool, after_signature):
            return [{
                "tx_signature": "BACKFILL", "prev_active_bin": 100,
                "new_active_bin": 101,
            }]

        async def subscribe(self, pool):
            yield {
                "tx_signature": "LIVE", "prev_active_bin": 101,
                "new_active_bin": 102,
            }
            await asyncio.Event().wait()

    runner = SwapStreamRunner(SwapObserver(log, grid, "p"), Source())
    task = runner.start()
    for _ in range(20):
        trades = [e for e in load_events(log.path) if e["event_type"] == "observed_trade"]
        if len(trades) == 2:
            break
        await asyncio.sleep(0)
    runner.stop()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert [t["tx_signature"] for t in trades] == ["BACKFILL", "LIVE"]
