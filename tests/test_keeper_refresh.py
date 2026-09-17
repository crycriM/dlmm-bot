"""Drift-refresh regression tests.

The keeper previously compared ``active_bin`` against the midpoint of its
*bid* bins (a point ~half a ladder away from the price by construction), so
``drift`` exceeded
``drift_threshold_bins`` on every QUOTE cycle and the keeper refreshed its
ladder — a real withdraw+swap+deposit — once per cycle. A refresh also never
adopted the position PDA returned by the executor, so ``get_position()`` kept
polling the closed one.
"""

import asyncio
import math

from dlmm_bot.clock import FrozenClock
from dlmm_bot.config import DLMMConfig
from dlmm_bot.exec_bridge import FakeExecBridge
from dlmm_bot.grid import VenueGrid
from dlmm_bot.keeper import Keeper, KeeperConfig
from dlmm_bot.ladder import LadderLevel
from dlmm_bot.risk_dlmm import PairType


def _keeper(drift_threshold: int = 3):
    grid = VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9)
    cfg = KeeperConfig(
        dlmm=DLMMConfig(gamma=1.0, kappa=0.5, bin_step_bps=20, ref_price=150.0,
                        levels=5, inner_offset=2, capital=1000.0, level_weight=0.2),
        grid=grid,
        pool_address="pool",
        dry_run=False,
        refresh_interval=5.0,
        position_id=None,
        pair_type=PairType.BLUECHIP,
        drift_threshold_bins=drift_threshold,
        base_mint="base",
        quote_mint="quote",
    )
    bridge = FakeExecBridge()
    bridge.set_receipt(position_id="pos-1")
    bridge.set_state("pool", active_bin=100, balances={"base": 0.0, "quote": 0.0},
                     tvl_usd=50_000.0)
    return Keeper(cfg=cfg, exec_bridge=bridge), bridge


def _cycle(keeper):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(keeper._cycle())
    finally:
        loop.close()


def _drive(keeper, bridge, *, amplitude: int, period: int = 10,
           cycles_after_position: int = 0, max_cycles: int = 200):
    """Run real cycles on a synthetic mean-reverting price until a position
    exists, then ``cycles_after_position`` more (keeping the oscillation)."""
    clock = FrozenClock(start=2_000_000.0, step=5.0)
    deposited_at = None
    with clock:
        for i in range(max_cycles):
            active = 100 + round(amplitude * math.sin(2 * math.pi * i / period))
            bridge.set_state(
                "pool", active_bin=active,
                balances={"base": 1.0 if keeper._current_position_id else 0.0,
                          "quote": 500.0 if keeper._current_position_id else 0.0},
                tvl_usd=50_000.0,
            )
            _cycle(keeper)
            clock.advance()
            if deposited_at is None and keeper._current_position_id is not None:
                deposited_at = i
            elif deposited_at is not None and i - deposited_at >= cycles_after_position:
                break
    return deposited_at


def _refresh_calls(bridge) -> int:
    return sum(1 for c in bridge.calls if c["method"] == "refresh_bundle")


class TestRefreshPositionId:
    def test_refresh_adopts_returned_position_id(self):
        keeper, bridge = _keeper()
        keeper._current_position_id = "pos-1"
        bridge.set_receipt(position_id="pos-2")
        ladder = [
            LadderLevel(bin_id=96, side="bid", size=10.0),
            LadderLevel(bin_id=104, side="ask", size=10.0),
        ]
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(keeper._refresh_ladder(ladder))
        finally:
            loop.close()
        assert keeper._current_position_id == "pos-2"


class TestDriftRefresh:
    def test_stable_price_does_not_refresh(self):
        keeper, bridge = _keeper()
        deposited_at = _drive(keeper, bridge, amplitude=1, cycles_after_position=24)
        assert deposited_at is not None
        # +/-1 bin moves (< drift_threshold 3) justify no refresh at all.
        assert _refresh_calls(bridge) == 0

    def test_drift_beyond_threshold_refreshes_without_storm(self):
        keeper, bridge = _keeper()
        deposited_at = _drive(keeper, bridge, amplitude=5, period=12,
                              cycles_after_position=24)
        assert deposited_at is not None
        # The +/-5 bin swing crosses the 3-bin threshold about once per half
        # period (~4 times in 24 cycles); the pre-fix keeper refreshed every
        # cycle (24+).
        assert 1 <= _refresh_calls(bridge) <= 8
