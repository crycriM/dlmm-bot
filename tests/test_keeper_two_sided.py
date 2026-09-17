"""Two-sided ladder position tracking.

A bid deposit and an ask deposit land in two distinct DLMM position PDAs (a
position is one contiguous bin range), but the keeper previously tracked only
the bid id, while the refresh response reported only the first leg. Every
placement or refresh therefore leaked an ask-side PDA that stop-quoting and
emergency-exit could never close. The executor now reports all opened ids
(``position_ids``), and the keeper adopts, observes, and withdraws all of them.
"""

import asyncio

from dlmm_bot.config import DLMMConfig
from dlmm_bot.exec_bridge import FakeExecBridge
from dlmm_bot.grid import VenueGrid
from dlmm_bot.keeper import Keeper, KeeperConfig
from dlmm_bot.ladder import LadderLevel
from dlmm_bot.risk_dlmm import PairType


class TwoSidedFake(FakeExecBridge):
    """Fake bridge that gives each deposit side its own PDA and reports the
    full opened-id list on refresh, like the real executor."""

    def deposit_single_sided(self, pool, side, *args, **kwargs):
        result = super().deposit_single_sided(pool, side, *args, **kwargs)
        if result.ok:
            pid = f"pos-{side}"
            result.position_id = pid
            result.data = {**(result.data or {}), "position_id": pid}
        return result

    def refresh_bundle(self, withdraw_position_id, swap_spec, deposit_spec):
        result = super().refresh_bundle(withdraw_position_id, swap_spec, deposit_spec)
        if result.ok:
            ids = ["pos-refresh-bid", "pos-refresh-ask"]
            result.position_id = ids[0]
            result.data = {
                **(result.data or {}),
                "position_id": ids[0],
                "position_ids": ids,
            }
        return result


def _keeper(log_dir: str | None = None):
    grid = VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9)
    cfg = KeeperConfig(
        dlmm=DLMMConfig(gamma=1.0, kappa=0.5, bin_step_bps=20, ref_price=150.0,
                        levels=1, inner_offset=2, capital=1000.0, level_weight=0.2),
        grid=grid,
        pool_address="pool",
        dry_run=False,
        position_id=None,
        pair_type=PairType.BLUECHIP,
        log_dir=log_dir,
        base_mint="base",
        quote_mint="quote",
    )
    bridge = TwoSidedFake()
    bridge.set_state("pool", active_bin=100, balances={"base": 1.0, "quote": 500.0},
                     tvl_usd=50_000.0)
    return Keeper(cfg=cfg, exec_bridge=bridge), bridge


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _ladder():
    return [
        LadderLevel(bin_id=98, side="bid", size=100.0),
        LadderLevel(bin_id=102, side="ask", size=1.0),
    ]


def _withdrawn(bridge):
    return [c["position_id"] for c in bridge.calls if c["method"] == "withdraw"]


class TestDepositTracking:
    def test_ask_pda_is_tracked_as_extra(self):
        keeper, _bridge = _keeper()
        _run(keeper._deposit_ladder(_ladder()))
        assert keeper._current_position_id == "pos-bid"
        assert keeper._extra_position_ids == ["pos-ask"]


class TestAmountQuantization:
    """The executor rejects amounts that are not whole raw token units, while
    ladder sizes begin as unquantized floats."""

    def test_deposit_amounts_quantize_to_integer_raw_units(self):
        keeper, bridge = _keeper()
        ladder = [
            LadderLevel(bin_id=98, side="bid", size=100.0 / 3),  # quote, 9 dec
            LadderLevel(bin_id=102, side="ask", size=1.0 / 3),   # base, 6 dec
        ]
        _run(keeper._deposit_ladder(ladder))
        deposits = [c for c in bridge.calls if c["method"] == "deposit_single_sided"]
        bid = next(c for c in deposits if c["side"] == "bid")
        ask = next(c for c in deposits if c["side"] == "ask")
        for amount in bid["amounts"]:
            assert (amount * 10**9).is_integer()
        for amount in ask["amounts"]:
            assert (amount * 10**6).is_integer()

    def test_levels_quantizing_to_zero_are_dropped(self):
        keeper, bridge = _keeper()
        ladder = [
            LadderLevel(bin_id=98, side="bid", size=100.0),
            LadderLevel(bin_id=102, side="ask", size=1e-9),  # < 1 raw base unit
        ]
        _run(keeper._deposit_ladder(ladder))
        deposits = [c for c in bridge.calls if c["method"] == "deposit_single_sided"]
        assert [c["side"] for c in deposits] == ["bid"]


class TestRefreshTracking:
    def test_refresh_closes_stale_ask_and_adopts_new_pdas(self):
        keeper, bridge = _keeper()
        _run(keeper._deposit_ladder(_ladder()))
        bridge.calls.clear()
        _run(keeper._refresh_ladder(_ladder()))
        # The stale ask PDA was withdrawn before the bundle was sent.
        assert _withdrawn(bridge)[:1] == ["pos-ask"]
        assert any(c["method"] == "refresh_bundle" for c in bridge.calls)
        # Both freshly opened PDAs are adopted; the bid stays primary.
        assert keeper._current_position_id == "pos-refresh-bid"
        assert keeper._extra_position_ids == ["pos-refresh-ask"]

    def test_refresh_aborts_when_stale_ask_cannot_be_withdrawn(self):
        keeper, bridge = _keeper()
        _run(keeper._deposit_ladder(_ladder()))
        bridge.set_next_result(False)
        bridge.calls.clear()
        _run(keeper._refresh_ladder(_ladder()))
        assert not any(c["method"] == "refresh_bundle" for c in bridge.calls)
        # The failed id is never silently dropped.
        assert keeper._extra_position_ids == ["pos-ask"]


class TestClosePaths:
    def test_stop_quoting_withdraws_every_tracked_pda(self):
        keeper, bridge = _keeper()
        _run(keeper._deposit_ladder(_ladder()))
        bridge.calls.clear()
        _run(keeper._stop_quoting())
        assert _withdrawn(bridge) == ["pos-bid", "pos-ask"]
        assert keeper._current_position_id is None
        assert keeper._extra_position_ids == []

    def test_emergency_exit_withdraws_every_tracked_pda(self):
        keeper, bridge = _keeper()
        _run(keeper._deposit_ladder(_ladder()))
        bridge.calls.clear()
        _run(keeper._emergency_exit())
        assert _withdrawn(bridge) == ["pos-bid", "pos-ask"]
        assert keeper._current_position_id is None
        assert keeper._extra_position_ids == []


class TestRestartRecovery:
    def test_event_log_carries_every_opened_pda_for_restart(self, tmp_path):
        # After a restart, the keeper restores its complete position set
        # from the durable event log, so BOTH opened PDAs must be recorded.
        keeper, _bridge = _keeper(log_dir=str(tmp_path))
        _run(keeper._deposit_ladder(_ladder()))
        keeper.log.close()
        from dlmm_bot.event_log import ReplayLog
        events = ReplayLog(str(next(tmp_path.iterdir()))).events()
        opened = sorted({
            e["position_id"] for e in events
            if e["event_type"] in ("position_created", "position_liquidity_added")
        })
        assert opened == ["pos-ask", "pos-bid"]
        # A restarted keeper adopts the logged ids and closes them all.
        restored, bridge = _keeper()
        restored._current_position_id = "pos-bid"
        restored._extra_position_ids = ["pos-ask"]
        _run(restored._stop_quoting())
        assert _withdrawn(bridge) == ["pos-bid", "pos-ask"]
