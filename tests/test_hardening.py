"""Fail-closed behaviour of the executor bridge and the keeper's signing paths."""

import asyncio
import sys

import pytest

from dlmm_bot.config import DLMMConfig
from dlmm_bot.exec_bridge import ExecBridge, ExecResult, FakeExecBridge
from dlmm_bot.grid import VenueGrid
from dlmm_bot.keeper import MAX_ACTION_FAILURES, Keeper, KeeperConfig
from dlmm_bot.ladder import LadderLevel


def py_bridge(script, timeout=10):
    return ExecBridge([sys.executable, "-c", script], timeout=timeout)


class TestExecBridge:
    def test_timeout_kills_executor_and_flags_unknown_outcome(self):
        bridge = py_bridge("import time; time.sleep(60)", timeout=0.3)
        try:
            result = bridge.get_state("pool")
        finally:
            bridge.stop()
        assert not result.ok and result.unknown_outcome and "timeout" in result.error

    def test_stderr_flood_does_not_deadlock(self):
        # 300 KB of stderr before replying: fills the 64 KiB pipe unless it is drained
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "sys.stderr.write('x' * 300000)\n"
            "print(json.dumps({'ok': True, 'data': {}}), flush=True)\n"
        )
        bridge = py_bridge(script, timeout=10)
        try:
            assert bridge.get_state("pool").ok
        finally:
            bridge.stop()

    def test_eof_reports_stderr_tail_and_unknown_outcome(self):
        bridge = py_bridge("import sys; sys.stdin.readline(); sys.stderr.write('boom\\n')")
        try:
            result = bridge.get_state("pool")
        finally:
            bridge.stop()
        assert not result.ok and result.unknown_outcome

    @pytest.mark.parametrize("call", [
        lambda b: b.swap("a", "b", float("nan"), "pool"),
        lambda b: b.swap("a", "b", 0.0, "pool"),
        lambda b: b.swap("a", "b", 1.0, "pool", max_slippage_bps=5000),
        lambda b: b.withdraw("pos", bps=0),
        lambda b: b.withdraw("pos", bps=101),
        lambda b: b.deposit_single_sided("pool", "bid", [1], [float("inf")],
                                         expected_active_bin=1, max_active_bin_slippage=0),
    ])
    def test_invalid_arguments_are_refused_before_the_wire(self, call):
        bridge = ExecBridge(["/nonexistent-executor"])  # must never be spawned
        result = call(bridge)
        assert not result.ok and "refusing" in result.error and bridge._proc is None


@pytest.fixture
def cfg():
    return KeeperConfig(
        dlmm=DLMMConfig(gamma=1.0, kappa=0.5, bin_step_bps=20, ref_price=150.0,
                        levels=5, inner_offset=2, capital=1000.0, level_weight=0.2),
        grid=VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9),
        pool_address="test_pool", dry_run=False, refresh_interval=0.0,
        base_mint="BASE", quote_mint="QUOTE",
    )


LADDER = [LadderLevel(bin_id=98, side="bid", size=75), LadderLevel(bin_id=102, side="ask", size=0.5)]


class TestKeeperHalts:
    def test_unknown_outcome_deposit_halts_instead_of_retrying(self, cfg):
        class TimedOutBridge(FakeExecBridge):
            def deposit_single_sided(self, *a, **k):
                self.calls.append({"method": "deposit_single_sided"})
                return ExecResult(ok=False, unknown_outcome=True, error="executor timeout")

        bridge = TimedOutBridge()
        bridge.set_state("test_pool", active_bin=100, balances={"base": 1.0, "quote": 100.0}, tvl_usd=5e4)
        keeper = Keeper(cfg, bridge)
        asyncio.run(keeper.run(max_cycles=10))
        assert keeper._halted
        assert [c["method"] for c in bridge.calls].count("deposit_single_sided") == 1  # not re-sent
        assert keeper._cycle_count == 1  # the loop was left, not just paused

    def test_consecutive_failures_halt(self, cfg):
        keeper = Keeper(cfg, FakeExecBridge())
        failed = ExecResult(ok=False, error="rejected")
        for _ in range(MAX_ACTION_FAILURES - 1):
            keeper._emit_result("r", "swap", failed, "gas")
        assert not keeper._halted
        keeper._emit_result("r", "swap", ExecResult(ok=True), "gas")  # success resets the count
        for _ in range(MAX_ACTION_FAILURES - 1):
            keeper._emit_result("r", "swap", failed, "gas")
        assert not keeper._halted
        keeper._emit_result("r", "swap", failed, "gas")
        assert keeper._halted

    def test_emergency_exit_is_final_no_redeposit(self, cfg):
        bridge = FakeExecBridge()
        bridge.set_state("test_pool", active_bin=100, balances={"base": 0.0, "quote": 0.0}, tvl_usd=5e4)
        keeper = Keeper(cfg, bridge)
        asyncio.run(keeper._emergency_exit())
        assert keeper._halted
        bridge.calls.clear()
        record = asyncio.run(keeper._cycle())
        assert record.action == "halted"
        assert not any(c["method"] in ("deposit_single_sided", "refresh_bundle") for c in bridge.calls)

    @pytest.mark.parametrize("state", [
        {"balances": {"base": 1.0, "quote": 1.0}},                       # no active_bin (was: bin 0)
        {"active_bin": 100, "balances": {"base": float("nan"), "quote": 1.0}},
        {"active_bin": 100, "balances": {"base": -1.0, "quote": 1.0}},
        {"active_bin": "x", "balances": {}},
    ])
    def test_malformed_state_holds_without_actuating(self, cfg, state):
        bridge = FakeExecBridge()
        bridge._state["test_pool"] = state
        keeper = Keeper(cfg, bridge)
        record = asyncio.run(keeper._cycle())
        assert record.action == "bad_state"
        assert [c["method"] for c in bridge.calls] == ["get_state"]


class TestTvlKillSwitch:
    def policy(self):
        from dlmm_bot.risk_dlmm import DLMMRiskConfig, DLMMRiskPolicy
        return DLMMRiskPolicy(DLMMRiskConfig(tvl_drop_pct=30.0, tvl_window_s=300.0, min_tvl_usd=1000.0))

    def test_gradual_drain_across_polls_trips_the_window(self):
        policy = self.policy()
        readings = [100_000, 90_000, 80_000, 70_000, 65_000]  # each poll < 30%, total 35%
        kills = [policy.evaluate_tvl(5.0 * i, tvl)[0] for i, tvl in enumerate(readings)]
        assert kills == [False, False, False, False, True]

    def test_old_peak_outside_window_is_forgotten(self):
        policy = self.policy()
        policy.evaluate_tvl(0.0, 100_000)
        assert policy.evaluate_tvl(1000.0, 60_000) == (False, "")  # 40% but > 300 s later

    def test_nan_tvl_is_ignored_and_does_not_poison_baseline(self):
        policy = self.policy()
        policy.evaluate_tvl(0.0, 100_000)
        assert policy.evaluate_tvl(5.0, float("nan")) == (False, "")
        assert policy.evaluate_tvl(10.0, 60_000)[0] is True  # baseline survived the NaN


class TestHedge:
    def test_emergency_exit_closes_the_perp_short(self, cfg):
        from dlmm_bot.hedge import HedgeConfig
        from dlmm_bot.risk_dlmm import PairType

        cfg.pair_type = PairType.EXOTIC
        cfg.hedge_config = HedgeConfig(venue="hl", coin="ETH")
        published = []
        bridge = FakeExecBridge()
        keeper = Keeper(cfg, bridge, publish=lambda subject, payload: published.append((subject, payload)))
        keeper._hedge_short = 2.5
        asyncio.run(keeper._emergency_exit())
        (subject, intent), = published
        assert subject == "ctrl.hl.ETH"
        assert intent["target_inventory"] == 0.0 and intent["current_inventory"] == -2.5
        assert keeper._hedge_short == 0.0

    def test_non_finite_inputs_never_trade_or_poison_the_ema(self):
        from dlmm_bot.hedge import HedgeConfig, HedgeController

        controller = HedgeController(HedgeConfig(venue="hl", coin="ETH"))
        for bad in (dict(inventory_base=float("nan")), dict(price=float("inf")), dict(sigma_now=float("nan"))):
            args = dict(inventory_base=10.0, current_short=0.0, inventory_value_usd=1e4, price=1000.0, **{})
            args.update(bad)
            assert controller.evaluate(**args) == ("no_trade", 0.0, None)
        assert controller.state.ma_target == 0.0  # untouched
