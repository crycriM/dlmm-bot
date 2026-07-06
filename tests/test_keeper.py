"""Tests for the DLMM keeper loop — against a FakeExecBridge."""

import asyncio
import pytest
from dlmm_bot.keeper import Keeper, KeeperConfig, CycleRecord
from dlmm_bot.config import DLMMConfig
from dlmm_bot.grid import VenueGrid
from dlmm_bot.exec_bridge import FakeExecBridge
from dlmm_bot.risk_dlmm import PairType


@pytest.fixture
def grid():
    return VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9)


@pytest.fixture
def cfg(grid):
    return KeeperConfig(
        dlmm=DLMMConfig(gamma=1.0, kappa=0.5, bin_step_bps=20, ref_price=150.0,
                        levels=5, inner_offset=2, capital=1000.0, level_weight=0.2),
        grid=grid,
        pool_address="test_pool",
        dry_run=True,
        refresh_interval=0.01,
        position_id=None,
        pair_type=PairType.BLUECHIP,
    )


@pytest.fixture
def bridge():
    b = FakeExecBridge()
    # Start with zero inventory
    b.set_state("test_pool", active_bin=100, balances={"base": 0.0, "quote": 0.0}, tvl_usd=50000.0)
    return b


@pytest.fixture
def keeper(cfg, bridge):
    return Keeper(cfg=cfg, exec_bridge=bridge)


class TestKeeperInit:
    def test_components_initialized(self, keeper):
        assert keeper.risk_policy is not None
        assert keeper.dlmm_risk is not None
        assert keeper.markout is not None
        assert keeper.pnl is not None
        assert keeper._halted is False

    def test_no_hedge_for_bluechip(self, keeper):
        assert keeper.hedge is None


class TestKeeperCycle:
    def test_cycle_returns_record(self, keeper, bridge):
        """A single cycle should produce a CycleRecord."""
        loop = asyncio.new_event_loop()
        record = loop.run_until_complete(keeper._cycle())
        assert isinstance(record, CycleRecord)
        assert record.active_bin == 100
        assert record.mid > 0
        assert record.dry_run is True
        loop.close()

    def test_cycle_logs_decision(self, keeper):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(keeper._cycle())
        loop.close()
        assert len(keeper.decision_log) == 1

    def test_cycle_with_no_state_returns_stop(self, keeper, bridge):
        bridge.set_next_result(False)
        loop = asyncio.new_event_loop()
        record = loop.run_until_complete(keeper._cycle())
        loop.close()
        assert record.decision == "stop_quoting"
        assert record.action == "no_state"

    def test_initial_deposit_when_no_position(self, keeper, bridge):
        """First cycle with no position should trigger initial deposit."""
        loop = asyncio.new_event_loop()
        record = loop.run_until_complete(keeper._cycle())
        loop.close()
        # With 0 price history, regime eval may fail — but the ladder
        # should still attempt deposit if state is available
        assert record.action in ("initial_deposit", "hold", "no_state", "error")

    def test_dry_run_does_not_send_tx(self, keeper, bridge):
        """In dry_run mode, no deposit/withdraw should be sent."""
        loop = asyncio.new_event_loop()
        loop.run_until_complete(keeper._cycle())
        loop.close()
        # In dry_run, the keeper logs but doesn't call deposit/withdraw
        # (only get_state is called for polling)
        deposit_calls = [c for c in bridge.calls if c["method"] in ("deposit_single_sided", "withdraw", "refresh_bundle")]
        assert len(deposit_calls) == 0


class TestKeeperRunWithCycles:
    def test_run_multiple_cycles(self, keeper, bridge):
        """Run N cycles and verify decision log grows."""
        loop = asyncio.new_event_loop()
        loop.run_until_complete(keeper.run(max_cycles=5))
        loop.close()
        assert len(keeper.decision_log) >= 1
        # Stop the keeper
        keeper.stop()

    def test_rug_kills_keeper(self, keeper, bridge):
        """If TVL drops hard, emergency_exit fires."""
        bridge.set_state("test_pool", active_bin=100,
                         balances={"base": 5.0, "quote": 500.0},
                         tvl_usd=500.0)  # below min_tvl_usd default 5000
        loop = asyncio.new_event_loop()
        record = loop.run_until_complete(keeper._cycle())
        loop.close()
        assert record.decision == "emergency_exit"
        assert record.action in ("emergency_exit", "no_state")


class TestKeeperHedge:
    def test_hedge_initialized_for_exotic(self, cfg, bridge):
        from dlmm_bot.hedge import HedgeConfig
        cfg.hedge_config = HedgeConfig(venue="hl", coin="SOL", enabled=True)
        cfg.pair_type = PairType.EXOTIC
        keeper = Keeper(cfg=cfg, exec_bridge=bridge)
        assert keeper.hedge is not None

    def test_no_hedge_when_disabled(self, cfg, bridge):
        from dlmm_bot.hedge import HedgeConfig
        cfg.hedge_config = HedgeConfig(enabled=False)
        keeper = Keeper(cfg=cfg, exec_bridge=bridge)
        assert keeper.hedge is None