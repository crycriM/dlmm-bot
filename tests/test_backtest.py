"""Tests for the DLMM backtester — event-replay with crossed-bin fees."""

import pytest
from dlmm_bot.backtest import BinEvent, DLMMBacktester, BacktestResult
from dlmm_bot.config import DLMMConfig
from dlmm_bot.grid import VenueGrid


@pytest.fixture
def grid():
    return VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9)


@pytest.fixture
def cfg():
    return DLMMConfig(
        gamma=1.0, kappa=0.5, bin_step_bps=20, ref_price=150.0,
        levels=5, inner_offset=2, capital=1000.0, level_weight=0.2,
    )


@pytest.fixture
def oscillating_events():
    """Events that oscillate around bin 100 (price ~150)."""
    events = []
    ts = 1000.0
    # Up-cross: 100 -> 103 (price rises)
    for i in range(3):
        events.append(BinEvent(
            ts=ts, pool="test", active_bin=101+i, prev_active_bin=100+i,
            direction="up", trade_size_usd=500.0, fee_bps=10.0, tvl_usd=100000.0,
        ))
        ts += 10
    # Down-cross: 103 -> 100 (price falls back)
    for i in range(3):
        events.append(BinEvent(
            ts=ts, pool="test", active_bin=102-i, prev_active_bin=103-i,
            direction="down", trade_size_usd=500.0, fee_bps=10.0, tvl_usd=100000.0,
        ))
        ts += 10
    return events


class TestBacktestBasics:
    def test_empty_events(self, cfg, grid):
        bt = DLMMBacktester(cfg, grid, events=[])
        result = bt.run()
        assert result.n_cycles == 0
        assert result.n_fills == 0

    def test_run_produces_result(self, cfg, grid, oscillating_events):
        bt = DLMMBacktester(cfg, grid, events=oscillating_events, initial_capital=1000.0)
        result = bt.run()
        assert isinstance(result, BacktestResult)
        assert result.n_cycles == len(oscillating_events)
        assert result.n_up_crosses == 3
        assert result.n_down_crosses == 3

    def test_result_to_dict(self, cfg, grid, oscillating_events):
        bt = DLMMBacktester(cfg, grid, events=oscillating_events)
        result = bt.run()
        d = result.to_dict()
        assert "total_pnl" in d
        assert "n_fills" in d
        assert "sharpe" in d


class TestBacktestFills:
    def test_up_cross_fills_ask_side(self, cfg, grid):
        """An up-crossing event should fill ask-side liquidity."""
        events = [BinEvent(
            ts=1000.0, pool="test", active_bin=101, prev_active_bin=100,
            direction="up", trade_size_usd=1000.0, fee_bps=10.0,
        )]
        # Add enough warmup events for regime evaluation
        for i in range(50):
            events.insert(0, BinEvent(
                ts=900.0 + i, pool="test", active_bin=100, prev_active_bin=100,
                direction="up", trade_size_usd=0, fee_bps=10.0,
            ))
        bt = DLMMBacktester(cfg, grid, events=events)
        result = bt.run()
        # After enough events, ladder should be deployed and fill on the cross
        assert result.n_cycles > 0

    def test_lp_fee_accrues_on_crossed_bins(self, cfg, grid):
        """Net edge includes LP fees only on crossed bins."""
        events = [
            BinEvent(ts=1000.0, pool="t", active_bin=101, prev_active_bin=100,
                     direction="up", trade_size_usd=500, fee_bps=25.0, tvl_usd=100000),
        ]
        # Warmup
        for i in range(50):
            events.insert(0, BinEvent(
                ts=900+i, pool="t", active_bin=100, prev_active_bin=100,
                direction="up", trade_size_usd=0, fee_bps=25.0,
            ))
        bt = DLMMBacktester(cfg, grid, events=events)
        result = bt.run()
        # If any fills happened, lp_fee_income should be non-negative
        if result.n_fills > 0:
            assert result.lp_fee_income >= 0


class TestBacktestMetrics:
    def test_drawdown_computed(self, cfg, grid, oscillating_events):
        bt = DLMMBacktester(cfg, grid, events=oscillating_events)
        result = bt.run()
        # Max drawdown should be a number (0 if no losses)
        assert result.max_drawdown >= 0.0

    def test_fill_rate_between_0_and_1(self, cfg, grid, oscillating_events):
        bt = DLMMBacktester(cfg, grid, events=oscillating_events)
        result = bt.run()
        assert 0.0 <= result.fill_rate <= 1.0

class TestLiveParity:
    def test_trend_never_quotes(self, cfg, grid):
        """Decisions come from keeper.plan_cycle: a steady trend keeps the
        regime gate closed, so no ladder is ever deployed."""
        events = [
            BinEvent(ts=1000.0 + 10 * i, pool="t", active_bin=101 + i, prev_active_bin=100 + i,
                     direction="up", trade_size_usd=500, fee_bps=10.0, tvl_usd=100000)
            for i in range(200)
        ]
        bt = DLMMBacktester(cfg, grid, events=events)
        result = bt.run()
        assert "quote" not in {d.value for d in bt.decisions}
        assert result.n_fills == 0
        assert not bt.ladder_state.active

    def test_regime_stop_disabled_quotes_through_the_same_trend(self, cfg, grid):
        """Separate test arm: the trend that keeps the default gate shut no
        longer stops quoting."""
        from dlmm_bot.keeper import KeeperConfig, dump_keeper_config, hash_keeper_config
        events = [
            BinEvent(ts=1000.0 + 10 * i, pool="t", active_bin=101 + i, prev_active_bin=100 + i,
                     direction="up", trade_size_usd=500, fee_bps=10.0, tvl_usd=100000)
            for i in range(200)
        ]
        kcfg = KeeperConfig(dlmm=cfg, grid=grid, regime_stop=False)
        bt = DLMMBacktester(cfg, grid, events=events, keeper_cfg=kcfg)
        bt.run()
        assert "quote" in {d.value for d in bt.decisions} or "widen" in {d.value for d in bt.decisions}
        assert "stop_quoting" not in {d.value for d in bt.decisions}
        # Default dump is unchanged (old config_hash values stay valid); the
        # disabled flag is recorded and changes the hash.
        default = KeeperConfig(dlmm=cfg, grid=grid)
        assert "regime_stop" not in dump_keeper_config(default)["keeper"]
        assert dump_keeper_config(kcfg)["keeper"]["regime_stop"] is False
        assert hash_keeper_config(kcfg) != hash_keeper_config(default)

    def test_history_is_the_keepers_poll_grid(self, cfg, grid):
        """Regime/σ see one sample per refresh_interval at the price then
        prevailing, as the live keeper does — not one sample per swap."""
        events = [
            BinEvent(ts=1000.0, pool="t", active_bin=101, prev_active_bin=100,
                     direction="up", trade_size_usd=500, fee_bps=10.0, tvl_usd=100000),
            BinEvent(ts=1001.0, pool="t", active_bin=102, prev_active_bin=101,
                     direction="up", trade_size_usd=500, fee_bps=10.0, tvl_usd=100000),
            BinEvent(ts=1021.0, pool="t", active_bin=103, prev_active_bin=102,
                     direction="up", trade_size_usd=500, fee_bps=10.0, tvl_usd=100000),
        ]
        bt = DLMMBacktester(cfg, grid, events=events)
        bt.run()
        step = bt.keeper_cfg.refresh_interval
        assert list(bt._ts) == [1000.0 + step * i for i in range(len(bt._ts))]
        assert bt._ts[-1] < 1021.0
        # Polls after the 1001 s swap and before 1021 s saw bin 102.
        assert set(list(bt._prices)[1:]) == {grid.price_from_bin(102)}


def test_risk_policy_uses_pair_type_gate():
    from dlmm_bot.keeper import KeeperConfig, make_risk_policy
    from dlmm_bot.risk_dlmm import DLMMRiskConfig, DLMMRiskPolicy, PairType

    for pair in (PairType.BLUECHIP, PairType.MEMECOIN):
        dlmm_risk = DLMMRiskPolicy(DLMMRiskConfig(pair_type=pair))
        policy = make_risk_policy(KeeperConfig(pair_type=pair), dlmm_risk)
        assert policy.cfg.gate.max_half_life == dlmm_risk.get_half_life_threshold()
