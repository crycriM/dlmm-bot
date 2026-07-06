"""Tests for the DLMM-specific risk policy."""

import pytest
from dlmm_bot.risk_dlmm import (
    PairType, DLMMRiskConfig, DLMMRiskPolicy,
    default_risk_config_for_pair,
)


class TestPairTypeDefaults:
    def test_bluechip_defaults(self):
        cfg = default_risk_config_for_pair(PairType.BLUECHIP)
        assert cfg.pair_type == PairType.BLUECHIP
        assert not cfg.one_sided_only
        assert cfg.max_inventory_pct == 80.0
        assert cfg.max_drawdown_pct == 10.0

    def test_memecoin_defaults(self):
        cfg = default_risk_config_for_pair(PairType.MEMECOIN)
        assert cfg.pair_type == PairType.MEMECOIN
        assert cfg.one_sided_only
        assert cfg.max_inventory_pct == 50.0
        assert cfg.max_drawdown_pct == 8.0
        assert cfg.tvl_drop_pct == 20.0  # tighter

    def test_exotic_defaults(self):
        cfg = default_risk_config_for_pair(PairType.EXOTIC)
        assert cfg.pair_type == PairType.EXOTIC
        assert not cfg.one_sided_only
        assert cfg.max_inventory_pct == 70.0


class TestTVLRugDetection:
    def test_no_kill_on_normal_tvl(self):
        rp = DLMMRiskPolicy(DLMMRiskConfig())
        kill, reason = rp.evaluate_tvl(ts=1.0, tvl_usd=100000.0)
        assert not kill
        assert reason == ""

    def test_kill_on_tvl_below_minimum(self):
        cfg = DLMMRiskConfig(min_tvl_usd=5000.0)
        rp = DLMMRiskPolicy(cfg)
        kill, reason = rp.evaluate_tvl(ts=1.0, tvl_usd=3000.0)
        assert kill
        assert "below minimum" in reason
        assert rp.state.kill_switch_fired

    def test_kill_on_large_tvl_drop(self):
        cfg = DLMMRiskConfig(tvl_drop_pct=30.0, min_tvl_usd=100.0)
        rp = DLMMRiskPolicy(cfg)
        rp.evaluate_tvl(ts=1.0, tvl_usd=100000.0)
        kill, reason = rp.evaluate_tvl(ts=2.0, tvl_usd=60000.0)  # 40% drop
        assert kill
        assert "dropped" in reason
        assert rp.state.kill_switch_fired

    def test_no_kill_on_small_tvl_drop(self):
        cfg = DLMMRiskConfig(tvl_drop_pct=30.0, min_tvl_usd=100.0)
        rp = DLMMRiskPolicy(cfg)
        rp.evaluate_tvl(ts=1.0, tvl_usd=100000.0)
        kill, _ = rp.evaluate_tvl(ts=2.0, tvl_usd=90000.0)  # 10% drop
        assert not kill

    def test_none_tvl_no_kill(self):
        rp = DLMMRiskPolicy(DLMMRiskConfig())
        kill, _ = rp.evaluate_tvl(ts=1.0, tvl_usd=None)
        assert not kill


class TestInventoryCap:
    def test_no_breach_when_balanced(self):
        rp = DLMMRiskPolicy(DLMMRiskConfig(
            pair_type=PairType.BLUECHIP,
            max_inventory_pct=80.0,
            one_sided_only=False,
        ))
        breached, _ = rp.check_inventory_cap(
            base_inventory=40.0,
            quote_inventory_in_base=40.0,
            total_capital_base=100.0,
        )
        assert not breached

    def test_breach_when_one_side_exceeds(self):
        rp = DLMMRiskPolicy(DLMMRiskConfig(
            pair_type=PairType.BLUECHIP,
            max_inventory_pct=80.0,
        ))
        breached, reason = rp.check_inventory_cap(
            base_inventory=90.0,
            quote_inventory_in_base=10.0,
            total_capital_base=100.0,
        )
        assert breached
        assert "exceeds cap" in reason

    def test_memecoin_two_sided_triggers(self):
        rp = DLMMRiskPolicy(DLMMRiskConfig(
            pair_type=PairType.MEMECOIN,
            one_sided_only=True,
        ))
        breached, reason = rp.check_inventory_cap(
            base_inventory=50.0,
            quote_inventory_in_base=50.0,
            total_capital_base=100.0,
        )
        assert breached
        assert "Two-sided" in reason

    def test_memecoin_one_sided_ok(self):
        rp = DLMMRiskPolicy(DLMMRiskConfig(
            pair_type=PairType.MEMECOIN,
            one_sided_only=True,
        ))
        breached, _ = rp.check_inventory_cap(
            base_inventory=100.0,
            quote_inventory_in_base=0.0,
            total_capital_base=100.0,
        )
        assert not breached


class TestHalfLifeThreshold:
    def test_bluechip_tolerates_longer_half_life(self):
        rp_b = DLMMRiskPolicy(default_risk_config_for_pair(PairType.BLUECHIP))
        rp_m = DLMMRiskPolicy(default_risk_config_for_pair(PairType.MEMECOIN))
        assert rp_b.get_half_life_threshold() > rp_m.get_half_life_threshold()

    def test_exotic_relaxes_when_hedge_active(self):
        rp = DLMMRiskPolicy(default_risk_config_for_pair(PairType.EXOTIC))
        without_hedge = rp.get_half_life_threshold(hedge_active=False)
        with_hedge = rp.get_half_life_threshold(hedge_active=True)
        assert with_hedge > without_hedge


class TestMemecoinGate:
    def test_memecoin_blocks_on_trend(self):
        rp = DLMMRiskPolicy(default_risk_config_for_pair(PairType.MEMECOIN))
        ok, reason = rp.should_quote_memecoin(
            half_life=100.0, trending=True, tvl_usd=50000.0,
        )
        assert not ok
        assert "Trending" in reason

    def test_memecoin_blocks_on_long_half_life(self):
        rp = DLMMRiskPolicy(default_risk_config_for_pair(PairType.MEMECOIN))
        ok, reason = rp.should_quote_memecoin(
            half_life=9999.0, trending=False, tvl_usd=50000.0,
        )
        assert not ok
        assert "Half-life" in reason

    def test_memecoin_blocks_on_low_tvl(self):
        rp = DLMMRiskPolicy(default_risk_config_for_pair(PairType.MEMECOIN))
        ok, reason = rp.should_quote_memecoin(
            half_life=100.0, trending=False, tvl_usd=100.0,
        )
        assert not ok
        assert "TVL" in reason

    def test_memecoin_blocks_on_kill_switch(self):
        rp = DLMMRiskPolicy(default_risk_config_for_pair(PairType.MEMECOIN))
        rp.state.kill_switch_fired = True
        rp.state.kill_reason = "TVL rug"
        ok, _ = rp.should_quote_memecoin(
            half_life=100.0, trending=False, tvl_usd=50000.0,
        )
        assert not ok

    def test_non_memecoin_always_ok(self):
        rp = DLMMRiskPolicy(default_risk_config_for_pair(PairType.BLUECHIP))
        ok, _ = rp.should_quote_memecoin(
            half_life=9999.0, trending=True, tvl_usd=0.0,
        )
        assert ok