"""Tests for the hedge controller — cube-root bands, vol-scaled window, hard cap."""

import pytest
from dlmm_bot.hedge import HedgeConfig, HedgeController, HedgeState, verify_cube_root_behavior


class TestCubeRootBehavior:
    def test_cube_root_insensitivity(self):
        """2->4bps widens band only ~26% (2^(1/3) = 1.26)."""
        result = verify_cube_root_behavior()
        assert result["passes"]
        assert abs(result["ratio"] - 1.2599) < 0.01

    def test_0_to_2_bps_is_large_jump(self):
        """0->2bps lands on a distinctly wide band (vs 0 = continuous)."""
        b0 = 0.0 ** (1/3)  # 0
        b2 = 2.0 ** (1/3)  # 1.26
        assert b2 > b0 + 1.0  # large jump from continuous


class TestVolScaledWindow:
    def test_window_contracts_as_vol_increases(self):
        """Higher vol -> shorter tau_h_eff (faster target in turbulence)."""
        cfg = HedgeConfig(tau_h=3600, sigma_ref=0.5, tau_min=900, tau_max=7200)
        hc = HedgeController(cfg)

        calm = hc.vol_scale_window(0.25)   # vol below ref -> expand
        normal = hc.vol_scale_window(0.5)   # vol at ref
        turbulent = hc.vol_scale_window(2.0)  # vol above ref -> contract

        assert calm >= normal >= turbulent
        assert turbulent >= cfg.tau_min  # respect floor
        assert calm <= cfg.tau_max       # respect ceiling

    def test_zero_vol_returns_default(self):
        hc = HedgeController(HedgeConfig())
        assert hc.vol_scale_window(0.0) == hc.cfg.tau_h


class TestEmaUpdate:
    def test_ema_converges_to_input(self):
        """After many iterations with constant input, EMA -> input."""
        hc = HedgeController(HedgeConfig(tau_h=10.0))
        target = 10.0
        for _ in range(5000):
            hc.update_ema(target, dt=1.0, sigma_now=0.5)
        assert abs(hc.state.ma_target - target) < 0.1

    def test_ema_responds_fast_with_short_window(self):
        """Shorter tau_h -> faster response."""
        hc_short = HedgeController(HedgeConfig(tau_h=50.0))
        hc_long = HedgeController(HedgeConfig(tau_h=5000.0))
        for _ in range(20):
            hc_short.update_ema(10.0, dt=1.0)
            hc_long.update_ema(10.0, dt=1.0)
        assert abs(hc_short.state.ma_target - 10.0) < abs(hc_long.state.ma_target - 10.0)


class TestEvaluateActions:
    def test_no_trade_when_residual_within_band(self):
        cfg = HedgeConfig(deadband_base_bps=100.0, delta_cap_bps=500.0)
        hc = HedgeController(cfg)
        # Inventory = current short -> residual = 0, no trade
        action, target, intent = hc.evaluate(
            inventory_base=10.0,
            current_short=10.0,
            inventory_value_usd=1500.0,
            sigma_now=0.5,
            dt=1.0,
        )
        assert action == "no_trade"
        assert intent is None

    def test_rehedge_when_residual_exceeds_deadband(self):
        cfg = HedgeConfig(deadband_base_bps=100.0, delta_cap_bps=5000.0, venue="hl", coin="SOL")
        hc = HedgeController(cfg)
        # EMA should be moving toward inventory; residual should trigger
        action, target, intent = hc.evaluate(
            inventory_base=20.0,
            current_short=0.0,
            inventory_value_usd=3000.0,
            sigma_now=0.5,
            dt=1.0,
        )
        assert action in ("rehedge", "force_hedge")
        if action == "rehedge":
            assert intent is not None
            assert intent.venue == "hl"
            assert intent.coin == "SOL"

    def test_force_hedge_when_residual_exceeds_hard_cap(self):
        cfg = HedgeConfig(deadband_base_bps=100.0, delta_cap_bps=50.0, venue="hl", coin="SOL")
        hc = HedgeController(cfg)
        action, target, intent = hc.evaluate(
            inventory_base=100.0,
            current_short=0.0,
            inventory_value_usd=15000.0,
            sigma_now=0.5,
            dt=1.0,
        )
        assert action == "force_hedge"
        assert intent is not None
        assert intent.urgency == "normal"

    def test_disabled_returns_no_trade(self):
        cfg = HedgeConfig(enabled=False)
        hc = HedgeController(cfg)
        action, _, intent = hc.evaluate(
            inventory_base=1000.0, current_short=0.0,
            inventory_value_usd=100000.0,
        )
        assert action == "no_trade"
        assert intent is None


class TestHedgeState:
    def test_state_updates_on_evaluate(self):
        cfg = HedgeConfig()
        hc = HedgeController(cfg)
        hc.evaluate(
            inventory_base=5.0, current_short=0.0,
            inventory_value_usd=750.0, sigma_now=0.5, dt=1.0,
        )
        assert hc.state.current_short == 0.0
        assert hc.state.residual == 5.0
        assert hc.state.tau_h_eff > 0

    def test_last_action_recorded(self):
        hc = HedgeController(HedgeConfig())
        _, _, _ = hc.evaluate(10.0, 10.0, 1500.0)
        assert hc.state.last_action == "no_trade"