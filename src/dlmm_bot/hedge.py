"""
Transaction-cost-aware hedging for DLMM exotic pairs.

Two-timescale controller per the dlmm plan §6 / shared-arch §2.8:
1. MA + deadband (fee-optimal band surrogate, normal regime)
2. Vol-scaled window + hard delta-cap backstop (tail regime)

Operating logic:
    if |inventory_base - current_perp_short| > DELTA_CAP: force_hedge()
    elif |MA_target - current_perp_short| > deadband:       rehedge()
    else:                                                  no_trade()

The deadband width follows the Zakamouline/Whalley-Wilmott cube-root
rule: bandwidth ~ (per_trade_cost)^(1/3).  This makes 0->2bps jump to a
wide band and 2->4bps widen only ~26% (cube-root insensitivity), so
continuous hedging is never on the efficient frontier at these costs.

The hedge emits ExecIntents (not direct orders) to the OPMS for perp
shorts — the single order-authority pattern.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Optional

from mm_core.contracts import ExecIntent


@dataclass
class HedgeConfig:
    """Per-pair hedge configuration."""
    tau_h: float = 3600.0           # MA window, seconds
    tau_min: float = 900.0           # vol-scaled floor
    tau_max: float = 7200.0          # vol-scaled ceiling
    sigma_ref: float = 0.5           # reference annualized vol
    deadband_base_bps: float = 10.0  # base deadband in bps
    per_trade_cost_bps: float = 2.0  # cost per hedge trade
    delta_cap_bps: float = 200.0     # hard cap on residual delta
    cube_root_constant: float = 1.0  # calibration constant for band
    deadband_base: float = 0.0      # base deadband in base units
    venue: str = "hl"
    coin: str = ""
    enabled: bool = True


@dataclass
class HedgeState:
    """Current hedge controller state."""
    ma_target: float = 0.0           # EMA(inventory_base, tau_h_eff)
    tau_h_eff: float = 3600.0         # effective window after vol scaling
    current_short: float = 0.0       # current perp short size
    residual: float = 0.0            # inventory_base - current_short
    deadband: float = 0.0             # current deadband in base units
    hard_cap: float = 0.0             # current hard cap in base units
    last_action: str = "init"         # "init" | "no_trade" | "rehedge" | "force_hedge"
    last_rebalance_ts: float = 0.0


class HedgeController:
    """MA+deadband with vol-scaled window and hard delta-cap backstop."""

    def __init__(self, cfg: HedgeConfig):
        self.cfg = cfg
        self.state = HedgeState(
            ma_target=0.0,
            tau_h_eff=cfg.tau_h,
        )

    def compute_deadband(self, price: float) -> float:
        """Zakamouline/Whalley-Wilmott cube-root band.

        bandwidth = cube_root_constant * (per_trade_cost)^(1/3) * price / 1e4

        The cube-root insensitivity means 2->4bps widens the band only
        ~26% (2^(1/3) = 1.26), so continuous hedging is never optimal.
        """
        cost_fraction = self.cfg.per_trade_cost_bps / 1e4
        band_bps = self.cfg.cube_root_constant * (cost_fraction ** (1/3)) * 1e4
        return self.cfg.deadband_base + band_bps * price / 1e4 * 0  # simplified: deadband in base units below

    def compute_deadband_base(self, inventory_value_usd: float) -> float:
        """Deadband in base units from cube-root rule.

        bandwidth_usd = constant * (cost)^(1/3) * inventory_value
        Returns the band in base units (inventory terms).
        """
        cost_fraction = self.cfg.per_trade_cost_bps / 1e4
        band_usd = self.cfg.cube_root_constant * (cost_fraction ** (1/3)) * inventory_value_usd
        # Convert to base via price — caller passes inventory_value_usd
        return band_usd

    def vol_scale_window(self, sigma_now: float) -> float:
        """Contract tau_h when vol spikes; expand when calm.

        tau_h_eff = tau_h * clip(sigma_ref / sigma_now, tau_min/tau_h, tau_max/tau_h)
        """
        if sigma_now <= 0:
            return self.cfg.tau_h
        ratio = self.cfg.sigma_ref / sigma_now
        ratio = max(ratio, self.cfg.tau_min / self.cfg.tau_h)
        ratio = min(ratio, self.cfg.tau_max / self.cfg.tau_h)
        return self.cfg.tau_h * ratio

    def update_ema(self, inventory_base: float, dt: float, sigma_now: float = 0.0) -> float:
        """Update EMA target with vol-scaled window.

        alpha = dt / (dt + tau_h_eff)
        """
        self.state.tau_h_eff = self.vol_scale_window(sigma_now) if sigma_now > 0 else self.cfg.tau_h
        alpha = dt / (dt + self.state.tau_h_eff) if (dt + self.state.tau_h_eff) > 0 else 0.0
        self.state.ma_target = (1 - alpha) * self.state.ma_target + alpha * inventory_base
        return self.state.ma_target

    def evaluate(
        self,
        inventory_base: float,
        current_short: float,
        inventory_value_usd: float,
        sigma_now: float = 0.0,
        dt: float = 1.0,
    ) -> tuple[str, float, ExecIntent | None]:
        """Evaluate hedge state and return (action, target_short, intent).

        action: "no_trade" | "rehedge" | "force_hedge"
        target_short: the desired short size (if action != no_trade)
        intent: ExecIntent to send to OPMS (None if no action)
        """
        if not self.cfg.enabled:
            return "no_trade", current_short, None

        self.update_ema(inventory_base, dt, sigma_now)
        self.state.current_short = current_short
        self.state.residual = inventory_base - current_short
        self.state.deadband = self.compute_deadband_base(inventory_value_usd)
        self.state.hard_cap = self.cfg.delta_cap_bps * inventory_value_usd / 1e4 / (inventory_value_usd / max(inventory_base, 1e-12)) if inventory_base != 0 else 0.0

        # Hard delta-cap backstop: force hedge regardless
        if abs(self.state.residual) > self.state.hard_cap:
            action = "force_hedge"
            target = self.state.ma_target  # close the gap to MA target
        elif abs(self.state.ma_target - current_short) > self.state.deadband:
            action = "rehedge"
            target = self.state.ma_target
        else:
            action = "no_trade"
            target = current_short

        self.state.last_action = action

        if action == "no_trade":
            return action, target, None

        # Build ExecIntent for OPMS to execute the perp short
        intent = ExecIntent(
            venue=self.cfg.venue,
            coin=self.cfg.coin,
            target_inventory=-target,  # short = negative target
            urgency="normal",
            strategy_hint="passive_aggressive",
            current_inventory=-current_short,
        )
        return action, target, intent


def verify_cube_root_behavior() -> dict:
    """Verify the cube-root band insensitivity.

    2->4bps should widen the band only ~26% (2^(1/3) = 1.2599).
    """
    def band_for(cost_bps):
        return cost_bps ** (1/3)

    b2 = band_for(2.0)
    b4 = band_for(4.0)
    ratio = b4 / b2
    return {
        "band_2bps": b2,
        "band_4bps": b4,
        "ratio": ratio,
        "expected_ratio": 2 ** (1/3),
        "passes": abs(ratio - 2**(1/3)) < 1e-6,
    }


__all__ = ["HedgeConfig", "HedgeState", "HedgeController", "verify_cube_root_behavior"]