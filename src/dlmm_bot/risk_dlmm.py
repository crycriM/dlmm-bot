"""
DLMM-specific risk: rug/liquidity-withdrawal kill-switch and pair-type
inventory caps.

This extends mm_core.risk_policy with checks that only apply to DLMM
spot LP positions (no liquidation risk, but rug/custody risk instead).

Pair types (per dlmm plan §5):
  - bluechip (SOL/USDC): full ladder, low kill-switch sensitivity
  - memecoin (MEMECOIN/SOL): one-sided cap, strict gate, rug kill-switch
  - exotic (large-cap/large-cap): both legs hedgeable, relaxed trend gate

The rug monitor is a separate consumer per shared-arch §5 — a stuck
keeper can't block kill-switch detection.  This module provides the
detection logic that the separate consumer runs.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from mm_core.risk_policy import Decision, RiskConfig


class PairType(str, Enum):
    BLUECHIP = "bluechip"
    MEMECOIN = "memecoin"
    EXOTIC = "exotic"


@dataclass
class DLMMRiskConfig:
    """Risk parameters specific to DLMM (on top of mm_core RiskConfig)."""
    pair_type: PairType = PairType.BLUECHIP
    # Rug / liquidity-withdrawal kill-switch
    tvl_drop_pct: float = 30.0            # fire if TVL drops this % in one interval
    tvl_window_s: float = 300.0            # lookback window for TVL drop
    min_tvl_usd: float = 5000.0            # below this, stop quoting
    # Inventory caps (pair-type specific)
    max_inventory_pct: float = 80.0         # max % of capital in one side
    one_sided_only: bool = False           # memecoin: only quote leg you're willing to hold
    # Drawdown (tighter for memecoin)
    max_drawdown_pct: float = 10.0
    # Wider tolerances for bluechip
    bluechip_half_life_max: float = 7200.0   # 2h
    memecoin_half_life_max: float = 1800.0   # 30 min
    exotic_half_life_max: float = 3600.0     # 1h, relaxed when hedge active


@dataclass
class DLMMRiskState:
    """State for DLMM-specific risk checks."""
    last_tvl: float | None = None
    last_tvl_ts: float | None = None
    peak_equity: float = float("-inf")
    kill_switch_fired: bool = False
    kill_reason: str = ""


class DLMMRiskPolicy:
    """Additional risk checks for DLMM positions.

    This is designed to run alongside (not replace) the shared
    mm_core.RiskPolicy. The shared policy handles drawdown, gap, regime
    gate, markout, and inventory caps. This adds:
    - Rug / liquidity-withdrawal detection (TVL drop)
    - Minimum TVL gate
    - Pair-type specific inventory cap enforcement
    - One-sided posture for memecoins
    """

    def __init__(self, cfg: DLMMRiskConfig = DLMMRiskConfig()):
        self.cfg = cfg
        self.state = DLMMRiskState()

    def evaluate_tvl(self, ts: float, tvl_usd: float | None) -> tuple[bool, str]:
        """Check TVL for rug/liquidity-withdrawal. Returns (kill, reason).

        kill=True means the keeper should emergency_exit immediately.
        """
        if tvl_usd is None:
            return False, ""

        # Minimum TVL gate
        if tvl_usd < self.cfg.min_tvl_usd:
            self.state.kill_switch_fired = True
            self.state.kill_reason = f"TVL below minimum: ${tvl_usd:.0f} < ${self.cfg.min_tvl_usd:.0f}"
            return True, self.state.kill_reason

        # TVL drop detection
        if self.state.last_tvl is not None and self.state.last_tvl_ts is not None:
            elapsed = ts - self.state.last_tvl_ts
            if elapsed > 0 and elapsed <= self.cfg.tvl_window_s:
                drop_pct = (self.state.last_tvl - tvl_usd) / self.state.last_tvl * 100.0
                if drop_pct > self.cfg.tvl_drop_pct:
                    self.state.kill_switch_fired = True
                    self.state.kill_reason = (
                        f"TVL dropped {drop_pct:.1f}% in {elapsed:.0f}s: "
                        f"${self.state.last_tvl:.0f} → ${tvl_usd:.0f}"
                    )
                    return True, self.state.kill_reason

        self.state.last_tvl = tvl_usd
        self.state.last_tvl_ts = ts
        return False, ""

    def check_inventory_cap(
        self,
        base_inventory: float,
        quote_inventory_in_base: float,
        total_capital_base: float,
    ) -> tuple[bool, str]:
        """Check if inventory exceeds pair-type-specific cap.

        Returns (exceeded, reason). For memecoins, one_sided_only means
        we should only hold one leg — any two-sided position triggers.
        """
        if total_capital_base <= 0:
            return False, ""

        if self.cfg.one_sided_only:
            # Memecoin: should only have one side
            if base_inventory > 0 and quote_inventory_in_base > 0:
                return True, f"Two-sided position on memecoin pair (base={base_inventory}, quote={quote_inventory_in_base})"
            return False, ""

        total = base_inventory + quote_inventory_in_base
        if total <= 0:
            return False, ""

        # Check if either side exceeds max_inventory_pct
        max_base = total_capital_base * self.cfg.max_inventory_pct / 100.0
        if abs(base_inventory) > max_base:
            return True, f"Base inventory {base_inventory:.4f} exceeds cap {max_base:.4f}"
        if abs(quote_inventory_in_base) > max_base:
            return True, f"Quote inventory {quote_inventory_in_base:.4f} exceeds cap {max_base:.4f}"

        return False, ""

    def get_half_life_threshold(self, hedge_active: bool = False) -> float:
        """Get pair-type-specific OU half-life gate threshold."""
        if self.cfg.pair_type == PairType.BLUECHIP:
            return self.cfg.bluechip_half_life_max
        elif self.cfg.pair_type == PairType.MEMECOIN:
            return self.cfg.memecoin_half_life_max
        elif self.cfg.pair_type == PairType.EXOTIC:
            # Relax when hedge active (trend is hedged, not toxic)
            if hedge_active:
                return self.cfg.exotic_half_life_max * 2.0
            return self.cfg.exotic_half_life_max
        return 3600.0

    def should_quote_memecoin(
        self,
        half_life: float,
        trending: bool,
        tvl_usd: float | None,
    ) -> tuple[bool, str]:
        """Strict gate for memecoin pairs.

        Memecoins need: short half-life, not trending, sufficient TVL,
        and no rug signal.
        """
        if self.cfg.pair_type != PairType.MEMECOIN:
            return True, ""

        if self.state.kill_switch_fired:
            return False, f"Kill switch active: {self.state.kill_reason}"

        if half_life > self.cfg.memecoin_half_life_max:
            return False, f"Half-life {half_life:.0f}s too long for memecoin"

        if trending:
            return False, "Trending market — memecoin MM disabled"

        if tvl_usd is not None and tvl_usd < self.cfg.min_tvl_usd:
            return False, f"TVL ${tvl_usd:.0f} below minimum ${self.cfg.min_tvl_usd:.0f}"

        return True, ""


def default_risk_config_for_pair(pair_type: PairType) -> DLMMRiskConfig:
    """Factory for pair-type-specific default configs."""
    if pair_type == PairType.BLUECHIP:
        return DLMMRiskConfig(
            pair_type=PairType.BLUECHIP,
            max_inventory_pct=80.0,
            one_sided_only=False,
            max_drawdown_pct=10.0,
            tvl_drop_pct=40.0,
            min_tvl_usd=5000.0,
        )
    elif pair_type == PairType.MEMECOIN:
        return DLMMRiskConfig(
            pair_type=PairType.MEMECOIN,
            max_inventory_pct=50.0,
            one_sided_only=True,
            max_drawdown_pct=8.0,
            tvl_drop_pct=20.0,
            min_tvl_usd=10000.0,
            tvl_window_s=120.0,
        )
    elif pair_type == PairType.EXOTIC:
        return DLMMRiskConfig(
            pair_type=PairType.EXOTIC,
            max_inventory_pct=70.0,
            one_sided_only=False,
            max_drawdown_pct=12.0,
            tvl_drop_pct=35.0,
            min_tvl_usd=8000.0,
        )
    return DLMMRiskConfig()


__all__ = [
    "PairType", "DLMMRiskConfig", "DLMMRiskState", "DLMMRiskPolicy",
    "default_risk_config_for_pair",
]