"""
DLMM keeper: off-chain poll → evaluate → actuate loop.

Per the dlmm plan §9 and the shared-arch §5, the keeper:
1. Polls market data (MarketSnapshot from bus/WS — or get_state from exec bridge)
2. Runs mm_core.evaluate_regime + RiskPolicy (+ DLMM-specific risk checks)
3. Builds ladder via build_ladder with AS-driven center/skew
4. Actuates:
   - QUOTE/WIDEN → refresh_bundle when drift exceeds threshold (gas is a PnL cost)
   - STOP_QUOTING → withdraw to single-sided safe leg
   - DE_RISK/EMERGENCY_EXIT → dlmm_exec: stop bids, drain asks, TWAP remainder
5. Logs a per-cycle decision record (JSON lines) — the shadow-mode artifact

The keeper never imports opms; it talks to exec bridge + bus.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Callable, Optional

from mm_core.contracts import MarketSnapshot, ExecIntent, QuoteSpec
from mm_core.inventory import TwoTokenInventory, Caps
from mm_core.markout import MarkoutTracker
from mm_core.pnl import Fill, PnLLedger
from mm_core.risk_policy import Decision, RiskConfig, RiskPolicy
from mm_core.regime import evaluate_regime, GateConfig, should_quote
from mm_core.as_core import gueant_reservation_price, gueant_half_spread
from mm_core.vol import VOLATILITY_MODELS

from dlmm_bot.grid import VenueGrid
from dlmm_bot.ladder import build_ladder, LadderConfig, LadderLevel
from dlmm_bot.config import DLMMConfig
from dlmm_bot.exec_bridge import ExecBridge, FakeExecBridge, ExecResult
from dlmm_bot.risk_dlmm import DLMMRiskPolicy, DLMMRiskConfig, PairType
from dlmm_bot.hedge import HedgeController, HedgeConfig

logger = logging.getLogger(__name__)

DRIFT_THRESHOLD_BINS = 3
REFRESH_GAS_LAMPORTS = 50000  # ~0.00005 SOL per refresh


@dataclass
class KeeperConfig:
    """Configuration for the DLMM keeper loop."""
    dlmm: DLMMConfig = field(default_factory=DLMMConfig)
    grid: VenueGrid = field(default_factory=lambda: VenueGrid(150.0, 2, 6, 9))
    refresh_interval: float = 5.0      # seconds between poll cycles
    drift_threshold_bins: int = 3      # re-center when active bin drifts this many bins
    inv_tolerance: float = 0.25         # inventory skew tolerance
    pair_type: PairType = PairType.BLUECHIP
    hedge_config: HedgeConfig | None = None
    pool_address: str = ""
    dry_run: bool = True                # shadow mode: log decisions, don't submit tx
    position_id: str | None = None     # current LP position
    max_history: int = 500              # price history deque size
    sentinel_path: str | None = None   # kill sentinel file path (written by tvl_monitor.py)


@dataclass
class CycleRecord:
    """One keeper cycle's decision record — the shadow-mode artifact."""
    ts: float
    active_bin: int
    mid: float
    regime_half_life: float
    regime_hurst: float
    regime_trending: bool
    decision: str
    urgency: str
    action: str
    inventory_base: float
    inventory_quote: float
    r_reservation: float
    half_spread: float
    ladder_center: int
    ladder_levels: int
    refresh_needed: bool
    refresh_reason: str
    pnl_total: float = 0.0
    dry_run: bool = True
    net_delta: float = 0.0
    sigma: float = 0.0


class Keeper:
    """The DLMM MM keeper loop."""

    def __init__(
        self,
        cfg: KeeperConfig,
        exec_bridge: ExecBridge | FakeExecBridge,
        publish: Callable | None = None,
    ):
        self.cfg = cfg
        self.exec = exec_bridge
        self.publish = publish or (lambda subject, payload: None)

        # Price history for regime/vol estimation
        self._price_history: deque = deque(maxlen=cfg.max_history)
        self._ts_history: deque = deque(maxlen=cfg.max_history)

        # mm_core components
        risk_cfg = RiskConfig(gate=GateConfig())
        self.risk_policy = RiskPolicy(cfg=risk_cfg)
        self.markout = MarkoutTracker()
        self.pnl = PnLLedger(venue="meteora", symbol=cfg.pool_address or "unknown")

        # DLMM-specific risk
        dlmm_risk_cfg = DLMMRiskConfig(pair_type=cfg.pair_type)
        self.dlmm_risk = DLMMRiskPolicy(dlmm_risk_cfg)

        # Hedge controller (optional — only for exotic pairs)
        self.hedge: HedgeController | None = None
        if cfg.hedge_config and cfg.hedge_config.enabled:
            self.hedge = HedgeController(cfg.hedge_config)

        # State
        self._running = False
        self._active_bin: int = 0
        self._center_bin: int = 0  # last deployed center
        self._current_position_id: str | None = cfg.position_id
        self._inventory_base: float = 0.0
        self._inventory_quote: float = 0.0  # in quote tokens
        self._halted = False
        self._cycle_count = 0
        self._decision_log: list[CycleRecord] = []

    async def run(self, max_cycles: int | None = None) -> None:
        """Main loop. max_cycles=None runs until stopped."""
        self._running = True
        logger.info("DLMM keeper started (dry_run=%s)", self.cfg.dry_run)
        while self._running:
            try:
                await self._cycle()
            except Exception as e:
                logger.exception("Keeper cycle error: %s", e)
            self._cycle_count += 1
            if max_cycles and self._cycle_count >= max_cycles:
                break
            await asyncio.sleep(self.cfg.refresh_interval)

    def stop(self):
        self._running = False

    async def _cycle(self) -> CycleRecord:
        """One poll→evaluate→actuate cycle."""
        ts = time.time()

        if self.cfg.sentinel_path and os.path.exists(self.cfg.sentinel_path):
            with open(self.cfg.sentinel_path) as f:
                reason = f.read().strip()
            logger.critical("Sentinel kill: %s", reason)
            await self._emergency_exit()
            os.remove(self.cfg.sentinel_path)
            return self._make_record(
                ts, Decision.EMERGENCY_EXIT, "emergency", "sentinel_kill",
                refresh_reason=reason,
            )

        # 1. Poll state
        state_result = self.exec.get_state(self.cfg.pool_address)
        if not state_result.ok or state_result.data is None:
            logger.warning("get_state failed: %s", state_result.error)
            return self._make_record(ts, Decision.STOP_QUOTING, "error", "no_state")

        state = state_result.data
        self._active_bin = state.get("active_bin", state.get("activeBin", 0))
        balances = state.get("balances", state.get("balances", {}))
        self._inventory_base = float(balances.get("base", 0.0))
        self._inventory_quote = float(balances.get("quote", 0.0))

        mid = self.cfg.grid.price_from_bin(self._active_bin)
        self._price_history.append(mid)
        self._ts_history.append(ts)
        self.pnl.mark(ts, mid)

        # TVL from state (if available from bus publisher)
        tvl_usd = state.get("tvl_usd")

        # 2. DLMM-specific risk checks (rug, TVL, inventory caps)
        kill, kill_reason = self.dlmm_risk.evaluate_tvl(ts, tvl_usd)
        if kill:
            logger.critical("Rug kill-switch: %s", kill_reason)
            await self._emergency_exit()
            return self._make_record(
                ts, Decision.EMERGENCY_EXIT, "emergency", "rug_kill_switch",
                mid=mid, refresh_reason=kill_reason,
            )

        inv_cap_breached, inv_reason = self.dlmm_risk.check_inventory_cap(
            self._inventory_base,
            self._inventory_quote / mid if mid > 0 else 0.0,
            self.cfg.dlmm.capital / mid if mid > 0 else 0.0,
        )
        if inv_cap_breached:
            logger.warning("Inventory cap breached: %s", inv_reason)
            await self._de_risk()
            return self._make_record(
                ts, Decision.DE_RISK, "normal", "inventory_cap",
                mid=mid, refresh_reason=inv_reason,
            )

        # 3. Regime + AS + shared risk policy
        price_history = list(zip(self._ts_history, self._price_history))
        regime = evaluate_regime(price_history)
        # Adjust gate for pair type
        gate = GateConfig(
            max_half_life=self.dlmm_risk.get_half_life_threshold(
                hedge_active=self.hedge is not None
            ),
        )

        inventory = TwoTokenInventory(
            base=self._inventory_base,
            quote=self._inventory_quote,
            mid=mid,
            target_base_share=0.5,
            _caps=Caps(max_position=self.cfg.dlmm.capital * 0.8 / mid, critical_position=self.cfg.dlmm.capital * 0.9 / mid),
        )

        equity = self.pnl.explain(ts, mid).total_pnl + self.cfg.dlmm.capital
        avg_markout = self.markout.avg_markout_bps(30.0)
        decision, urgency = self.risk_policy.evaluate(
            ts=ts, mid=mid, equity=equity,
            inventory=inventory, regime=regime,
            avg_markout_bps=avg_markout,
        )

        # 4. Actuate
        action = "none"
        refresh_needed = False
        refresh_reason = ""

        if decision == Decision.EMERGENCY_EXIT:
            await self._emergency_exit()
            action = "emergency_exit"
        elif decision == Decision.STOP_QUOTING:
            await self._stop_quoting()
            action = "stop_quoting"
        elif decision == Decision.DE_RISK:
            await self._de_risk()
            action = "de_risk"
        else:
            # QUOTE or WIDEN: build ladder and check if refresh needed
            vol_model = VOLATILITY_MODELS.get(
                self.cfg.dlmm.vol_model if hasattr(self.cfg.dlmm, 'vol_model') else "close_to_close",
                VOLATILITY_MODELS["close_to_close"],
            )
            sigma = vol_model(price_history)

            r = gueant_reservation_price(
                mid=mid,
                q=inventory.net_delta(),
                gamma=self.cfg.dlmm.gamma,
                sigma=sigma,
                kappa=self.cfg.dlmm.kappa,
            )
            half_spread = gueant_half_spread(
                gamma=self.cfg.dlmm.gamma,
                sigma=sigma,
                kappa=self.cfg.dlmm.kappa,
            )

            # Skew from inventory error
            total_inv_base = self._inventory_base + (self._inventory_quote / mid if mid > 0 else 0)
            base_share = self._inventory_base / total_inv_base if total_inv_base > 0 else 0.5
            skew = max(-1.0, min(1.0, 2.0 * (base_share - 0.5)))

            if decision == Decision.WIDEN:
                half_spread *= 2.0  # widen factor

            ladder = build_ladder(
                grid=self.cfg.grid,
                active_bin=self._active_bin,
                r=r,
                S=mid,
                half_spread=half_spread,
                skew=skew,
                cfg=LadderConfig(
                    levels=self.cfg.dlmm.levels,
                    inner_offset=self.cfg.dlmm.inner_offset,
                    capital=self.cfg.dlmm.capital,
                    level_weight=self.cfg.dlmm.level_weight,
                ),
            )

            center_bin = min(l.level.bin_id for l in ladder if l.side == "bid") if ladder else self._active_bin
            drift = abs(self._active_bin - self._center_bin) if self._center_bin else 0

            if self._current_position_id is None:
                refresh_needed = True
                refresh_reason = "initial_deposit"
                action = "initial_deposit"
                await self._deposit_ladder(ladder)
            elif drift >= self.cfg.drift_threshold_bins:
                refresh_needed = True
                refresh_reason = f"drift_{drift}_bins"
                action = "refresh"
                await self._refresh_ladder(ladder)
            elif decision == Decision.WIDEN:
                refresh_needed = True
                refresh_reason = "widen_spread"
                action = "refresh_widen"
                await self._refresh_ladder(ladder)
            else:
                action = "hold"

            # Hedge evaluation for exotic pairs
            if self.hedge and self.cfg.pair_type == PairType.EXOTIC:
                hedge_action, hedge_target, hedge_intent = self.hedge.evaluate(
                    inventory_base=self._inventory_base,
                    current_short=0.0,  # read from OPMS in production
                    inventory_value_usd=total_inv_base * mid,
                    sigma_now=regime.vol,
                    dt=self.cfg.refresh_interval,
                )
                if hedge_intent:
                    self.publish(f"ctrl.{self.cfg.hedge_config.venue}.{self.cfg.hedge_config.coin}", asdict(hedge_intent))

            record = self._make_record(
                ts, decision, urgency, action, mid=mid,
                r=r, half_spread=half_spread,
                center_bin=center_bin if 'center_bin' in dir() else self._active_bin,
                refresh_reason=refresh_reason,
                refresh_needed=refresh_needed,
                net_delta=inventory.net_delta(),
                sigma=sigma,
            )
            self._log_decision(record)
            return record

        record = self._make_record(ts, decision, urgency, action, mid=mid, refresh_reason=refresh_reason)
        self._log_decision(record)
        return record

    async def _deposit_ladder(self, ladder: list[LadderLevel]) -> None:
        """Initial deposit of single-sided positions."""
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would deposit %d levels", len(ladder))
            return
        bids = [l for l in ladder if l.side == "bid"]
        asks = [l for l in ladder if l.side == "ask"]

        # Deposit bid side (quote token below mid)
        if bids:
            result = self.exec.deposit_single_sided(
                pool=self.cfg.pool_address,
                side="bid",
                bin_ids=[l.bin_id for l in bids],
                amounts=[l.size for l in bids],
                strategy_type="Spot",
            )
            if result.ok:
                self._current_position_id = result.data.get("position_id") if result.data else None
                self._center_bin = min(l.bin_id for l in bids) + (
                    max(l.bin_id for l in bids) - min(l.bin_id for l in bids)
                ) // 2 if bids else self._active_bin
                logger.info("Deposited bid side: %s", result.tx_signatures)
            else:
                logger.error("Bid deposit failed: %s", result.error)

        # Deposit ask side (base token above mid)
        if asks:
            result = self.exec.deposit_single_sided(
                pool=self.cfg.pool_address,
                side="ask",
                bin_ids=[l.bin_id for l in asks],
                amounts=[l.size for l in asks],
                strategy_type="Spot",
            )
            if not result.ok:
                logger.error("Ask deposit failed: %s", result.error)

        # Gas as PnL cost
        self.pnl.on_cash_flow("rebalance", time.time(), -REFRESH_GAS_LAMPORTS / 1e9, label="deposit_gas")

    async def _refresh_ladder(self, ladder: list[LadderLevel]) -> None:
        """Refresh: withdraw → optional swap → redeposit via Jito bundle."""
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would refresh %d levels", len(ladder))
            return
        if self._current_position_id is None:
            logger.warning("Cannot refresh: no current position_id")
            return

        bids = [l for l in ladder if l.side == "bid"]
        asks = [l for l in ladder if l.side == "ask"]

        result = self.exec.refresh_bundle(
            withdraw_position_id=self._current_position_id,
            swap_spec=None,  # TODO: compute rebalance swap from inventory imbalance
            deposit_spec={
                "pool": self.cfg.pool_address,
                "bid_bins": [l.bin_id for l in bids],
                "ask_bins": [l.bin_id for l in asks],
                "bid_amounts": [l.size for l in bids],
                "ask_amounts": [l.size for l in asks],
            },
        )
        if result.ok:
            center = min(l.bin_id for l in ladder if l.side == "bid") if bids else self._active_bin
            self._center_bin = center
            logger.info("Refreshed ladder: %s", result.tx_signatures)
        else:
            logger.error("Refresh failed: %s", result.error)

        self.pnl.on_cash_flow("rebalance", time.time(), -REFRESH_GAS_LAMPORTS / 1e9, label="refresh_gas")

    async def _stop_quoting(self) -> None:
        """Stop quoting: withdraw to single-sided safe leg."""
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would stop quoting (withdraw to safe leg)")
            return
        if self._current_position_id:
            result = self.exec.withdraw(self._current_position_id)
            if result.ok:
                logger.info("Stopped quoting: withdrew position %s", self._current_position_id)
                self._current_position_id = None
            else:
                logger.error("Withdraw failed: %s", result.error)

    async def _de_risk(self) -> None:
        """De-risk: stop bids, drain asks, TWAP remainder over Jupiter."""
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would de-risk (stop bids, drain asks, TWAP)")
            return
        # Step 1: withdraw current position
        if self._current_position_id:
            result = self.exec.withdraw(self._current_position_id)
            if not result.ok:
                logger.error("De-risk withdraw failed: %s", result.error)
                return
            self._current_position_id = None
            self.pnl.on_cash_flow("rebalance", time.time(), -REFRESH_GAS_LAMPORTS / 1e9, label="de_risk_withdraw")

        # Step 2: TWAP the remainder via Jupiter swap
        # In production, this would chunk the remaining inventory into N
        # depth-checked swaps. For now, a single swap to safe leg.
        if self._inventory_base > 0.01:
            result = self.exec.swap(
                in_mint="base",
                out_mint="quote",
                amount=self._inventory_base,
                max_slippage_bps=100,
            )
            if result.ok:
                logger.info("De-risk swap executed: %s", result.tx_signatures)
                self.pnl.on_cash_flow("rebalance", time.time(), -REFRESH_GAS_LAMPORTS / 1e9, label="de_risk_swap")
            else:
                logger.error("De-risk swap failed: %s", result.error)

    async def _emergency_exit(self) -> None:
        """Emergency exit: Jito bundle withdraw all + swap to safe leg."""
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] EMERGENCY EXIT — would Jito bundle withdraw + swap")
            self._halted = True
            return
        # Withdraw everything
        if self._current_position_id:
            result = self.exec.withdraw(self._current_position_id, bps=100)
            if not result.ok:
                logger.critical("Emergency withdraw FAILED: %s", result.error)
            else:
                logger.critical("Emergency withdraw ok: %s", result.tx_signatures)
            self._current_position_id = None
        # Swap all base to quote (safe leg)
        if self._inventory_base > 0.001:
            result = self.exec.swap(
                in_mint="base",
                out_mint="quote",
                amount=self._inventory_base,
                max_slippage_bps=200,  # accept 2% slippage in emergency
            )
            if result.ok:
                logger.critical("Emergency swap ok: %s", result.tx_signatures)
            else:
                logger.critical("Emergency swap FAILED: %s", result.error)
        self._halted = True

    def _make_record(
        self, ts: float, decision: Decision, urgency: str, action: str,
        mid: float = 0.0, r: float = 0.0, half_spread: float = 0.0,
        center_bin: int = 0, refresh_reason: str = "",
        refresh_needed: bool = False,
    ) -> CycleRecord:
        return CycleRecord(
            ts=ts,
            active_bin=self._active_bin,
            mid=mid,
            regime_half_life=0.0,  # filled from regime if available
            regime_hurst=0.0,
            regime_trending=False,
            decision=decision.value,
            urgency=urgency,
            action=action,
            inventory_base=self._inventory_base,
            inventory_quote=self._inventory_quote,
            r_reservation=r,
            half_spread=half_spread,
            ladder_center=center_bin or self._center_bin,
            ladder_levels=self.cfg.dlmm.levels,
            refresh_needed=refresh_needed,
            refresh_reason=refresh_reason,
            pnl_total=self.pnl.explain(ts, mid).total_pnl if mid > 0 else 0.0,
            dry_run=self.cfg.dry_run,
        )

    def _log_decision(self, record: CycleRecord) -> None:
        """Log the decision record as JSON lines — the shadow-mode artifact."""
        self._decision_log.append(record)
        logger.info(
            "cycle %d: decision=%s action=%s mid=%.4f active_bin=%d",
            self._cycle_count, record.decision, record.action, record.mid, record.active_bin,
        )

    @property
    def decision_log(self) -> list[CycleRecord]:
        return self._decision_log


__all__ = ["Keeper", "KeeperConfig", "CycleRecord"]