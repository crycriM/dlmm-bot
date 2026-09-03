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
import hashlib
import json
import logging
import os
import time
from collections import deque
from dataclasses import asdict, dataclass, field
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
from dlmm_bot.event_log import EventLog, git_sha, _pkg_dir, _jsonable
from dlmm_bot.swap_observer import SwapObserver

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
    log_dir: str | None = None          # event-log dir; None disables file logging
    base_mint: str = ""
    quote_mint: str = ""
    executor_version: str = ""
    gas_token_price_quote: float | None = None


def dump_keeper_config(cfg: KeeperConfig) -> dict:
    """Deterministic, JSON-serializable config dump (run_started + config_hash)."""
    return {
        "dlmm": asdict(cfg.dlmm),
        "grid": {
            "ref_price": cfg.grid.ref_price,
            "bin_step_bps": cfg.grid.bin_step_bps,
            "base_decimals": cfg.grid.base_decimals,
            "quote_decimals": cfg.grid.quote_decimals,
        },
        "keeper": {
            "refresh_interval": cfg.refresh_interval,
            "drift_threshold_bins": cfg.drift_threshold_bins,
            "inv_tolerance": cfg.inv_tolerance,
            "pair_type": cfg.pair_type.value,
            "hedge_config": asdict(cfg.hedge_config) if cfg.hedge_config else None,
            "pool_address": cfg.pool_address,
            "dry_run": cfg.dry_run,
            "position_id": cfg.position_id,
            "max_history": cfg.max_history,
            "sentinel_path": cfg.sentinel_path,
            "base_mint": cfg.base_mint,
            "quote_mint": cfg.quote_mint,
            "executor_version": cfg.executor_version,
            "gas_token_price_quote": cfg.gas_token_price_quote,
        },
    }


def hash_keeper_config(cfg: KeeperConfig) -> str:
    """sha256 of the serialized config — rerun refuses mismatches without override."""
    payload = json.dumps(
        dump_keeper_config(cfg), sort_keys=True, default=str
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def rebuild_keeper_config(dump: dict) -> KeeperConfig:
    """Invert dump_keeper_config() for rerun (dlmm-logging-plan §7)."""
    from dlmm_bot.config import DLMMConfig

    dlmm_d = dict(dump["dlmm"])
    gate_d = dlmm_d.pop("gate", {})
    risk_d = dict(dlmm_d.pop("risk", {}))
    risk_d.pop("gate", None)  # nested GateConfig, dropped from RiskConfig rebuild
    k = dump["keeper"]
    return KeeperConfig(
        dlmm=DLMMConfig(
            **dlmm_d,
            gate=GateConfig(**gate_d),
            risk=RiskConfig(**risk_d),
        ),
        grid=VenueGrid(**dump["grid"]),
        refresh_interval=k.get("refresh_interval", 5.0),
        drift_threshold_bins=k.get("drift_threshold_bins", 3),
        inv_tolerance=k.get("inv_tolerance", 0.25),
        pair_type=PairType(k.get("pair_type", PairType.BLUECHIP.value)),
        hedge_config=HedgeConfig(**k["hedge_config"]) if k.get("hedge_config") else None,
        pool_address=k.get("pool_address", ""),
        dry_run=k.get("dry_run", True),
        position_id=k.get("position_id"),
        max_history=k.get("max_history", 500),
        sentinel_path=k.get("sentinel_path"),
        base_mint=k.get("base_mint", ""),
        quote_mint=k.get("quote_mint", ""),
        executor_version=k.get("executor_version", ""),
        gas_token_price_quote=k.get("gas_token_price_quote"),
    )


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
        event_log: EventLog | None = None,
        swap_observer: "SwapObserver | None" = None,
        sentinel_reader: Callable[[str], tuple[bool, str | None]] | None = None,
        sentinel_clearer: Callable[[str], None] | None = None,
    ):
        self.cfg = cfg
        self.exec = exec_bridge
        self.publish = publish or (lambda subject, payload: None)
        self.log = event_log
        self.swap_observer = swap_observer
        self._owns_log = False
        self._sentinel_reader = sentinel_reader or self._read_sentinel
        self._sentinel_clearer = sentinel_clearer or os.remove

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

        # Event-sourcing state (dlmm-logging-plan §1-2)
        self._config_hash: str = hash_keeper_config(cfg)
        self._run_id: str = self.log.run_id if self.log is not None else ""
        self._req_counter = 0
        self._last_mid: float | None = None
        self._last_mid_ts: float | None = None
        self._prev_active_bin: int | None = None
        self._last_regime = None
        self._run_started_emitted = bool(
            self.log is not None and getattr(self.log, "_seq", 0)
        )
        self._run_stopped_emitted = False

        if self.log is None and cfg.log_dir:
            self._run_id = self._new_run_id()
            path = os.path.join(cfg.log_dir, f"{self._run_id}.jsonl")
            self.log = EventLog(path, run_id=self._run_id, config_hash=self._config_hash)
            self._owns_log = True
        elif self.log is not None:
            if not self.log.config_hash:
                self.log.config_hash = self._config_hash
            elif self.log.config_hash != self._config_hash:
                raise ValueError("event-log config_hash does not match KeeperConfig")

        if self.swap_observer is None and self.log is not None:
            self.swap_observer = SwapObserver(self.log, cfg.grid, cfg.pool_address)
        if self.swap_observer is not None:
            self.swap_observer.set_fill_callback(
                lambda ts, side, price, size: self.markout.on_fill(
                    ts, side, price, size
                )
            )

    def emit(self, event_type: str, **fields) -> int:
        """Emit to the event log if one is attached; no-op otherwise."""
        if self.log is None:
            return 0
        return self.log.emit(event_type, **fields)

    def _next_req_id(self) -> str:
        self._req_counter += 1
        return f"req_{self._req_counter:06d}"

    @staticmethod
    def _read_sentinel(path: str) -> tuple[bool, str | None]:
        if not os.path.exists(path):
            return False, None
        with open(path, encoding="utf-8") as fh:
            return True, fh.read().strip()

    def _finish_cycle(self, record: CycleRecord) -> CycleRecord:
        """Persist decisions from every exit path, including failures/kills."""
        self._log_decision(record)
        return record

    def _ensure_run_started(self) -> None:
        if self.log is None or self._run_started_emitted:
            return
        self._run_id = self._run_id or self.log.run_id or self._new_run_id()
        self.log.emit(
            "run_started",
            config=dump_keeper_config(self.cfg),
            pool_address=self.cfg.pool_address,
            base_mint=self.cfg.base_mint,
            quote_mint=self.cfg.quote_mint,
            base_decimals=self.cfg.grid.base_decimals,
            quote_decimals=self.cfg.grid.quote_decimals,
            dry_run=self.cfg.dry_run,
            git_sha_dlmm=git_sha(_pkg_dir("dlmm_bot")),
            git_sha_mm_core=git_sha(_pkg_dir("mm_core")),
            executor_version=self.cfg.executor_version
                or type(self.exec).__name__,
            initial_inventory={
                "base": self._inventory_base,
                "quote": self._inventory_quote,
            },
        )
        self._run_started_emitted = True

    def _finalize_log(self, reason: str | None = None) -> None:
        if self.log is None or self._run_stopped_emitted:
            return
        self._ensure_run_started()
        self.log.emit("run_stopped", reason=reason or self._stop_reason())
        self._run_stopped_emitted = True
        if self._owns_log:
            self.log.close()

    @property
    def event_log_path(self) -> str | None:
        return self.log.path if self.log is not None else None

    async def run(self, max_cycles: int | None = None) -> None:
        """Main loop. max_cycles=None runs until stopped."""
        self._running = True
        self._run_id = self._run_id or self._new_run_id()
        self._ensure_run_started()
        logger.info("DLMM keeper started (dry_run=%s)", self.cfg.dry_run)
        try:
            while self._running:
                try:
                    await self._cycle()
                except Exception as e:
                    logger.exception("Keeper cycle error: %s", e)
                self._cycle_count += 1
                if max_cycles and self._cycle_count >= max_cycles:
                    break
                await asyncio.sleep(self.cfg.refresh_interval)
        finally:
            self._finalize_log()

    def _new_run_id(self) -> str:
        return f"run_{self.cfg.pool_address or 'pool'}_{time.time_ns()}"

    def _stop_reason(self) -> str:
        return "halted" if self._halted else "stop"

    def stop(self):
        self._running = False
        self._finalize_log()

    async def _cycle(self) -> CycleRecord:
        """One poll→evaluate→actuate cycle."""
        self._ensure_run_started()
        ts = time.time()
        if self.log is not None:
            self.log.set_cycle(self._cycle_count)

        # 0. Sentinel kill-switch (written by tvl_monitor.py) — now loggable
        if self.cfg.sentinel_path:
            sentinel_exists, sentinel_reason = self._sentinel_reader(
                self.cfg.sentinel_path
            )
            if sentinel_exists:
                logger.critical("Sentinel kill: %s", sentinel_reason)
            self.emit(
                "sentinel_check",
                path=self.cfg.sentinel_path,
                exists=sentinel_exists,
                reason=sentinel_reason,
            )
            if sentinel_exists:
                await self._emergency_exit()
                self._sentinel_clearer(self.cfg.sentinel_path)
                return self._finish_cycle(self._make_record(
                    ts, Decision.EMERGENCY_EXIT, "emergency", "sentinel_kill",
                    refresh_reason=sentinel_reason,
                ))

        # 1. Poll state
        state_result = self.exec.get_state(self.cfg.pool_address)
        if not state_result.ok or state_result.data is None:
            logger.warning("get_state failed: %s", state_result.error)
            self.emit(
                "state_observation", ts=ts, state=None, ok=False,
                error=state_result.error, mid=None,
            )
            return self._finish_cycle(
                self._make_record(ts, Decision.STOP_QUOTING, "error", "no_state")
            )

        state = state_result.data
        self._active_bin = state.get("active_bin", state.get("activeBin", 0))
        balances = state.get("balances", state.get("balances", {}))
        self._inventory_base = float(balances.get("base", 0.0))
        self._inventory_quote = float(balances.get("quote", 0.0))

        mid = self.cfg.grid.price_from_bin(self._active_bin)
        self._price_history.append(mid)
        self._ts_history.append(ts)
        self.pnl.mark(ts, mid)
        obs_seq = self.emit(
            "state_observation",
            ts=ts,  # explicit cycle ts → deterministic replay clock freeze
            state=_jsonable(state),
            active_bin=self._active_bin,
            balances=_jsonable(balances),
            balances_raw=_jsonable(state.get("balances_raw")),
            tvl_usd=state.get("tvl_usd"),
            mid=mid,
        )

        # Active-bin move: poll-derived price_change (swap stream dedupes later)
        if self._prev_active_bin is not None and self._active_bin != self._prev_active_bin:
            self.emit(
                "price_change",
                prev_active_bin=self._prev_active_bin,
                new_active_bin=self._active_bin,
                direction="up" if self._active_bin > self._prev_active_bin else "down",
                mid_before=self.cfg.grid.price_from_bin(self._prev_active_bin),
                mid_after=mid,
                source="poll",
                obs_seq=obs_seq,
            )
        self._prev_active_bin = self._active_bin

        # Per-cycle position observation (dlmm-logging-plan §4)
        if self._current_position_id is not None:
            pos_result = self.exec.get_position(self._current_position_id)
            pos_data = pos_result.data if isinstance(pos_result.data, dict) else {}
            self.emit(
                "position_observation",
                position_id=self._current_position_id,
                ok=pos_result.ok,
                error=pos_result.error,
                data=_jsonable(pos_result.data),
                active_bin=pos_data.get("active_bin", self._active_bin),
                bins=_jsonable(pos_data.get("bins", [])),
                claimable_fee_x=pos_data.get("claimable_fee_x"),
                claimable_fee_y=pos_data.get("claimable_fee_y"),
                claimable_fee_x_raw=pos_data.get("claimable_fee_x_raw"),
                claimable_fee_y_raw=pos_data.get("claimable_fee_y_raw"),
            )

        self._last_mid, self._last_mid_ts = mid, ts
        if self.swap_observer is not None:
            self.swap_observer.set_last_mid(mid, ts)
        self.emit("pnl_mark", ts=ts, mid=mid)

        # TVL from state (if available from bus publisher)
        tvl_usd = state.get("tvl_usd")

        # 2. DLMM-specific risk checks (rug, TVL, inventory caps)
        kill, kill_reason = self.dlmm_risk.evaluate_tvl(ts, tvl_usd)
        if kill:
            logger.critical("Rug kill-switch: %s", kill_reason)
            await self._emergency_exit()
            return self._finish_cycle(self._make_record(
                ts, Decision.EMERGENCY_EXIT, "emergency", "rug_kill_switch",
                mid=mid, refresh_reason=kill_reason,
            ))

        inv_cap_breached, inv_reason = self.dlmm_risk.check_inventory_cap(
            self._inventory_base,
            self._inventory_quote / mid if mid > 0 else 0.0,
            self.cfg.dlmm.capital / mid if mid > 0 else 0.0,
        )
        if inv_cap_breached:
            logger.warning("Inventory cap breached: %s", inv_reason)
            await self._de_risk()
            return self._finish_cycle(self._make_record(
                ts, Decision.DE_RISK, "normal", "inventory_cap",
                mid=mid, refresh_reason=inv_reason,
            ))

        # 3. Regime + AS + shared risk policy
        price_history = list(zip(self._ts_history, self._price_history))
        regime = evaluate_regime(price_history)
        self._last_regime = regime  # persisted for _make_record + event log
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

            center_bin = min(l.bin_id for l in ladder if l.side == "bid") if ladder else self._active_bin
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
                    subject = f"ctrl.{self.cfg.hedge_config.venue}.{self.cfg.hedge_config.coin}"
                    self.emit("hedge_intent", subject=subject, **asdict(hedge_intent))
                    self.publish(subject, asdict(hedge_intent))

            record = self._make_record(
                ts, decision, urgency, action, mid=mid,
                r=r, half_spread=half_spread,
                center_bin=center_bin if 'center_bin' in dir() else self._active_bin,
                refresh_reason=refresh_reason,
                refresh_needed=refresh_needed,
                net_delta=inventory.net_delta(),
                sigma=sigma,
            )
            self._log_decision(record, skew=skew, ladder=ladder)
            return record

        record = self._make_record(ts, decision, urgency, action, mid=mid, refresh_reason=refresh_reason)
        self._log_decision(record)
        return record

    def _emit_action(self, req_id: str, verb: str, payload: dict) -> None:
        self.emit("action_request", req_id=req_id, verb=verb, payload=_jsonable(payload))

    def _chain_fields(self, result: ExecResult) -> dict:
        signatures = list(result.tx_signatures)
        return {
            "tx_signature": signatures[0] if len(signatures) == 1 else None,
            "tx_signatures": signatures,
            "transactions": _jsonable(result.tx_receipts),
            "slot": result.slot,
            "block_time": result.block_time,
            "fee_lamports": result.total_fee_lamports,
            "compute_unit_price": result.compute_unit_price,
        }

    def _emit_result(
        self, req_id: str, verb: str, result: ExecResult, gas_label: str
    ) -> None:
        """Correlate request→result and persist every transaction receipt."""
        self.emit(
            "action_result",
            req_id=req_id,
            verb=verb,
            ok=result.ok,
            error=result.error,
            position_id=(result.data.get("position_id") if isinstance(result.data, dict) else None)
            or result.position_id,
            data=_jsonable(result.data),
            **self._chain_fields(result),
        )
        self._gas(gas_label, result)

    def _gas(self, label: str, result: ExecResult) -> None:
        """Record gas in native and quote units for every submitted action."""
        actual_fee = result.total_fee_lamports
        if actual_fee is None and not (result.ok or result.tx_signatures):
            return
        fee_lamports = (
            actual_fee if actual_fee is not None else REFRESH_GAS_LAMPORTS
        )
        amount_sol = -fee_lamports / 1e9
        native_price = self.cfg.gas_token_price_quote
        if native_price is None:
            native_price = self._last_mid
        amount_quote = amount_sol * native_price if native_price is not None else None
        ts = time.time()
        if amount_quote is not None:
            self.pnl.on_cash_flow("rebalance", ts, amount_quote, label=label)
        self.emit(
            "cash_flow",
            label=label,
            ts=ts,
            amount_sol=amount_sol,
            amount_quote=amount_quote,
            fee_lamports=fee_lamports,
            actual_fee=actual_fee is not None,
            tx_signatures=list(result.tx_signatures),
            transactions=_jsonable(result.tx_receipts),
        )

    def _bin_payload(self, levels: list[LadderLevel]) -> list[dict]:
        rows = []
        for level in levels:
            is_ask = level.side == "ask"
            amount_base = level.size if is_ask else 0.0
            amount_quote = 0.0 if is_ask else level.size
            rows.append({
                "bin_id": level.bin_id,
                "side": level.side,
                "amount": level.size,  # legacy bridge amount
                "amount_base": amount_base,
                "amount_quote": amount_quote,
                "amount_base_raw": self.cfg.grid.to_raw(amount_base, "base"),
                "amount_quote_raw": self.cfg.grid.to_raw(amount_quote, "quote"),
                "price": self.cfg.grid.price_from_bin(level.bin_id),
            })
        return rows

    def _register_ladder(self, ladder: list[LadderLevel], position_id: str | None) -> None:
        if self.swap_observer is None:
            return
        self.swap_observer.register_ladder(
            {row["bin_id"]: row for row in self._bin_payload(ladder)},
            position_id,
        )

    async def _deposit_ladder(self, ladder: list[LadderLevel]) -> None:
        """Initial deposit of single-sided positions."""
        self._ensure_run_started()
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would deposit %d levels", len(ladder))
            return
        bids = [l for l in ladder if l.side == "bid"]
        asks = [l for l in ladder if l.side == "ask"]
        # Deposit bid side (quote token below mid)
        if bids:
            req_id = self._next_req_id()
            payload = {
                "pool": self.cfg.pool_address,
                "side": "bid",
                "bin_ids": [l.bin_id for l in bids],
                "amounts": [l.size for l in bids],
                "strategy_type": "Spot",
            }
            self._emit_action(req_id, "deposit_single_sided", payload)
            result = self.exec.deposit_single_sided(**payload)
            self._emit_result(req_id, "deposit_single_sided", result, "deposit_gas")
            if result.ok:
                self._current_position_id = (
                    result.data.get("position_id") if result.data else None
                ) or result.position_id
                self._center_bin = min(l.bin_id for l in bids) + (
                    max(l.bin_id for l in bids) - min(l.bin_id for l in bids)
                ) // 2 if bids else self._active_bin
                self.emit(
                    "position_created",
                    position_id=self._current_position_id,
                    min_bin_id=min(l.bin_id for l in bids),
                    max_bin_id=max(l.bin_id for l in bids),
                    bins=self._bin_payload(bids),
                    strategy_type="Spot",
                    **self._chain_fields(result),
                )
                logger.info("Deposited bid side: %s", result.tx_signatures)
            else:
                logger.error("Bid deposit failed: %s", result.error)

        # Deposit ask side (base token above mid)
        if asks:
            req_id = self._next_req_id()
            payload = {
                "pool": self.cfg.pool_address,
                "side": "ask",
                "bin_ids": [l.bin_id for l in asks],
                "amounts": [l.size for l in asks],
                "strategy_type": "Spot",
            }
            self._emit_action(req_id, "deposit_single_sided", payload)
            result = self.exec.deposit_single_sided(**payload)
            self._emit_result(req_id, "deposit_single_sided", result, "deposit_gas")
            if result.ok:
                self.emit(
                    "position_liquidity_added",
                    position_id=self._current_position_id,
                    min_bin_id=min(l.bin_id for l in asks),
                    max_bin_id=max(l.bin_id for l in asks),
                    bins=self._bin_payload(asks),
                    strategy_type="Spot",
                    **self._chain_fields(result),
                )
            else:
                logger.error("Ask deposit failed: %s", result.error)

        self._register_ladder(ladder, self._current_position_id)

    async def _refresh_ladder(self, ladder: list[LadderLevel]) -> None:
        """Refresh: withdraw → optional swap → redeposit via Jito bundle."""
        self._ensure_run_started()
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would refresh %d levels", len(ladder))
            return
        if self._current_position_id is None:
            logger.warning("Cannot refresh: no current position_id")
            return

        bids = [l for l in ladder if l.side == "bid"]
        asks = [l for l in ladder if l.side == "ask"]

        req_id = self._next_req_id()
        payload = {
            "withdraw_position_id": self._current_position_id,
            "swap_spec": None,  # TODO: compute rebalance swap from inventory imbalance
            "deposit_spec": {
                "pool": self.cfg.pool_address,
                "bid_bins": [l.bin_id for l in bids],
                "ask_bins": [l.bin_id for l in asks],
                "bid_amounts": [l.size for l in bids],
                "ask_amounts": [l.size for l in asks],
            },
        }
        self._emit_action(req_id, "refresh_bundle", payload)
        result = self.exec.refresh_bundle(**payload)
        self._emit_result(req_id, "refresh_bundle", result, "refresh_gas")
        if result.ok:
            center = min(l.bin_id for l in ladder if l.side == "bid") if bids else self._active_bin
            self._center_bin = center
            old_pid = self._current_position_id
            new_pid = (
                result.data.get("position_id") if isinstance(result.data, dict) else None
            ) or result.position_id or old_pid
            data = result.data if isinstance(result.data, dict) else {}
            self.emit(
                "position_withdrawn",
                position_id=old_pid,
                bps=100,
                fees_claimed=data.get("fees_claimed"),
                amounts_returned=data.get("amounts_returned"),
                **self._chain_fields(result),
            )
            self.emit(
                "position_liquidity_added",
                position_id=new_pid,
                bins=self._bin_payload(ladder),
                **self._chain_fields(result),
            )
            self._register_ladder(ladder, new_pid)
            logger.info("Refreshed ladder: %s", result.tx_signatures)
        else:
            logger.error("Refresh failed: %s", result.error)


    async def _stop_quoting(self) -> None:
        """Stop quoting: withdraw to single-sided safe leg."""
        self._ensure_run_started()
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would stop quoting (withdraw to safe leg)")
            return
        if self._current_position_id:
            req_id = self._next_req_id()
            payload = {"position_id": self._current_position_id, "bps": 100}
            self._emit_action(req_id, "withdraw", payload)
            result = self.exec.withdraw(**payload)
            self._emit_result(req_id, "withdraw", result, "stop_quoting_gas")
            data = result.data if isinstance(result.data, dict) else {}
            if result.ok:
                self.emit(
                    "position_withdrawn",
                    position_id=self._current_position_id,
                    bps=100,
                    fees_claimed=data.get("fees_claimed"),
                    amounts_returned=data.get("amounts_returned"),
                    **self._chain_fields(result),
                )
                logger.info("Stopped quoting: withdrew position %s", self._current_position_id)
                self._current_position_id = None
                if self.swap_observer is not None:
                    self.swap_observer.clear()
            else:
                logger.error("Withdraw failed: %s", result.error)

    async def _de_risk(self) -> None:
        """De-risk: stop bids, drain asks, TWAP remainder over Jupiter."""
        self._ensure_run_started()
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] Would de-risk (stop bids, drain asks, TWAP)")
            return
        # Step 1: withdraw current position
        if self._current_position_id:
            req_id = self._next_req_id()
            payload = {"position_id": self._current_position_id, "bps": 100}
            self._emit_action(req_id, "withdraw", payload)
            result = self.exec.withdraw(**payload)
            self._emit_result(req_id, "withdraw", result, "de_risk_withdraw")
            data = result.data if isinstance(result.data, dict) else {}
            if not result.ok:
                logger.error("De-risk withdraw failed: %s", result.error)
                return
            self.emit(
                "position_withdrawn",
                position_id=self._current_position_id,
                bps=100,
                fees_claimed=data.get("fees_claimed"),
                amounts_returned=data.get("amounts_returned"),
                **self._chain_fields(result),
            )
            self._current_position_id = None
            if self.swap_observer is not None:
                self.swap_observer.clear()

        # Step 2: TWAP the remainder via Jupiter swap
        # In production, this would chunk the remaining inventory into N
        # depth-checked swaps. For now, a single swap to safe leg.
        if self._inventory_base > 0.01:
            req_id = self._next_req_id()
            payload = {
                "in_mint": "base",
                "out_mint": "quote",
                "amount": self._inventory_base,
                "max_slippage_bps": 100,
                "pool": None,
            }
            self._emit_action(req_id, "swap", payload)
            result = self.exec.swap(**payload)
            self._emit_result(req_id, "swap", result, "de_risk_swap")
            if result.ok:
                logger.info("De-risk swap executed: %s", result.tx_signatures)
            else:
                logger.error("De-risk swap failed: %s", result.error)

    async def _emergency_exit(self) -> None:
        """Emergency exit: Jito bundle withdraw all + swap to safe leg."""
        self._ensure_run_started()
        if self.cfg.dry_run:
            logger.info("[DRY-RUN] EMERGENCY EXIT — would Jito bundle withdraw + swap")
            self._halted = True
            return
        # Withdraw everything
        if self._current_position_id:
            req_id = self._next_req_id()
            payload = {"position_id": self._current_position_id, "bps": 100}
            self._emit_action(req_id, "withdraw", payload)
            result = self.exec.withdraw(**payload)
            self._emit_result(req_id, "withdraw", result, "emergency_withdraw")
            data = result.data if isinstance(result.data, dict) else {}
            if not result.ok:
                logger.critical("Emergency withdraw FAILED: %s", result.error)
            else:
                self.emit(
                    "position_closed",
                    position_id=self._current_position_id,
                    fees_claimed=data.get("fees_claimed"),
                    amounts_returned=data.get("amounts_returned"),
                    **self._chain_fields(result),
                )
                logger.critical("Emergency withdraw ok: %s", result.tx_signatures)
            self._current_position_id = None
            if self.swap_observer is not None:
                self.swap_observer.clear()
        # Swap all base to quote (safe leg)
        if self._inventory_base > 0.001:
            req_id = self._next_req_id()
            payload = {
                "in_mint": "base",
                "out_mint": "quote",
                "amount": self._inventory_base,
                "max_slippage_bps": 200,  # accept 2% slippage in emergency
                "pool": None,
            }
            self._emit_action(req_id, "swap", payload)
            result = self.exec.swap(**payload)
            self._emit_result(req_id, "swap", result, "emergency_swap")
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
        net_delta: float = 0.0, sigma: float = 0.0,
    ) -> CycleRecord:
        regime = self._last_regime
        return CycleRecord(
            ts=ts,
            active_bin=self._active_bin,
            mid=mid,
            regime_half_life=getattr(regime, "half_life", 0.0) or 0.0,
            regime_hurst=getattr(regime, "hurst", 0.0) or 0.0,
            regime_trending=bool(getattr(regime, "trending", False)),
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
            net_delta=net_delta,
            sigma=sigma,
        )

    def _log_decision(
        self, record: CycleRecord,
        skew: float | None = None,
        ladder: list[LadderLevel] | None = None,
    ) -> None:
        """Log the decision record to the in-memory list + emit a `decision`
        event to the event log (the shadow-mode / rerun artifact)."""
        self._decision_log.append(record)
        logger.info(
            "cycle %d: decision=%s action=%s mid=%.4f active_bin=%d",
            self._cycle_count, record.decision, record.action, record.mid, record.active_bin,
        )
        self.emit(
            "decision",
            **asdict(record),
            skew=skew,
            ladder=_jsonable([
                {
                    "bin_id": l.bin_id,
                    "side": l.side,
                    "price": self.cfg.grid.price_from_bin(l.bin_id),
                    "size": l.size,
                } for l in (ladder or [])
            ]),
        )

    @property
    def decision_log(self) -> list[CycleRecord]:
        return self._decision_log


__all__ = [
    "Keeper", "KeeperConfig", "CycleRecord",
    "dump_keeper_config", "hash_keeper_config", "rebuild_keeper_config",
]