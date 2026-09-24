"""
DLMM event-replay backtester.

Replays historical bin-crossing events (swaps that cross bins) through the
AS-driven ladder, accumulating:
  - spread capture (buy low bin, sell high bin)
  - crossed-bin dynamic fees (only crossed bins accrue)
  - square-root impact on rebalance swaps
  - refresh gas as a PnL line item
  - markout/adverse-selection stats

Fill rule: when a swap crosses a bin where the ladder has a resting
position, the position fills at that bin's fixed price. Single-sided
positions fill only on the crossed side.

Evaluation metrics:
  - Sharpe ratio > baseline
  - Max drawdown < baseline
  - Realized markout net of fees/gas > 0
  - Fill rate appropriate for regime
  - Adverse-selection cost < expected
  - IL + funding < edge (exotic pairs)
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Optional

from mm_core.pnl import Fill, PnLLedger
from mm_core.markout import MarkoutTracker
from mm_core.risk_policy import Decision

from dlmm_bot.oracle import OracleBars
from dlmm_bot.config import DLMMConfig
from dlmm_bot.grid import VenueGrid
from dlmm_bot.keeper import KeeperConfig, make_risk_policy, plan_cycle
from dlmm_bot.risk_dlmm import DLMMRiskConfig, DLMMRiskPolicy

GAS_COST = 0.00005  # per on-chain action, booked like the live keeper's gas line


@dataclass
class BinEvent:
    """A historical bin-crossing event from the data pipeline."""
    ts: float
    pool: str
    active_bin: int             # bin id after the event
    prev_active_bin: int        # bin id before the event
    direction: str              # "up" (price rose) | "down" (price fell)
    trade_size_usd: float      # size of the swap that crossed
    fee_bps: float             # dynamic fee at this timestamp
    tvl_usd: float | None = None


@dataclass
class LadderState:
    """Current deposited ladder state in the backtest."""
    active: bool = False
    bin_levels: dict[int, float] = field(default_factory=dict)  # bin_id → size deposited
    center_bin: int = 0
    widened: bool = False  # built under WIDEN; keeper refreshes on the transition only
    side: str = "bid"  # which side has liquidity at each bin


@dataclass
class BacktestResult:
    """Metrics from a backtest run."""
    n_cycles: int = 0
    n_fills: int = 0
    n_refreshes: int = 0
    total_pnl: float = 0.0
    spread_capture: float = 0.0
    markout_pnl: float = 0.0
    lp_fee_income: float = 0.0
    rebalance_cost: float = 0.0
    max_drawdown: float = 0.0
    sharpe: float = 0.0
    fill_rate: float = 0.0
    avg_round_trip_bps: float = 0.0
    n_up_crosses: int = 0
    n_down_crosses: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


class DLMMBacktester:
    """Event-replay backtester for the DLMM MM strategy.

    Decides through dlmm_bot.keeper.plan_cycle — the live keeper's exact
    risk / regime / AS / ladder path — and simulates only the actuation.
    """

    def __init__(
        self,
        cfg: DLMMConfig,
        grid: VenueGrid,
        events: list[BinEvent],
        initial_capital: float = 1000.0,
        keeper_cfg: KeeperConfig | None = None,
        oracle: OracleBars | None = None,
        gas_cost: float = GAS_COST,
    ):
        self.cfg = cfg
        self.grid = grid
        self.events = sorted(events, key=lambda e: e.ts)
        self.initial_capital = initial_capital
        self.keeper_cfg = keeper_cfg or KeeperConfig(dlmm=cfg, grid=grid)
        self.oracle = oracle  # preloaded bars; same regime input as a live oracle keeper
        self.gas_cost = gas_cost  # quote units per on-chain action (see calibrate --gas-lamports)

        # Same components, same construction as Keeper.__init__
        self.pnl = PnLLedger(venue="meteora", symbol="backtest")
        self.markout = MarkoutTracker()
        self.dlmm_risk = DLMMRiskPolicy(DLMMRiskConfig(pair_type=self.keeper_cfg.pair_type))
        self.risk = make_risk_policy(self.keeper_cfg, self.dlmm_risk)

        self.ladder_state = LadderState()
        # Total holdings (wallet + deposited), what the live keeper reads as balances
        self.base = 0.0
        self.quote = 0.0
        self.halted = False
        self.decisions: list[Decision] = []

        # Keeper parity: the live keeper samples the pool price once per
        # refresh_interval, so the regime/σ history is that poll grid, not one
        # sample per swap. _last_* is the latest observed price (end-of-run mark).
        self._prices: deque = deque(maxlen=self.keeper_cfg.max_history)
        self._ts: deque = deque(maxlen=self.keeper_cfg.max_history)
        self._last_ts: float | None = None
        self._last_mid: float | None = None

        # Results
        self._equity_curve: list[float] = []
        self._peak_equity: float = float("-inf")
        self.n_fills = 0
        self.n_refreshes = 0
        self.n_cycles = 0
        self.n_up_crosses = 0
        self.n_down_crosses = 0

    def run(self) -> BacktestResult:
        """Run the backtest over all events."""
        if self.events:
            # Start 50/50 base/quote; the base leg is booked through the ledger
            # at mid so its price risk shows up in PnL.
            first = self.events[0]
            mid = self.grid.price_from_bin(first.prev_active_bin)
            self.base = self.initial_capital / 2.0 / mid
            self.quote = self.initial_capital / 2.0
            self.pnl.on_fill(Fill(ts=first.ts, side="buy", price=mid, size=self.base,
                                  mid_at_fill=mid, label="initial_inventory"))
        for event in self.events:
            if self.halted:  # live keeper halts after emergency exit
                break
            self._process_event(event)
        return self._compute_result()

    def _book_fill(self, fill: Fill) -> None:
        self.pnl.on_fill(fill)
        signed = fill.size if fill.side == "buy" else -fill.size
        self.base += signed
        self.quote -= signed * fill.price

    def _gas(self, ts: float, label: str) -> None:
        self.pnl.on_cash_flow("rebalance", ts, -self.gas_cost, label=label)
        self.quote -= self.gas_cost

    def _withdraw(self, ts: float, label: str) -> None:
        if self.ladder_state.active:
            self.ladder_state = LadderState()
            self.n_refreshes += 1
            self._gas(ts, label)

    def _swap_base_out(self, ts: float, mid: float, min_base: float, label: str) -> None:
        """ponytail: swap fills at mid, no price impact — add the sqrt impact
        model once Jupiter swap fills are measured."""
        if self.base > min_base:
            self._book_fill(Fill(ts=ts, side="sell", price=mid, size=self.base,
                                 mid_at_fill=mid, label=label))
            self._gas(ts, label)

    def _deploy(self, ts: float, active_bin: int, ladder, widened: bool = False) -> None:
        # Ladder units: ask size is base, bid size is quote. Stored as base so
        # fills match the SwapObserver rule.
        levels = {}
        for level in ladder:
            if level.size <= 0:
                continue
            price = self.grid.price_from_bin(level.bin_id)
            levels[level.bin_id] = level.size if level.side == "ask" else level.size / price
        self.ladder_state = LadderState(active=True, bin_levels=levels, center_bin=active_bin,
                                        widened=widened)
        self.n_refreshes += 1
        self._gas(ts, "refresh_gas")

    def _sample_poll_grid(self, ts: float, mid: float) -> None:
        """Poll-grid samples up to `ts`, each at the price prevailing then.

        Grid points strictly before this swap saw the previous swap's price;
        the current price reaches `plan_cycle` as the live observation.
        """
        step = self.keeper_cfg.refresh_interval
        if self._last_ts is None:
            self._ts.append(ts)
            self._prices.append(mid)
        else:
            t = self._ts[-1] + step
            while t < ts:
                self._ts.append(t)
                self._prices.append(self._last_mid)
                t += step
        self._last_ts, self._last_mid = ts, mid

    def _process_event(self, event: BinEvent) -> None:
        """Process one bin-crossing event, then run one keeper cycle."""
        self.n_cycles += 1
        mid = self.grid.price_from_bin(event.active_bin)
        prev_mid = self.grid.price_from_bin(event.prev_active_bin)
        self._sample_poll_grid(event.ts, mid)
        self.pnl.mark(event.ts, mid)
        self.markout.on_mid(event.ts, mid)

        # Determine which bins were crossed
        if event.direction == "up":
            crossed_bins = list(range(event.prev_active_bin, event.active_bin))
            self.n_up_crosses += 1
        else:
            crossed_bins = list(range(event.active_bin + 1, event.prev_active_bin + 1))
            self.n_down_crosses += 1

        # Up-cross fills asks (we sell base), down-cross fills bids (we buy base)
        for bin_id in crossed_bins:
            size = self.ladder_state.bin_levels.get(bin_id, 0.0)
            if size <= 0:
                continue
            bin_price = self.grid.price_from_bin(bin_id)
            up = event.direction == "up"
            fill = Fill(
                ts=event.ts, side="sell" if up else "buy", price=bin_price, size=size,
                mid_at_fill=prev_mid, label="bin_ask_cross" if up else "bin_bid_cross",
            )
            self._book_fill(fill)
            self.markout.on_fill(event.ts, fill.side, fill.price, fill.size)
            self.n_fills += 1
            self.ladder_state.bin_levels[bin_id] = 0.0

            # LP fee accrues only on crossed bins
            fee_income = size * bin_price * event.fee_bps / 1e4
            self.pnl.on_cash_flow("lp_fee", event.ts, fee_income, label=f"bin_{bin_id}_fee")
            self.quote += fee_income

        self._cycle(event, mid)

        equity = self.pnl.explain(event.ts, mid).total_pnl + self.initial_capital
        self._equity_curve.append(equity)
        self._peak_equity = max(self._peak_equity, equity)

    def _cycle(self, event: BinEvent, mid: float) -> None:
        """Keeper._cycle steps 2-4 with simulated actuation."""
        plan = plan_cycle(
            self.keeper_cfg, self.risk, self.dlmm_risk, self.markout, self.pnl,
            ts=event.ts, mid=mid, active_bin=event.active_bin,
            inventory_base=self.base, inventory_quote=self.quote,
            price_history=list(zip(self._ts, self._prices)) + [(event.ts, mid)],
            tvl_usd=event.tvl_usd,
            regime_history=self.oracle.regime_history(event.ts) if self.oracle else None,
        )
        self.decisions.append(plan.decision)

        if plan.decision == Decision.EMERGENCY_EXIT:
            self._withdraw(event.ts, "emergency_withdraw")
            self._swap_base_out(event.ts, mid, 0.001, "emergency_swap")
            self.halted = True
        elif plan.decision == Decision.DE_RISK:
            self._withdraw(event.ts, "de_risk_withdraw")
            self._swap_base_out(event.ts, mid, 0.01, "de_risk_swap")
        elif plan.decision == Decision.STOP_QUOTING:
            self._withdraw(event.ts, "stop_quoting_gas")
        elif plan.decision == Decision.HOLD:
            pass
        else:
            center = self.ladder_state.center_bin
            drift = abs(event.active_bin - center) if center else 0
            if (
                not self.ladder_state.active
                or drift >= self.keeper_cfg.drift_threshold_bins
                or (plan.decision == Decision.WIDEN and not self.ladder_state.widened)
            ):
                self._deploy(event.ts, event.active_bin, plan.ladder,
                             widened=plan.decision == Decision.WIDEN)

    def _compute_result(self) -> BacktestResult:
        """Compute final metrics."""
        breakdown = self.pnl.explain(
            self._last_ts,
            self._last_mid,
        ) if self._last_mid is not None else None

        total_pnl = breakdown.total_pnl if breakdown else 0.0
        spread = breakdown.spread_capture if breakdown else 0.0
        markout = breakdown.markout_pnl if breakdown else 0.0
        lp_fees = breakdown.lp_fee_income if breakdown else 0.0
        rebalance = breakdown.rebalance_cost if breakdown else 0.0

        # Max drawdown
        max_dd = 0.0
        peak = float("-inf")
        for eq in self._equity_curve:
            peak = max(peak, eq)
            if peak > 0:
                dd = (peak - eq) / peak
                max_dd = max(max_dd, dd)

        # Sharpe ratio (simplified — from equity curve returns)
        Sharpe = 0.0
        if len(self._equity_curve) > 1:
            returns = [
                (self._equity_curve[i] - self._equity_curve[i-1]) / max(abs(self._equity_curve[i-1]), 1e-12)
                for i in range(1, len(self._equity_curve))
            ]
            if returns:
                mean_ret = sum(returns) / len(returns)
                var_ret = sum((r - mean_ret) ** 2 for r in returns) / max(len(returns) - 1, 1)
                std_ret = math.sqrt(var_ret) if var_ret > 0 else 0
                Sharpe = (mean_ret / std_ret * math.sqrt(365 * 24 * 3600 / self.keeper_cfg.refresh_interval)) if std_ret > 0 else 0.0

        # Fill rate
        fill_rate = self.n_fills / max(self.n_up_crosses + self.n_down_crosses, 1)

        # Average round-trip bps
        avg_rt_bps = 0.0
        if self.n_fills > 0:
            avg_rt_bps = (spread / self.n_fills / self.grid.ref_price) * 1e4 if self.grid.ref_price > 0 else 0.0

        return BacktestResult(
            n_cycles=self.n_cycles,
            n_fills=self.n_fills,
            n_refreshes=self.n_refreshes,
            total_pnl=total_pnl,
            spread_capture=spread,
            markout_pnl=markout,
            lp_fee_income=lp_fees,
            rebalance_cost=rebalance,
            max_drawdown=max_dd * 100,  # as percentage
            sharpe=Sharpe,
            fill_rate=fill_rate,
            avg_round_trip_bps=avg_rt_bps,
            n_up_crosses=self.n_up_crosses,
            n_down_crosses=self.n_down_crosses,
        )


__all__ = ["BinEvent", "LadderState", "BacktestResult", "DLMMBacktester"]
