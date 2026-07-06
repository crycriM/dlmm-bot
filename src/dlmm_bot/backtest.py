"""
DLMM event-replay backtester.

Per the dlmm plan §7/§9: replays historical bin-crossing events (swaps
that cross bins) through the AS-driven ladder, accumulating:
  - spread capture (buy low bin, sell high bin)
  - crossed-bin dynamic fees (only crossed bins accrue)
  - square-root impact on rebalance swaps
  - refresh gas as a PnL line item
  - markout/adverse-selection stats

Fill rule: when a swap crosses a bin where the ladder has a resting
position, the position fills at that bin's fixed price. Single-sided
positions fill only on the crossed side.

Metrics (per dlmm plan §9 gates):
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

from mm_core.contracts import MarketSnapshot
from mm_core.pnl import Fill, PnLLedger, CashFlow
from mm_core.as_core import gueant_reservation_price, gueant_half_spread
from mm_core.vol import VOLATILITY_MODELS
from mm_core.regime import evaluate_regime, should_quote, GateConfig
from mm_core.inventory import TwoTokenInventory, Caps
from mm_core.risk_policy import RiskPolicy, RiskConfig, Decision
from mm_core.markout import MarkoutTracker

from dlmm_bot.grid import VenueGrid
from dlmm_bot.ladder import build_ladder, LadderConfig, LadderLevel
from dlmm_bot.config import DLMMConfig


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
    """Event-replay backtester for the DLMM MM strategy."""

    def __init__(
        self,
        cfg: DLMMConfig,
        grid: VenueGrid,
        events: list[BinEvent],
        initial_capital: float = 1000.0,
    ):
        self.cfg = cfg
        self.grid = grid
        self.events = sorted(events, key=lambda e: e.ts)
        self.initial_capital = initial_capital

        # PnL ledger
        self.pnl = PnLLedger(venue="meteora", symbol=grid.ref_price and "backtest" or "backtest")

        # Markout tracker
        self.markout = MarkoutTracker()

        # Risk
        self.risk = RiskPolicy(cfg=RiskConfig(gate=GateConfig()))

        # Ladder state
        self.ladder_state = LadderState()

        # Price history for regime
        self._prices: deque = deque(maxlen=500)
        self._ts: deque = deque(maxlen=500)

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
        for event in self.events:
            self._process_event(event)
        return self._compute_result()

    def _process_event(self, event: BinEvent) -> None:
        """Process one bin-crossing event."""
        self.n_cycles += 1
        mid = self.grid.price_from_bin(event.active_bin)
        prev_mid = self.grid.price_from_bin(event.prev_active_bin)
        self._prices.append(mid)
        self._ts.append(event.ts)
        self.pnl.mark(event.ts, mid)

        # Markout tracking
        self.markout.on_mid(event.ts, mid)

        # Determine which bins were crossed
        if event.direction == "up":
            crossed_bins = list(range(event.prev_active_bin, event.active_bin))
            self.n_up_crosses += 1
        else:
            crossed_bins = list(range(event.active_bin + 1, event.prev_active_bin + 1))
            self.n_down_crosses += 1

        # Check fills at each crossed bin
        for bin_id in crossed_bins:
            bin_price = self.grid.price_from_bin(bin_id)
            if bin_id in self.ladder_state.bin_levels:
                size = self.ladder_state.bin_levels[bin_id]
                if size <= 0:
                    continue

                # Determine fill side: up-cross fills asks (we sell base), down-cross fills bids (we buy base)
                if event.direction == "up":
                    # Ask side: we sold base at bin_price
                    fill = Fill(
                        ts=event.ts, side="sell", price=bin_price, size=size,
                        mid_at_fill=prev_mid, label="bin_ask_cross",
                    )
                else:
                    # Bid side: we bought base at bin_price
                    fill = Fill(
                        ts=event.ts, side="buy", price=bin_price, size=size,
                        mid_at_fill=prev_mid, label="bin_bid_cross",
                    )
                self.pnl.on_fill(fill)
                self.markout.on_fill(event.ts, fill.side, fill.price, fill.size)
                self.n_fills += 1

                # Remove filled liquidity from this bin
                self.ladder_state.bin_levels[bin_id] = 0.0

                # Accrue LP fee (only on crossed bins)
                fee_income = size * bin_price * event.fee_bps / 1e4
                self.pnl.on_cash_flow("lp_fee", event.ts, fee_income, label=f"bin_{bin_id}_fee")

        # Check if regime gate allows quoting
        if len(self._prices) >= 2:
            price_history = list(zip(self._ts, self._prices))
            try:
                regime = evaluate_regime(price_history)
                should = should_quote(regime, GateConfig())
            except Exception:
                should = True

            if not should:
                # Stop quoting: withdraw
                if self.ladder_state.active:
                    self.ladder_state = LadderState()
                    self.n_refreshes += 1
                    self.pnl.on_cash_flow("rebalance", event.ts, -0.00005, label="stop_quoting_gas")
                return

        # Check if refresh needed (drift from center)
        drift = abs(event.active_bin - self.ladder_state.center_bin)
        if not self.ladder_state.active or drift >= 3:
            self._refresh_ladder(event, mid)
            self.n_refreshes += 1

        # Track equity for drawdown
        equity = self.pnl.explain(event.ts, mid).total_pnl + self.initial_capital
        self._equity_curve.append(equity)
        self._peak_equity = max(self._peak_equity, equity)

    def _refresh_ladder(self, event: BinEvent, mid: float) -> None:
        """Build and deploy a new ladder."""
        price_history = list(zip(self._ts, self._prices))
        if len(price_history) < 2:
            return

        vol_model = VOLATILITY_MODELS["close_to_close"]
        sigma = vol_model(price_history)

        inventory = TwoTokenInventory(
            base=0.0,  # simplified — from state in production
            quote=0.0,
            mid=mid,
            target_base_share=0.5,
            _caps=Caps(max_position=100.0, critical_position=90.0),
        )

        r = gueant_reservation_price(
            mid=mid, q=0.0, gamma=self.cfg.gamma,
            sigma=sigma, kappa=self.cfg.kappa,
        )
        half_spread = gueant_half_spread(
            gamma=self.cfg.gamma, sigma=sigma, kappa=self.cfg.kappa,
        )

        ladder = build_ladder(
            grid=self.grid, active_bin=event.active_bin,
            r=r, S=mid, half_spread=half_spread, skew=0.0,
            cfg=LadderConfig(
                levels=self.cfg.levels,
                inner_offset=self.cfg.inner_offset,
                capital=self.cfg.capital,
                level_weight=self.cfg.level_weight,
            ),
        )

        # Deploy ladder
        new_levels = {}
        for level in ladder:
            new_levels[level.bin_id] = level.size

        self.ladder_state = LadderState(
            active=True,
            bin_levels=new_levels,
            center_bin=event.active_bin,
        )

        # Gas cost for refresh
        self.pnl.on_cash_flow("rebalance", event.ts, -0.00005, label="refresh_gas")

    def _compute_result(self) -> BacktestResult:
        """Compute final metrics."""
        breakdown = self.pnl.explain(
            self._ts[-1] if self._ts else 0,
            self._prices[-1] if self._prices else self.grid.ref_price,
        ) if self._prices else None

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
                Sharpe = (mean_ret / std_ret * math.sqrt(365 * 24 * 3600 / self.cfg.refresh_interval)) if std_ret > 0 else 0.0

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