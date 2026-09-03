"""
PnL explain as a pure fold over the event log (dlmm-logging-plan §5).

`explain_run(events)` re-derives the full PnL breakdown from logged facts
only — no in-memory keeper state:

- spread capture / realized PnL: bin_fill → mm_core Fill → PnLLedger
- LP fee income: deltas of claimable_fee_x/y between position_observation
  events (accrual basis) cross-checked against fees claimed at withdraw
  (cash basis); divergence beyond tolerance is flagged
- rebalance cost: our own swap action_results (realized slippage vs mid)
- gas: actual fee_lamports from every action_result
- markout: MarkoutTracker fed from bin_fill + the logged mid series
- inventory & IL: fold position_* + bin_fill → per-timestamp base/quote
  balances; IL vs the constant-0.5-share (immutable-position) benchmark

This becomes the authoritative number for a run; the in-cycle
pnl.explain().total_pnl stays as a fast live estimate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from mm_core.markout import MarkoutTracker
from mm_core.pnl import Fill, PnLLedger


@dataclass
class RunPnL:
    """Full explained PnL for one logged run."""
    n_events: int = 0
    n_fills: int = 0
    n_trades: int = 0
    n_actions: int = 0

    total_pnl: float = 0.0
    trading_pnl: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    spread_capture: float = 0.0
    markout_pnl: float = 0.0

    lp_fee_accrued: float = 0.0      # accrual basis (claimable deltas, USD)
    lp_fee_claimed: float = 0.0      # cash basis (fees actually withdrawn)
    lp_fee_divergence: float = 0.0
    fee_divergence_flagged: bool = False

    rebalance_cost: float = 0.0      # cash-channel rebalance total (<= 0)
    swap_slippage: float = 0.0       # realized rebalance-swap slippage vs mid
    gas_lamports: int = 0
    gas_sol: float = 0.0

    avg_markout_bps_30s: float | None = None

    final_inventory: dict | None = None
    il_pct: float | None = None
    inventory_trace: list[dict] = field(default_factory=list)

    flags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        from dataclasses import asdict
        return asdict(self)


def _event_mid(ev: dict) -> float | None:
    for key in ("mid", "mid_after", "mid_at_fill"):
        v = ev.get(key)
        if isinstance(v, (int, float)):
            return float(v)
    state = ev.get("state")
    if isinstance(state, dict):
        v = state.get("mid")
        if isinstance(v, (int, float)):
            return float(v)
    return None


def explain_run(events: Iterable[dict]) -> RunPnL:
    """Fold a run log (as dicts, in seq order) into a RunPnL breakdown."""
    evs = list(events)
    ledger = PnLLedger(venue="dlmm", symbol="explain")
    markout = MarkoutTracker()
    out = RunPnL()

    claimable_prev: tuple[float, float] | None = None
    fee_claimed_usd = 0.0
    inv: dict | None = None  # {"base": float, "quote": float}
    initial: dict | None = None
    mid: float | None = None
    last_ts: float = 0.0

    for ev in evs:
        out.n_events += 1
        t = ev.get("event_type", "")
        ts = float(ev.get("ts") or ev.get("ts_wall") or 0.0)
        last_ts = max(last_ts, ts)
        m = _event_mid(ev)
        if m is not None:
            mid = m
            ledger.mark(ts, mid)
            markout.on_mid(ts, mid)

        if t == "observed_trade":
            out.n_trades += 1

        elif t == "bin_fill":
            side = ev.get("side_filled", "buy")
            price = float(ev.get("bin_price") or 0.0)
            size = float(ev.get("amount_base") or 0.0)
            if price <= 0 or size <= 0:
                continue
            fill = Fill(
                ts=ts, side=side, price=price, size=size,
                mid_at_fill=ev.get("mid_at_fill"),
                label=str(ev.get("label", "")),
            )
            ledger.on_fill(fill)
            markout.on_fill(ts, side, price, size)
            out.n_fills += 1
            inv = _apply_fill_to_inventory(inv, side, size, price)

        elif t == "position_observation":
            data = ev.get("data") if isinstance(ev.get("data"), dict) else {}
            fee_x = float(data.get("claimable_fee_x") or 0.0)
            fee_y = float(data.get("claimable_fee_y") or 0.0)
            if claimable_prev is not None and mid:
                out.lp_fee_accrued += (fee_x - claimable_prev[0]) * mid \
                    + (fee_y - claimable_prev[1])
            claimable_prev = (fee_x, fee_y)

        elif t in ("position_withdrawn", "position_closed"):
            fees = ev.get("fees_claimed")
            if isinstance(fees, dict) and mid:
                fee_claimed_usd += float(fees.get("x") or 0.0) * mid \
                    + float(fees.get("y") or 0.0)
            # fully exit open inventory (mirrors _inventory_trace)
            inv = {"base": 0.0, "quote": 0.0}

        elif t in ("position_created", "position_liquidity_added"):
            inv = _add_bins_to_inventory(inv, ev)
            if inv is not None and initial is None:
                initial = dict(inv)

        elif t == "action_result":
            out.n_actions += 1
            out.gas_lamports += int(ev.get("fee_lamports") or 0)
            if ev.get("verb") == "swap" and ev.get("ok"):
                data = ev.get("data") if isinstance(ev.get("data"), dict) else {}
                amt_in = data.get("amount_in") or data.get("amount_out_base")
                amt_out = data.get("amount_out")
                if amt_in and amt_out and mid:
                    # amount_in is base-denominated (USD = in*mid),
                    # amount_out is quote-denominated (USD ≈ as-is)
                    out.swap_slippage += max(
                        0.0, float(amt_in) * mid - float(amt_out)
                    )

        elif t == "cash_flow":
            label = str(ev.get("label", ""))
            channel = "lp_fee" if label == "fees_claimed" else "rebalance"
            ledger.on_cash_flow(
                channel, ts, float(ev.get("amount_sol") or 0.0), label=label
            )

    breakdown = ledger.explain(last_ts, mid) if mid else None
    if breakdown:
        out.total_pnl = breakdown.total_pnl
        out.trading_pnl = breakdown.trading_pnl
        out.realized_pnl = breakdown.realized_pnl
        out.unrealized_pnl = breakdown.unrealized_pnl
        out.spread_capture = breakdown.spread_capture
        out.markout_pnl = breakdown.markout_pnl
        out.rebalance_cost = breakdown.rebalance_cost

    out.lp_fee_claimed = fee_claimed_usd
    out.lp_fee_divergence = abs(out.lp_fee_accrued - out.lp_fee_claimed)
    tol = max(1e-6, 0.01 * max(abs(out.lp_fee_accrued), abs(out.lp_fee_claimed)))
    out.fee_divergence_flagged = out.lp_fee_divergence > tol
    if out.fee_divergence_flagged:
        out.flags.append(
            f"lp_fee divergence: accrued={out.lp_fee_accrued:.6f} "
            f"claimed={out.lp_fee_claimed:.6f}"
        )

    out.gas_sol = out.gas_lamports / 1e9
    out.avg_markout_bps_30s = markout.avg_markout_bps(30.0)

    # Inventory trace + IL (constant-0.5-share / immutable benchmark)
    if initial is not None and inv is not None and mid:
        out.inventory_trace = [
            {
                "ts": ts,
                "base": d["base"],
                "quote": d["quote"],
                "value_usd": d["base"] * m + d["quote"],
            }
            for ts, d, m in _inventory_trace(evs)
        ]
        final_value = inv["base"] * mid + inv["quote"]
        benchmark = initial["base"] * mid + initial["quote"]
        if benchmark > 0:
            out.il_pct = (final_value / benchmark - 1.0) * 100.0
        out.final_inventory = {
            "base": inv["base"], "quote": inv["quote"], "value_usd": final_value,
        }

    return out


def _apply_fill_to_inventory(
    inv: dict | None, side: str, size: float, price: float
) -> dict | None:
    inv = dict(inv) if inv else {"base": 0.0, "quote": 0.0}
    if side == "buy":
        # bid filled below mid: spent quote, gained base
        inv["base"] += size
        inv["quote"] -= size * price
    else:
        # ask filled above mid: sold base, gained quote
        inv["base"] -= size
        inv["quote"] += size * price
    return inv


def _add_bins_to_inventory(
    inv: dict | None, ev: dict
) -> dict | None:
    bins = ev.get("bins")
    if not isinstance(bins, list):
        return inv
    inv = dict(inv) if inv else {"base": 0.0, "quote": 0.0}
    for b in bins:
        if not isinstance(b, dict):
            continue
        amount = b.get("amount")
        price = b.get("price")
        if amount is None or price is None:
            continue
        if b.get("side") == "ask":
            inv["base"] += float(amount)
        else:
            inv["quote"] += float(amount) * float(price)
    return inv


def _inventory_trace(
    events: Iterable[dict]
) -> list[tuple[float, dict, float]]:
    """Yield (ts, inventory, mid) after each inventory-relevant event.

    Starts from zero inventory and re-folds every deposit so the trace never
    double-counts the opening position (mirrors the main fold in
    ``explain_run``)."""
    inv: dict | None = None
    mid = 0.0
    for ev in events:
        t = ev.get("event_type", "")
        ts = float(ev.get("ts") or ev.get("ts_wall") or 0.0)
        m = _event_mid(ev)
        if m is not None:
            mid = m
        if t == "position_created" or t == "position_liquidity_added":
            inv = _add_bins_to_inventory(inv, ev) or inv
        elif t in ("position_withdrawn", "position_closed"):
            inv = {"base": 0.0, "quote": 0.0}
        elif t == "bin_fill":
            price = float(ev.get("bin_price") or 0.0)
            size = float(ev.get("amount_base") or 0.0)
            if price > 0 and size > 0:
                inv = _apply_fill_to_inventory(
                    inv, ev.get("side_filled", "buy"), size, price
                )
        else:
            continue
        if inv is not None:
            yield ts, dict(inv), mid


__all__ = ["RunPnL", "explain_run"]
