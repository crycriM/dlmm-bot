#!/usr/bin/env python
"""
Ping-pong DLMM soak: one-sided liquidity, never swapped, slowly perp-hedged.

The LP never swaps base <-> quote. Tokens in hand sit next to the active bin
(base in the `width` bins above it, quote in the `width` bins below), and a
crossed bin converts in place (DLMM semantics). An oscillation through the
range therefore flips the same bins back and forth at their fixed prices:
zero spread, the pool fee on every flip. When the active bin leaves the range
by `shift` bins the position is all one token; it is withdrawn and
redeposited, still one-sided, next to the active bin. That shift is the only
rebalance. The accumulated token is hedged on a perp instead:
short = EMA_tau(base held), re-hedged only when the gap exceeds a band.

Replays the executor's swap capture (the calibrate.py loader) and books both
legs through mm_core.pnl.PnLLedger. Research note:
project-internal/dlmm-ping-pong-lp.md.

Usage:
    python tools/pingpong_soak.py <swaps.jsonl> [--width 5,10,20] [--tau 900,3600]
"""

from __future__ import annotations

import argparse
import bisect
import itertools
import json
import sys
from typing import Callable

from mm_core.pnl import Fill, PnLLedger

from calibrate import load_events
from dlmm_bot.backtest import GAS_COST, BinEvent
from dlmm_bot.grid import VenueGrid
from dlmm_bot.hedge import HedgeConfig, HedgeController

YEAR_S = 365 * 24 * 3600
HALF_KEYS = ("hedged_total", "lp_total", "idle_hold")


def simulate(
    events: list[BinEvent], grid: VenueGrid, *, width: int, shift: int, tau: float,
    capital: float = 1000.0, gas_cost: float = GAS_COST, band_pct: float = 5.0,
    perp_cost_bps: float = 5.0, funding_apr: float = 0.0,
    pool_bin_quote: float | Callable[[BinEvent], float] = 0.0,
    in_bin_haircut: float = 1.0,
    protocol_fee_pct: float = 0.0,
) -> dict:
    """One run: the LP leg alone, the perp hedge, and their sum.

    A non-zero `pool_bin_quote` (a constant, or per event from `depth_at`)
    credits swaps that stay inside the active bin: the pool fee times our
    share of that bin, ours / (pool depth + ours).

    `in_bin_haircut` scales only that in-bin credit. The pro-rata share has
    no queue priority, no time in book and no self-trade effect, so it is an
    upper bound; a haircut re-checks the gate under a lower fill assumption.
    """
    if not 0 <= protocol_fee_pct <= 100:
        raise ValueError("protocol_fee_pct must be between 0 and 100")
    lp_fee_share = 1.0 - protocol_fee_pct / 100
    lp, perp = PnLLedger("meteora", "pingpong"), PnLLedger("hl", "hedge")
    first = events[0]
    mid = mid0 = grid.price_from_bin(first.prev_active_bin)
    base, quote = capital / 2 / mid0, capital / 2  # wallet (undeployed)
    lp.on_fill(Fill(first.ts, "buy", mid0, base, mid_at_fill=mid0, label="initial_inventory"))
    bins: dict[int, list[float]] = {}  # bin_id -> [base, quote], ours only
    fees = gas = in_bin_fees = 0.0
    n_positions = n_flips = n_shifts = n_hedges = in_range = 0

    def pay_gas(ts: float, label: str) -> None:
        nonlocal gas
        gas += gas_cost
        lp.on_cash_flow("rebalance", ts, -gas_cost, label=label)

    def deploy(ts: float, active: int) -> None:
        # Tokens in hand only: no swap ever sizes a side it doesn't hold.
        nonlocal base, quote, n_positions
        n_positions = 0
        for amount, slot, lo in ((base, 0, active + 1), (quote, 1, active - width)):
            if amount <= 0:
                continue
            for b in range(lo, lo + width):
                bins.setdefault(b, [0.0, 0.0])[slot] += amount / width
            n_positions += 1
            pay_gas(ts, "deposit_gas")
        base = quote = 0.0

    def withdraw(ts: float) -> None:
        nonlocal base, quote
        base += sum(c[0] for c in bins.values())
        quote += sum(c[1] for c in bins.values())
        bins.clear()
        for _ in range(n_positions):
            pay_gas(ts, "withdraw_gas")

    def held_base() -> float:
        return base + sum(c[0] for c in bins.values())

    # The seeded 50/50 inventory is hedged at t0, so only *accumulation* lags.
    # ponytail: HedgeController.evaluate() compares a USD deadband with a
    # base-unit residual; reuse its EMA and apply the band here in base units.
    hedge = HedgeController(HedgeConfig(tau_h=tau))
    hedge.state.ma_target = short = base
    perp.on_fill(Fill(first.ts, "sell", mid0, short, fee=short * mid0 * perp_cost_bps / 1e4,
                      mid_at_fill=mid0, label="hedge_open"))
    deploy(first.ts, first.prev_active_bin)

    last_ts = first.ts
    peak_lp = peak_all = dd_lp = dd_all = 0.0
    for e in events:
        dt, last_ts = e.ts - last_ts, e.ts
        hedge.update_ema(held_base(), dt)  # inventory that prevailed over dt
        # ponytail: perp priced at the pool mid, no basis; constant funding rate.
        perp.on_funding(e.ts, funding_apr / YEAR_S, mid, dt)
        prev_mid, mid = mid, grid.price_from_bin(e.active_bin)

        # Backtester crossing rule; a crossed bin flips in place at its price.
        up = e.active_bin > e.prev_active_bin
        crossed = (range(e.prev_active_bin, e.active_bin) if up
                   else range(e.active_bin + 1, e.prev_active_bin + 1))
        for b in crossed:
            cell, price = bins.get(b), grid.price_from_bin(b)
            if cell is None or cell[0 if up else 1] <= 0:
                continue
            if up:
                size, cell[0] = cell[0], 0.0
                cell[1] += size * price
            else:
                size, cell[1] = cell[1] / price, 0.0
                cell[0] += size
            lp.on_fill(Fill(e.ts, "sell" if up else "buy", price, size,
                            mid_at_fill=prev_mid, label="bin_flip"))
            fee = size * price * e.fee_bps / 1e4 * lp_fee_share  # claimable, not compounded
            fees += fee
            lp.on_cash_flow("lp_fee", e.ts, fee, label=f"bin_{b}_fee")
            n_flips += 1

        cell = bins.get(e.active_bin)
        if pool_bin_quote and e.active_bin == e.prev_active_bin and cell:
            # ponytail: the in-bin swap's effect on the active bin's token mix
            # is ignored (it mostly reverts in-bin).
            ours = cell[0] * mid + cell[1]
            depth = pool_bin_quote(e) if callable(pool_bin_quote) else pool_bin_quote
            fee = (e.trade_size_usd * e.fee_bps / 1e4 * lp_fee_share * ours / (depth + ours)
                   * in_bin_haircut)
            fees += fee
            in_bin_fees += fee
            lp.on_cash_flow("lp_fee", e.ts, fee, label="in_bin_fee")

        lo, hi = min(bins), max(bins)
        gap = max(lo - e.active_bin, e.active_bin - hi, 0)
        in_range += gap == 0
        # Out of range: all one token; move it next to the active bin, don't
        # swap it. A 1-bin gap already sits there (the bins flipped in place),
        # so re-depositing the same range would only pay gas.
        a = e.active_bin
        target = (a + 1, a + width) if lo > a else (a - width, a - 1)
        if gap >= shift and (lo, hi) != target:
            withdraw(e.ts)
            deploy(e.ts, a)
            n_shifts += 1

        gap_short = hedge.state.ma_target - short
        if abs(gap_short) * mid > capital * band_pct / 100:
            perp.on_fill(Fill(e.ts, "sell" if gap_short > 0 else "buy", mid, abs(gap_short),
                              fee=abs(gap_short) * mid * perp_cost_bps / 1e4,
                              mid_at_fill=mid, label="hedge"))
            short += gap_short
            n_hedges += 1

        lp.mark(e.ts, mid)
        perp.mark(e.ts, mid)
        lp_eq = lp.explain(include_events=False).total_pnl
        all_eq = lp_eq + perp.explain(include_events=False).total_pnl
        peak_lp, peak_all = max(peak_lp, lp_eq), max(peak_all, all_eq)
        dd_lp, dd_all = max(dd_lp, peak_lp - lp_eq), max(dd_all, peak_all - all_eq)

    lpx, px = lp.explain(include_events=False), perp.explain(include_events=False)
    b_held = held_base()
    q_held = quote + sum(c[1] for c in bins.values())
    physical = b_held * mid + q_held + fees - gas - capital
    if abs(physical - lpx.total_pnl) > 1e-6 * capital:
        raise RuntimeError(f"ledger {lpx.total_pnl} != holdings {physical}: accounting broken")
    hold = capital / 2 / mid0 * (mid - mid0)
    return {
        "width": width, "shift": shift, "tau": tau,
        "n_events": len(events), "n_flips": n_flips, "n_shifts": n_shifts,
        "n_hedges": n_hedges, "time_in_range": in_range / len(events),
        "lp_fee": lpx.lp_fee_income, "in_bin_fee": in_bin_fees, "gas": lpx.rebalance_cost,
        # conversions vs holding the seed inventory: the adverse-selection line
        "conversion_vs_hold": lpx.trading_pnl - hold,
        "lp_total": lpx.total_pnl, "idle_hold": hold,
        "hedge_trading": px.trading_pnl, "hedge_fees": px.fee_pnl,
        "hedge_funding": px.funding_pnl,
        "hedged_total": lpx.total_pnl + px.total_pnl,
        "max_dd_lp": dd_lp, "max_dd_hedged": dd_all,
        "final_base_share": b_held * mid / (b_held * mid + q_held),
        "final_base": b_held, "final_short": short,
    }


def estimate_pool_bin_quote(events: list[BinEvent]) -> float:
    """Median quote notional per bin over swaps crossing >= 2 bins: those
    consume whole bins, so notional / bins approximates pool depth per bin.
    0.0 (in-bin fees off) when the capture has no such swap."""
    per_bin = sorted(e.trade_size_usd / abs(e.active_bin - e.prev_active_bin)
                     for e in events if abs(e.active_bin - e.prev_active_bin) >= 2)
    return per_bin[len(per_bin) // 2] if per_bin else 0.0


def depth_at(path: str, base_decimals: int, quote_decimals: int,
             max_age_seconds: float = 15.0) -> Callable[[BinEvent], float]:
    """Depth of the traded bin from a preceding, fresh sample of its pool.

    Missing, stale or unsampled depth yields infinity (zero fee credit),
    rather than substituting a future sample or another bin's reserves.
    Error rows are skipped. Timestamps are compared with swap block_time.
    """
    samples: dict[str, list] = {}
    with open(path) as fh:
        for line in fh:
            row = json.loads(line) if line.strip() else {}
            if row.get("bins"):
                samples.setdefault(row["pool"], []).append((row["ts"], {
                    b["bin_id"]: int(b["x_raw"]) / 10 ** base_decimals * b["price"]
                    + int(b["y_raw"]) / 10 ** quote_decimals
                    for b in row["bins"]}))
    if not samples:
        raise ValueError(f"no depth samples in {path}")
    for rows in samples.values():
        rows.sort(key=lambda s: s[0])
    times = {pool: [t for t, _ in rows] for pool, rows in samples.items()}

    def lookup(e: BinEvent) -> float:
        index = bisect.bisect_right(times.get(e.pool, []), e.ts) - 1
        if index < 0:
            return float("inf")
        sample_ts, book = samples[e.pool][index]
        if e.ts - sample_ts > max_age_seconds:
            return float("inf")
        return book.get(e.prev_active_bin, float("inf"))
    return lookup


def soak(events, grid, widths, shifts, taus, split, **kw) -> list[dict]:
    """Every (width, shift, tau) cell on the full capture, plus each half."""
    cut = int(len(events) * split) if split else 0
    rows = []
    for w, s, t in itertools.product(widths, shifts, taus):
        row = simulate(events, grid, width=w, shift=s, tau=t, **kw)
        halves = [events[:cut], events[cut:]] if cut else []
        half_runs = [simulate(h, grid, width=w, shift=s, tau=t, **kw) for h in halves]
        row["halves"] = [{k: result[k] for k in HALF_KEYS} for result in half_runs]
        # Gate: beat both zero and the idle hold on the full run and each
        # half; the hedge must also shrink drawdown.
        row["gate_pass"] = (
            row["hedged_total"] > max(0.0, row["idle_hold"])
            and row["max_dd_hedged"] < row["max_dd_lp"]
            and all(h["hedged_total"] > max(0.0, h["idle_hold"]) for h in row["halves"])
        )
        rows.append(row)
    return rows


def _fmt(r: dict) -> str:
    halves = ",".join(f"{h['hedged_total']:+.2f}" for h in r["halves"])
    return (
        f"w={r['width']:<3d} s={r['shift']:<2d} tau={r['tau']:<6g} "
        f"lp={r['lp_total']:+8.2f} (fee {r['lp_fee']:+.2f} [in-bin {r['in_bin_fee']:+.2f}] conv {r['conversion_vs_hold']:+.2f} "
        f"gas {r['gas']:+.2f})  hold={r['idle_hold']:+.2f}  "
        f"hedged={r['hedged_total']:+8.2f} (perp {r['hedge_trading']:+.2f} "
        f"fees {r['hedge_fees']:+.2f})  dd {r['max_dd_lp']:.2f}/{r['max_dd_hedged']:.2f}  "
        f"shifts={r['n_shifts']} flips={r['n_flips']} hedges={r['n_hedges']} "
        f"in-range={r['time_in_range']:.0%}  halves=[{halves}]  "
        f"{'PASS' if r['gate_pass'] else 'fail'}"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("swaps", help="path to a swap-stream .jsonl capture")
    ap.add_argument("--pool", default=None, help="filter rows to one pool address")
    ap.add_argument("--bin-step", type=int, default=4, help="pool binStep in bps")
    ap.add_argument("--base-decimals", type=int, default=9)
    ap.add_argument("--quote-decimals", type=int, default=6)
    ap.add_argument("--width", default="5,10,20,40", help="bins per side")
    ap.add_argument("--shift", default="3", help="bins out of range before a shift")
    ap.add_argument("--tau", default="900,3600,14400", help="hedge EMA window, seconds")
    ap.add_argument("--capital", type=float, default=1000.0)
    ap.add_argument("--band-pct", type=float, default=5.0,
                    help="re-hedge when |EMA - short| exceeds this %% of capital")
    ap.add_argument("--perp-cost-bps", type=float, default=5.0,
                    help="taker fee + half spread per hedge trade")
    ap.add_argument("--funding-apr", type=float, default=0.0,
                    help="perp funding, annualized; positive pays the short")
    ap.add_argument("--protocol-fee-pct", type=float, default=0.0,
                    help="percent of swap fees retained by the protocol; 0 gives a gross-fee bound")
    ap.add_argument("--gas-lamports", type=int, default=30_000,
                    help="SOL fee per on-chain action (withdraw / deposit)")
    ap.add_argument("--sol-price", type=float, default=None,
                    help="SOL in quote units; default: the capture's median mid")
    depth_src = ap.add_mutually_exclusive_group()
    depth_src.add_argument("--depth", default=None,
                           help="executor depth-sampler JSONL (DEPTH_SAMPLE_PATH): measured depth")
    ap.add_argument("--depth-max-age-seconds", type=float, default=15.0,
                    help="maximum age of preceding depth; use 120 for 60-second samples")
    depth_src.add_argument("--pool-bin-quote", type=float, default=None,
                           help="pool liquidity per bin near the active bin, quote units; "
                                "default: estimated from multi-bin swaps; 0 = no in-bin fees")
    ap.add_argument("--split", type=float, default=0.5, help="also run each half; 0 = off")
    ap.add_argument("--window-hours", type=float, default=0.0,
                    help="keep only the last N hours of the capture; 0 = whole capture")
    ap.add_argument("--in-bin-haircut", type=float, default=1.0,
                    help="fraction of the modeled in-bin fee credit to book (0-1); "
                         "the pro-rata share is an upper bound, so re-gate below 1")
    ap.add_argument("--json", default=None, help="write every cell here")
    args = ap.parse_args(argv)

    events = load_events(args.swaps, args.base_decimals, args.quote_decimals, args.pool)
    if not events:
        print(f"no events in {args.swaps}", file=sys.stderr)
        return 1
    if args.window_hours:
        cutoff = events[-1].ts - args.window_hours * 3600
        events = [e for e in events if e.ts >= cutoff]
        if not events:
            print(f"window of {args.window_hours:g} h leaves no events", file=sys.stderr)
            return 1
    grid = VenueGrid(
        ref_price=10 ** (args.base_decimals - args.quote_decimals),
        bin_step_bps=args.bin_step,
        base_decimals=args.base_decimals,
        quote_decimals=args.quote_decimals,
    )
    sol_price = args.sol_price
    if sol_price is None:
        mids = sorted(grid.price_from_bin(e.active_bin) for e in events)
        sol_price = mids[len(mids) // 2]
    gas_cost = args.gas_lamports * 1e-9 * sol_price
    if args.depth:
        pool_bin_quote = depth_at(args.depth, args.base_decimals, args.quote_decimals,
                                  args.depth_max_age_seconds)
    elif args.pool_bin_quote is not None:
        pool_bin_quote = args.pool_bin_quote
    else:
        pool_bin_quote = estimate_pool_bin_quote(events)

    rows = soak(
        events, grid,
        [int(x) for x in args.width.split(",")],
        [int(x) for x in args.shift.split(",")],
        [float(x) for x in args.tau.split(",")],
        args.split,
        capital=args.capital, gas_cost=gas_cost, band_pct=args.band_pct,
        perp_cost_bps=args.perp_cost_bps, funding_apr=args.funding_apr,
        pool_bin_quote=pool_bin_quote, in_bin_haircut=args.in_bin_haircut,
        protocol_fee_pct=args.protocol_fee_pct,
    )
    span_h = (events[-1].ts - events[0].ts) / 3600
    print(f"{len(events)} swaps over {span_h:.1f} h, gas {gas_cost:.5f}/action, "
          f"perp {args.perp_cost_bps:g} bps, funding {args.funding_apr:g} APR, "
          f"in-bin haircut {args.in_bin_haircut:g}, protocol share {args.protocol_fee_pct:g}%, "
          f"pool depth {'sampled: ' + args.depth if args.depth else f'{pool_bin_quote:,.0f}/bin'}")
    for r in rows:
        print(_fmt(r))
    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"swaps": args.swaps, "span_hours": span_h, "args": vars(args),
                       "cells": rows}, fh, indent=2)
        print(f"\nall cells → {args.json}")
    return 0 if any(r["gate_pass"] for r in rows) else 2


if __name__ == "__main__":
    sys.exit(main())
