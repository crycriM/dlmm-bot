#!/usr/bin/env python
"""
AS gamma/kappa calibration sweep over a recorded swap stream.

Feeds the executor's `swaps.jsonl` (spec §6 rows) through ``DLMMBacktester``
once per (gamma, kappa) pair, splits the capture in time, and reports the
in-sample winner's out-of-sample metrics next to the out-of-sample winner.
The gap between those two rows is the overfit estimate — a sweep that only
prints its best in-sample cell is a sales pitch, not a calibration.

Amounts come from the `*_raw` integer fields, never from the decimal
`amount_in`/`amount_out` fields: rows captured before 2026-09-18 carry those
mispriced (inverted `swapForY` mapping in the executor's `swapRows.ts`).

Usage:
    python tools/calibrate.py <swaps.jsonl> [--bin-step 4] [--capital 1000]
"""

from __future__ import annotations

import argparse
import json
import sys

from dlmm_bot.backtest import BinEvent, DLMMBacktester
from dlmm_bot.config import DLMMConfig
from dlmm_bot.grid import VenueGrid

# Defaults match the WSOL/USDC pool the M2/M3 gates ran against.
DEFAULT_GAMMAS = "0.1,0.3,1,3,10,30,100"
DEFAULT_KAPPAS = "0.5,1,2,5,10,20,50"


def load_events(
    path: str, base_decimals: int, quote_decimals: int, pool: str | None = None
) -> list[BinEvent]:
    """Swap-stream rows → BinEvents, sized in quote units from the raw fields."""
    events: list[BinEvent] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if pool and row.get("pool") != pool:
                continue
            prev = int(row["prev_active_bin"])
            new = int(row["new_active_bin"])
            # Direction from the bins themselves; the row's own field ties
            # no-move swaps to "down" arbitrarily.
            direction = "up" if new > prev else "down"
            # An up-swap pays quote in, a down-swap takes quote out. Either
            # way the quote leg is the trade's notional.
            quote_raw = int(
                row["amount_in_raw"] if direction == "up" else row["amount_out_raw"]
            )
            events.append(BinEvent(
                ts=float(row.get("block_time") or row["ts"]),
                pool=row.get("pool", ""),
                active_bin=new,
                prev_active_bin=prev,
                direction=direction,
                trade_size_usd=quote_raw / 10 ** quote_decimals,
                fee_bps=float(row.get("fee_bps") or 0.0),
                tvl_usd=row.get("tvl_usd"),
            ))
    events.sort(key=lambda e: e.ts)
    return events


def run_one(
    gamma: float, kappa: float, events: list[BinEvent], grid: VenueGrid, base: DLMMConfig
) -> dict:
    """One backtest at one (gamma, kappa)."""
    cfg = DLMMConfig(**{**base.__dict__, "gamma": gamma, "kappa": kappa})
    result = DLMMBacktester(cfg, grid, events, initial_capital=cfg.capital).run()
    return {"gamma": gamma, "kappa": kappa, **result.to_dict()}


def sweep(
    gammas: list[float], kappas: list[float],
    events: list[BinEvent], grid: VenueGrid, base: DLMMConfig,
) -> list[dict]:
    return [run_one(g, k, events, grid, base) for g in gammas for k in kappas]


def _fmt(row: dict) -> str:
    return (
        f"  gamma={row['gamma']:<6g} kappa={row['kappa']:<6g} "
        f"pnl={row['total_pnl']:>12.4f}  fills={row['n_fills']:>4d}  "
        f"refresh={row['n_refreshes']:>4d}  dd={row['max_drawdown']:>7.3f}%  "
        f"sharpe={row['sharpe']:>9.2f}"
    )


def calibrate(
    events: list[BinEvent], grid: VenueGrid, base: DLMMConfig,
    gammas: list[float], kappas: list[float], split: float,
) -> dict:
    """In-sample sweep, then the winner's out-of-sample run."""
    cut = int(len(events) * split)
    in_sample, out_sample = events[:cut], events[cut:]
    if not in_sample or not out_sample:
        raise ValueError(f"split {split} leaves one side empty ({len(events)} events)")

    is_rows = sweep(gammas, kappas, in_sample, grid, base)
    oos_rows = sweep(gammas, kappas, out_sample, grid, base)
    by_key = {(r["gamma"], r["kappa"]): r for r in oos_rows}

    best_is = max(is_rows, key=lambda r: r["total_pnl"])
    best_oos = max(oos_rows, key=lambda r: r["total_pnl"])
    chosen_oos = by_key[(best_is["gamma"], best_is["kappa"])]
    return {
        "n_events": len(events),
        "n_in_sample": len(in_sample),
        "n_out_sample": len(out_sample),
        "n_crossings": sum(1 for e in events if e.active_bin != e.prev_active_bin),
        "span_seconds": events[-1].ts - events[0].ts,
        "in_sample": is_rows,
        "out_of_sample": oos_rows,
        "best_in_sample": best_is,
        "chosen_out_of_sample": chosen_oos,
        "best_out_of_sample": best_oos,
        # A sweep whose winner never fills has calibrated nothing.
        "any_fills": any(r["n_fills"] for r in is_rows + oos_rows),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("swaps", help="path to a swap-stream .jsonl capture")
    ap.add_argument("--pool", default=None, help="filter rows to one pool address")
    ap.add_argument("--bin-step", type=int, default=4, help="pool binStep in bps")
    ap.add_argument("--base-decimals", type=int, default=9)
    ap.add_argument("--quote-decimals", type=int, default=6)
    ap.add_argument("--gamma", default=DEFAULT_GAMMAS)
    ap.add_argument("--kappa", default=DEFAULT_KAPPAS)
    ap.add_argument("--capital", type=float, default=1000.0)
    ap.add_argument("--levels", type=int, default=5)
    ap.add_argument("--inner-offset", type=int, default=1)
    ap.add_argument("--split", type=float, default=0.5, help="in-sample fraction")
    ap.add_argument("--json", default=None, help="write the full sweep here")
    args = ap.parse_args(argv)

    events = load_events(
        args.swaps, args.base_decimals, args.quote_decimals, args.pool
    )
    if not events:
        print(f"no events in {args.swaps}", file=sys.stderr)
        return 1

    # DLMM prices are raw ratios; the decimal price is scaled by the decimal
    # difference, so that scale *is* the grid's reference price at bin 0.
    grid = VenueGrid(
        ref_price=10 ** (args.base_decimals - args.quote_decimals),
        bin_step_bps=args.bin_step,
        base_decimals=args.base_decimals,
        quote_decimals=args.quote_decimals,
    )
    base = DLMMConfig(
        bin_step_bps=args.bin_step, ref_price=grid.ref_price,
        levels=args.levels, inner_offset=args.inner_offset,
        capital=args.capital, level_weight=1.0 / max(args.levels, 1),
    )
    gammas = [float(x) for x in args.gamma.split(",")]
    kappas = [float(x) for x in args.kappa.split(",")]

    report = calibrate(events, grid, base, gammas, kappas, args.split)

    print(
        f"{report['n_events']} events ({report['n_crossings']} bin crossings) "
        f"over {report['span_seconds'] / 60:.1f} min — "
        f"{report['n_in_sample']} in-sample / {report['n_out_sample']} out-of-sample"
    )
    print(f"grid: {len(gammas)} gamma x {len(kappas)} kappa = {len(gammas) * len(kappas)} cells")
    print("best in-sample:      " + _fmt(report["best_in_sample"]).strip())
    print("  same cell, OOS:    " + _fmt(report["chosen_out_of_sample"]).strip())
    print("best out-of-sample:  " + _fmt(report["best_out_of_sample"]).strip())
    if not report["any_fills"]:
        print(
            "\nNO FILLS in any cell: the ladder never rested where price went. "
            "Calibrated nothing — widen the grid, shrink inner_offset, or "
            "capture a longer window.",
            file=sys.stderr,
        )
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nfull sweep → {args.json}")
    return 0 if report["any_fills"] else 2


if __name__ == "__main__":
    sys.exit(main())
