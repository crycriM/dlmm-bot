#!/usr/bin/env python
"""
Screen a DLMM pool from hourly OHLCV bars, before spending days capturing swaps.

Bars cannot simulate fills — a 4 bps bin is crossed many times inside one hour
and the bar collapses that path into four numbers. What they can do is answer
the three questions that decide whether a capture is worth running at all:

  1. How volatile is this pair?  (sigma feeds the AS half-spread directly)
  2. How far does price travel per hour, measured in bins?  A ladder placed
     inside that range fills; one placed outside never does, and a drift
     threshold below it refreshes every hour.
  3. Does the pool pay enough fees to cover the refreshes that travel forces?

It also prints the gamma/kappa -> inner-offset table at the measured sigma,
which is the only channel gamma and kappa currently have into the ladder
(`build_ladder` rounds the half-spread to whole bins; see the calibration
section of project-internal/status.md).

Usage:
    python tools/pool_screen.py ~/Python/github/data/meteora/*.csv --bin-step 4
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
import sys

from mm_core.as_core import gueant_half_spread
from mm_core.vol import (
    estimate_volatility_close_to_close,
    estimate_volatility_parkinson,
)

GAMMAS = (0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0)
KAPPAS = (0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0)


def load_bars(path: str) -> list[dict]:
    """OHLCV CSV → bars, oldest first."""
    with open(path) as fh:
        bars = [
            {
                "ts": float(row["timestamp"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
            for row in csv.DictReader(fh)
        ]
    bars.sort(key=lambda b: b["ts"])
    return bars


def bins_per_bar(bars: list[dict], bin_step_bps: int) -> list[float]:
    """Each bar's high/low range expressed in bins."""
    width = math.log1p(bin_step_bps / 1e4)
    return [
        math.log(b["high"] / b["low"]) / width
        for b in bars
        if b["low"] > 0 and b["high"] >= b["low"]
    ]


def screen(bars: list[dict], bin_step_bps: int, fee_bps: float) -> dict:
    """Vol, travel-in-bins, and fee revenue for one pool."""
    closes = [(b["ts"], b["close"]) for b in bars]
    # Parkinson wants the real high/low, so feed it the extremes of each bar
    # rather than the close series the mm_core helper would otherwise window.
    hl = [(b["ts"], p) for b in bars for p in (b["high"], b["low"])]
    ranges = bins_per_bar(bars, bin_step_bps)
    volumes = [b["volume"] for b in bars]
    return {
        "n_bars": len(bars),
        "span_days": (bars[-1]["ts"] - bars[0]["ts"]) / 86400 if bars else 0.0,
        "last_price": bars[-1]["close"] if bars else 0.0,
        "sigma_annual_c2c": estimate_volatility_close_to_close(closes),
        "sigma_annual_parkinson": estimate_volatility_parkinson(hl),
        "bins_per_hour_median": statistics.median(ranges) if ranges else 0.0,
        "bins_per_hour_p90": (
            sorted(ranges)[int(len(ranges) * 0.9)] if ranges else 0.0
        ),
        "volume_per_hour_median": statistics.median(volumes) if volumes else 0.0,
        "pool_fees_per_hour_median": (
            statistics.median(volumes) * fee_bps / 1e4 if volumes else 0.0
        ),
    }


def inner_offsets(sigma_annual: float, price: float, bin_step_bps: int) -> dict:
    """(gamma, kappa) → the ladder's inner offset in bins, at this sigma."""
    width = math.log1p(bin_step_bps / 1e4)
    return {
        (g, k): max(
            1,
            round(math.log1p(gueant_half_spread(g, sigma_annual, k) / price) / width),
        )
        for g in GAMMAS
        for k in KAPPAS
    }


def report(name: str, bars: list[dict], bin_step_bps: int, fee_bps: float,
           capital: float, tvl: float | None) -> dict:
    s = screen(bars, bin_step_bps, fee_bps)
    print(f"\n=== {name}")
    print(f"  {s['n_bars']} bars over {s['span_days']:.0f} days, last {s['last_price']:.4f}")
    print(f"  sigma annualized: {s['sigma_annual_c2c'] * 100:>7.1f}% close-to-close, "
          f"{s['sigma_annual_parkinson'] * 100:>7.1f}% Parkinson")
    print(f"  hourly travel:    {s['bins_per_hour_median']:>7.1f} bins median, "
          f"{s['bins_per_hour_p90']:>7.1f} bins p90  (bin step {bin_step_bps} bps)")
    print(f"  hourly volume:    {s['volume_per_hour_median']:>12,.0f} median "
          f"→ {s['pool_fees_per_hour_median']:>10,.2f} in pool fees at {fee_bps} bps")
    # Fees accrue only to the *active bin's* liquidity, so a pool-wide TVL
    # share is the wrong denominator in both directions: concentrated bins
    # earn far more than it, bins the price never reaches earn nothing. Print
    # the sensitivity and let the deposit's real bin share pick the row.
    print("  our fee income per hour, by share of ACTIVE-BIN liquidity:")
    for share in (0.001, 0.01, 0.05):
        print(f"      {share * 100:>4.1f}% → {s['pool_fees_per_hour_median'] * share:>9.3f}")
    if tvl:
        print(f"      (for reference, {capital:,.0f} is {capital / tvl * 100:.4f}% of "
              f"{tvl:,.0f} pool-wide TVL — only reachable if liquidity were uniform)")
    # Refresh pressure. A bar's high/low range understates the path, so this is
    # a floor on how often the keeper's drift threshold is exceeded.
    print(f"  drift ≥3 bins exceeded in at least "
          f"{100 * sum(1 for r in bins_per_bar(bars, bin_step_bps) if r >= 3) / max(len(bars), 1):.0f}%"
          f" of hours (each refresh = withdraw + swap + deposit)")
    return s


def print_offsets(sigma: float, price: float, bin_step_bps: int) -> None:
    table = inner_offsets(sigma, price, bin_step_bps)
    print(f"\n  ladder inner offset in bins at sigma={sigma * 100:.1f}%:")
    print("    gamma\\kappa " + "".join(f"{k:>7g}" for k in KAPPAS))
    for g in GAMMAS:
        print(f"    {g:<11g}" + "".join(f"{table[(g, k)]:>7d}" for k in KAPPAS))
    distinct = len(set(table.values()))
    print(f"    {distinct} distinct ladders across {len(table)} cells")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csvs", nargs="+", help="OHLCV CSV files (timestamp,open,high,low,close,volume)")
    ap.add_argument("--bin-step", type=int, default=4, help="pool binStep in bps")
    ap.add_argument("--fee-bps", type=float, default=4.0, help="pool base fee in bps")
    ap.add_argument("--capital", type=float, default=1000.0)
    ap.add_argument("--tvl", type=float, default=None, help="pool TVL, for the fee-share bound")
    ap.add_argument("--offsets", action="store_true",
                    help="also print the gamma/kappa → inner-offset table")
    args = ap.parse_args(argv)

    for path in args.csvs:
        bars = load_bars(path)
        if len(bars) < 2:
            print(f"{path}: too few bars", file=sys.stderr)
            continue
        s = report(path.split("/")[-1], bars, args.bin_step, args.fee_bps,
                   args.capital, args.tvl)
        if args.offsets:
            print_offsets(s["sigma_annual_c2c"], s["last_price"], args.bin_step)
    return 0


if __name__ == "__main__":
    sys.exit(main())
