"""Ladder placement driven by Avellaneda–Stoikov reservation price.

The active bin is shifted by the AS inventory-tilt to produce a new
ladder center.  Each side is one contiguous block of `levels` bins whose
innermost bin sits `inner_offset` bins from center, and per-level sizes are
scaled by skew derived from inventory error.

Contiguous, not spaced every `inner` bins: the executor deposits each side
with Meteora's native one-sided instruction over a single contiguous range
and rejects gaps (2026-09-28: a spaced γ=30/κ=20 ladder failed live with
`bin_ids must be contiguous`). For inner = 1 both shapes are identical.
Neither side may reach across the active bin — bids stay below it, asks at
or above it — so a tilt larger than the offset slides that side's block to
the active-bin edge instead of placing liquidity the pool cannot hold.

AS mapping:
    center = active_bin + round((r - S) / bin_width_in_bins)
    inner_offset = round(half_spread / bin_width_in_bins)
    ask_frac = 0.5 * (1 + skew),  bid_frac = 1 - ask_frac
    per-level notional = capital * side_frac * level_weight  (quote units)

Level sizes are in the token the level deposits: bids in quote, asks in
base (notional / ask-bin price) — the units keeper._bin_payload sends.
"""

import math
from dataclasses import dataclass
from dlmm_bot.grid import VenueGrid

@dataclass
class LadderLevel:
    bin_id: int
    side: str          # "bid" | "ask"
    size: float        # bid: quote units, ask: base units

@dataclass
class LadderConfig:
    levels: int = 5
    inner_offset: int = 1
    capital: float = 1000.0
    level_weight: float = 0.2   # equal-weight default; decayed in production

def build_ladder(
    grid: VenueGrid,
    active_bin: int,
    r: float,            # AS reservation price
    S: float,            # current mid price
    half_spread: float,  # AS half-spread
    skew: float,         # inventory-derived skew in (-1, 1)
    cfg: LadderConfig = LadderConfig(),
) -> list[LadderLevel]:
    """Build a symmetric ladder around the AS-shifted center.

    Returns levels sorted by bin_id ascending.
    """
    if S <= 0 or r <= 0:
        raise ValueError("r and S must be positive prices")

    step = grid._step()
    bin_width = math.log(step)  # one bin in log-price space

    # Center shift from AS inventory tilt.  r and S are prices, bins are
    # log-price steps, so convert the tilt to log space before dividing.
    center_shift = round(math.log(r / S) / bin_width) if bin_width > 0 else 0
    center = active_bin + center_shift

    # Inner offset in bins: half_spread is a price distance around S
    offset_bins = (
        round(math.log1p(half_spread / S) / bin_width) if bin_width > 0 else cfg.inner_offset
    )
    inner = max(cfg.inner_offset, offset_bins)

    # Skew-derived fractions
    ask_frac = 0.5 * (1.0 + skew)
    bid_frac = 1.0 - ask_frac

    # Innermost bins, slid back to the active-bin edge when the tilt crosses it.
    top_bid = min(center - inner, active_bin - 1)
    low_ask = max(center + inner, active_bin)

    levels: list[LadderLevel] = []
    for i in range(cfg.levels):
        weight = cfg.level_weight
        bid_bin = top_bid - i
        bid_size = cfg.capital * bid_frac * weight
        levels.append(LadderLevel(bin_id=bid_bin, side="bid", size=bid_size))
        ask_bin = low_ask + i
        ask_size = cfg.capital * ask_frac * weight / grid.price_from_bin(ask_bin)
        levels.append(LadderLevel(bin_id=ask_bin, side="ask", size=ask_size))

    return sorted(levels, key=lambda l: l.bin_id)
