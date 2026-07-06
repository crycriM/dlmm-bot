"""VenueGrid: bin/tick geometry with token decimals for both legs.

DLMM price formula:  price = ref * (1 + binStep/1e4) ** binId
Inverse:             binId = log(price / ref) / log(1 + binStep/1e4)

Token decimals are used to convert raw on-chain amounts to decimal
quantities and back.  All conversions go through the grid so the
round-trip invariant is testable in one place.
"""

import math
from dataclasses import dataclass

@dataclass(frozen=True)
class VenueGrid:
    """Immutable grid carrying reference price, bin-step, and token decimals."""
    ref_price: float
    bin_step_bps: int          # DLMM binStep in basis points (e.g. 2 = 2 bps)
    base_decimals: int
    quote_decimals: int

    def _step(self) -> float:
        """The per-bin price multiplier: (1 + binStep/1e4)."""
        return 1.0 + self.bin_step_bps / 1e4

    def price_from_bin(self, bin_id: int) -> float:
        """Compute price at a given bin index."""
        return self.ref_price * self._step() ** bin_id

    def bin_from_price(self, price: float) -> int:
        """Compute bin index for a given price (rounded to nearest)."""
        if price <= 0 or self.ref_price <= 0:
            raise ValueError("price and ref_price must be positive")
        return round(math.log(price / self.ref_price) / math.log(self._step()))

    def to_decimal(self, raw: int, token: str) -> float:
        """Convert raw on-chain amount to decimal quantity."""
        decimals = self.base_decimals if token == "base" else self.quote_decimals
        return raw / 10 ** decimals

    def to_raw(self, amount: float, token: str) -> int:
        """Convert decimal quantity back to raw on-chain amount."""
        decimals = self.base_decimals if token == "base" else self.quote_decimals
        return round(amount * 10 ** decimals)
