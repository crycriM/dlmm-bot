"""DLMM configuration dataclass."""

from dataclasses import dataclass, field
from mm_core.regime import GateConfig
from mm_core.risk_policy import RiskConfig

@dataclass
class DLMMConfig:
    """Full configuration for the DLMM market-maker."""
    gamma: float = 1.0            # risk aversion
    kappa: float = 0.5            # fill-intensity decay
    bin_step_bps: int = 2         # DLMM bin step in basis points
    ref_price: float = 1.0        # reference price for grid
    inner_offset: int = 1         # min bins from center to first level
    levels: int = 5               # number of levels per side
    capital: float = 1000.0       # notional capital allocated
    level_weight: float = 0.2     # per-level weight (equal -> 1/levels)

    # Gate config
    gate: GateConfig = field(default_factory=GateConfig)

    # Risk config
    risk: RiskConfig = field(default_factory=RiskConfig)
