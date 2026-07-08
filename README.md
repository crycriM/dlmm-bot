# dlmm-bot

Dynamic Limit Order market-making bot for Meteora-style DLMM pools on Solana.

This package is the **DLMM execution-side bot** described in `clmm-animation/docs/dlmm-PROJECT_SUMMARY.md`, `dlmm-multi-venue-dlmm-mm-plan.md`, and `mm-bot-shared-architecture.md`. It pairs a Python keeper/backtester with shared logic from `mm_core`, while leaving Solana transaction building to a thin executor bridge.

## What this bot does

`dlmm-bot` is the venue-specific layer for:

- **DLMM ladder placement** around an AS reservation price
- **Keeper loop** that polls state, evaluates regime/risk, and refreshes liquidity
- **DLMM-specific risk** such as TVL rug detection and memecoin one-sided posture
- **Event-replay backtesting** on bin-crossing history
- **Optional perp hedge intents** for exotic pairs, emitted to OPMS rather than traded directly
- **JSON bridge** to a TypeScript/Solana executor for deposit, withdraw, swap, and bundle refresh

## Architecture

The split is intentional:

- **`mm_core`** owns shared math and policy: AS formulas, regime detection, risk policy, PnL, markout, contracts
- **`dlmm-bot`** owns DLMM-specific geometry, ladder construction, keeper orchestration, Solana-side risk, and executor integration

This matches the two-zone design from the docs: the Solana bot runs outside AWS, talks to a local executor, and can emit hedge `ExecIntent`s to the perp side without sharing execution code.

## Package layout

```text
src/dlmm_bot/
  backtest.py      # event-replay DLMM backtester with crossed-bin fees
  config.py        # DLMMConfig
  exec_bridge.py   # JSON stdin/stdout bridge to TS executor
  grid.py          # VenueGrid for DLMM bin geometry + token decimals
  hedge.py         # MA+deadband + vol-scaled hedge controller for exotic pairs
  keeper.py        # poll -> evaluate -> actuate loop
  ladder.py        # AS-driven ladder builder
  risk_dlmm.py     # rug kill-switch, TVL checks, pair-type posture
```

## Core concepts

### VenueGrid

`VenueGrid` centralizes:

- `price <-> bin` conversion using `ref_price` and `bin_step_bps`
- token decimal conversion for base and quote legs

This fixes the common DLMM mistake of treating bin 0 as price 1 without respecting pool scaling.

### Ladder placement

`build_ladder()` maps AS outputs onto DLMM bins:

- reservation price shifts the ladder center
- half-spread determines inner distance from center
- skew adjusts bid/ask capital split

The bot places liquidity as discrete bin levels, not CLOB orders.

### Keeper loop

`Keeper` follows the design docs:

1. Poll pool state from the executor bridge
2. Build price history and evaluate regime with `mm_core`
3. Run shared risk policy plus DLMM-specific TVL/inventory checks
4. Refresh, withdraw, or de-risk liquidity
5. Record per-cycle decisions for shadow/dry-run operation

### DLMM-specific risk posture

Implemented pair modes:

- **Bluechip**: full ladder, looser gate
- **Memecoin**: one-sided posture, stricter gate, rug/liquidity kill-switch
- **Exotic**: optional hedge path, relaxed gate when hedge is active

### Hedging

For hedgeable exotic pairs, `hedge.py` implements:

- EMA target inventory
- deadband from the cube-root transaction-cost rule
- vol-scaled hedge window
- hard delta-cap backstop

It emits `ExecIntent`s for an external perp executor instead of placing hedge orders itself.

### Backtesting

`DLMMBacktester` replays historical bin-crossing events and tracks:

- crossed-bin LP fee income
- fills at bin prices
- rebalance cost
- spread capture and markout
- drawdown, Sharpe, and fill-rate style metrics

## Install

```bash
pip install -e .
```

Requires Python 3.11+ and `mm_core`.

## Tests

```bash
pytest
```

The test suite covers:

- grid/bin conversion invariants
- ladder center shift and skew behavior
- hedge controller behavior
- DLMM risk rules
- keeper-loop decisions
- executor bridge protocol
- backtest accounting and metrics

