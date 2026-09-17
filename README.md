# dlmm-bot

Dynamic Limit Order market-making bot for Meteora-style DLMM pools on Solana.

This package pairs a Python DLMM keeper and event-replay backtester with shared
market-making logic from `mm_core`, while leaving Solana transaction building
to a thin executor bridge.

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

Each keeper cycle:

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

Create a project-local virtual environment. Do not install into the system
Python, and do not reuse another project's environment.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ../mm-core
python -m pip install -e ".[dev]"
```

Requires Python 3.11+ and the sibling `mm-core` checkout. The optional executor
integration tests also require Node.js and a built sibling
`solana-clmm-executor` checkout.

## Tests

The default suite is offline and does not need wallet credentials or network
access:

```bash
pytest
```

The opt-in subprocess lane uses the sibling executor's deterministic offline
handlers. It generates a test-only wallet key, reads required public IDs from
the sibling's canonical fixtures, and uses a deliberately closed loopback RPC
endpoint; it does not use a live wallet or pool:

```bash
pytest --executor-subprocess
```

The test suite covers:

- grid/bin conversion invariants
- ladder center shift and skew behavior
- hedge controller behavior
- DLMM risk rules
- keeper-loop decisions
- executor bridge protocol
- backtest accounting and metrics

## Configuration and secrets

`dlmm-bot` does not own exchange credentials. The Solana executor is the
execution boundary and receives its configuration through environment
variables at runtime. Never commit RPC URLs containing access tokens, API
keys, private keys, seed phrases, keypair files, wallet addresses, pool
addresses, or position addresses.

Local `.env` variants, key files, wallet JSON files, event logs, and runtime
log directories are ignored by Git. If a credential is ever committed,
revoke it and remove it from the full Git history before publishing; deleting
it only from the latest revision is insufficient.

`tools/live_keeper_soak.py` is an explicit, read-only integration gate. It
requires runtime-supplied public identifiers and an RPC endpoint, rejects
secret-bearing wallet variables, forces both layers into dry-run mode, and
sets all transaction budgets to zero. Keep real values in your local secret
manager or untracked environment, never in scripts or tests.

## Safety status

This project is experimental trading software. Default tests and examples are
offline. Before any live deployment, follow the staged backtest, shadow,
appropriate pre-production gates, and micro-capital rollout described in the
parent project documentation. Review `status.md` for current implementation
gaps and do not treat a passing unit suite as evidence of live-trading safety.
