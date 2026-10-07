#!/usr/bin/env bash
# Default is a short unsigned preflight. "live" is an explicit signing action.
# EXPERIMENT_PAIR picks the pool (default sol-usdc); non-SOL-base pairs need SOL_USD.
# Pair parameters and their scaling rule: dlmm-bot/project-internal/soak-basket-20261006.md.
set -euo pipefail
DLMM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXECUTOR_ROOT="$(cd "$DLMM_ROOT/../solana-clmm-executor" && pwd)"
MODE="${1:-preflight}"
case "$MODE" in preflight|live|recover) ;; *) echo 'usage: [EXPERIMENT_PAIR=...] bash tools/live_lp_2h.sh [preflight|live|recover POSITION_ID...]' >&2; exit 2 ;; esac
PAIR="${EXPERIMENT_PAIR:-sol-usdc}"
COOLDOWN=0
WSOL=So11111111111111111111111111111111111111112 USDC=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
# width/shift gap ~ 1.7x/2.5x the pool's median hourly travel in bins; capital/loss in quote units.
case "$PAIR" in
  sol-usdc)  POOL=5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 BASE=$WSOL QUOTE=$USDC
             WIDTH=20 GAP=2 COOLDOWN=1800 CAPITAL=10 LOSS=1 ;;
  zbcn-sol)  POOL=7U8DUKAds4SKeKU9zWNdM6iVXrsLc7fx8VUY5sw2zjWz BASE=ZBCNpuD7YMXzTHB2fhGkGi78MNsHGLRXUhRewNRm9RU QUOTE=$WSOL
             WIDTH=5 GAP=8 CAPITAL=0.08 LOSS=0.008 ;;
  met-usdc)  POOL=5hbf9JP8k5zdrZp9pokPypFQoBse5mGCmW6nqodurGcd BASE=METvsvVRapdj9cFLzq4Tr43xK4tAjQfwX76z3n6mWQL QUOTE=$USDC
             WIDTH=10 GAP=15 CAPITAL=10 LOSS=1 ;;
  usd1-usdc) POOL=7hZQ3QmsocJJLuHrw39R9XqZNzt4BmBBNVNSnvJExtqA BASE=USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB QUOTE=$USDC
             WIDTH=3 GAP=5 CAPITAL=10 LOSS=1 ;;
  *) echo "unknown EXPERIMENT_PAIR '$PAIR' (sol-usdc|zbcn-sol|met-usdc|usd1-usdc)" >&2; exit 2 ;;
esac
SOL_ARGS=()
if [[ "$BASE" != "$WSOL" && "$MODE" != recover ]]; then SOL_ARGS=(--sol-usd "${SOL_USD:?SOL_USD (USD per SOL) is required for $PAIR}"); fi
set -a
source "$EXECUTOR_ROOT/.env.test.write"
set +a
# This account's dRPC tier rejects Solana HTTP and PubSub (RPC code 35).
export SOLANA_STREAM_RPC_URL="$SOLANA_RPC_URL" SOLANA_STREAM_WS_URL="$SOLANA_WS_URL"
# Freeze the envelope; no inherited wider allow-list or relaxed policy cap.
export WALLET_PUBKEY=EHok6xvSGk1tTKabn7z4s4VJyuevQ4UPUymskStU7VvS
export SOLANA_RPC_MAX_CU_PER_SECOND=40 SOLANA_COMMITMENT=finalized
export MAX_SOL_PER_TX=0.23 MAX_SOL_PER_RUN=0.5 MAX_SLIPPAGE_BPS=25
export MAX_ACTIVE_BIN_SLIPPAGE_BINS=1 MAX_PRIORITY_FEE_LAMPORTS=10000
export JITO_ENABLED=false JITO_TIP_LAMPORTS=0
unset DEPTH_SAMPLE_PATH DEPTH_SAMPLE_INTERVAL_S
EXTRA=()
if [[ "$MODE" == live || "$MODE" == recover ]]; then
  export LIVE_WRITE_CONFIRM=yes DRY_RUN=false
  EXTRA+=(--live)
else
  unset LIVE_WRITE_CONFIRM
  export DRY_RUN=true
fi
RUN_DIR="${EXPERIMENT_OUT:-$DLMM_ROOT/logs/live-lp-2h-$PAIR-$(date -u +%Y%m%dT%H%M%SZ)-$MODE}"
npm --prefix "$EXECUTOR_ROOT" run build
mkdir -p "$DLMM_ROOT/logs"
if [[ "$MODE" == recover ]]; then
  shift
  if [[ $# -eq 0 ]]; then echo 'recover requires one or more position IDs' >&2; exit 2; fi
  exec flock -n "$DLMM_ROOT/logs/live-experiment-wallet.lock" \
    "$DLMM_ROOT/.venv/bin/python" "$DLMM_ROOT/tools/recover_live_lp.py" \
    --pool "$POOL" --base-mint "$BASE" --quote-mint "$QUOTE" \
    --wallet "$WALLET_PUBKEY" --out "$RUN_DIR" "$@"
fi
exec flock -n "$DLMM_ROOT/logs/live-experiment-wallet.lock" \
  "$DLMM_ROOT/.venv/bin/python" "$DLMM_ROOT/tools/live_lp_experiment.py" \
  --pool "$POOL" --base-mint "$BASE" --quote-mint "$QUOTE" "${SOL_ARGS[@]}" \
  --wallet "$WALLET_PUBKEY" --capital "${EXPERIMENT_CAPITAL:-$CAPITAL}" \
  --loss-limit "${EXPERIMENT_LOSS_LIMIT:-$LOSS}" --fee-budget-sol 0.01 \
  --duration-seconds 7200 --width "${EXPERIMENT_WIDTH:-$WIDTH}" \
  --shift-gap "${EXPERIMENT_SHIFT_GAP:-$GAP}" \
  --shift-cooldown-seconds "${EXPERIMENT_SHIFT_COOLDOWN_SECONDS:-$COOLDOWN}" --refresh-interval 30 \
  --out "$RUN_DIR" "${EXTRA[@]}"
