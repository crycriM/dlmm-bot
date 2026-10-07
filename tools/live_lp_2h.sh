#!/usr/bin/env bash
# Default is a short unsigned preflight. "live" is an explicit signing action.
set -euo pipefail
DLMM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXECUTOR_ROOT="$(cd "$DLMM_ROOT/../solana-clmm-executor" && pwd)"
MODE="${1:-preflight}"
case "$MODE" in preflight|live|recover) ;; *) echo 'usage: bash tools/live_lp_2h.sh [preflight|live|recover POSITION_ID...]' >&2; exit 2 ;; esac
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
RUN_DIR="${EXPERIMENT_OUT:-$DLMM_ROOT/logs/live-lp-2h-$(date -u +%Y%m%dT%H%M%SZ)-$MODE}"
npm --prefix "$EXECUTOR_ROOT" run build
mkdir -p "$DLMM_ROOT/logs"
if [[ "$MODE" == recover ]]; then
  shift
  if [[ $# -eq 0 ]]; then echo 'recover requires one or more position IDs' >&2; exit 2; fi
  exec flock -n "$DLMM_ROOT/logs/live-experiment-wallet.lock" \
    "$DLMM_ROOT/.venv/bin/python" "$DLMM_ROOT/tools/recover_live_lp.py" \
    --pool 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 \
    --wallet "$WALLET_PUBKEY" --out "$RUN_DIR" "$@"
fi
exec flock -n "$DLMM_ROOT/logs/live-experiment-wallet.lock" \
  "$DLMM_ROOT/.venv/bin/python" "$DLMM_ROOT/tools/live_lp_experiment.py" \
  --pool 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 \
  --base-mint So11111111111111111111111111111111111111112 \
  --quote-mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v \
  --wallet "$WALLET_PUBKEY" --capital "${EXPERIMENT_CAPITAL:-10}" \
  --loss-limit "${EXPERIMENT_LOSS_LIMIT:-1}" --fee-budget-sol 0.01 \
  --duration-seconds 7200 --width 20 --shift-gap "${EXPERIMENT_SHIFT_GAP:-2}" \
  --shift-cooldown-seconds "${EXPERIMENT_SHIFT_COOLDOWN_SECONDS:-1800}" --refresh-interval 30 \
  --out "$RUN_DIR" "${EXTRA[@]}"
