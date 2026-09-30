#!/usr/bin/env bash
# Prepared 2h ping-pong soak: one-sided ladder + perp hedge, stress-gated.
#
# Runs the last WINDOW_H hours of a swap capture through pingpong_soak.py with
# the two stress knobs the full-capture pass did not use: a haircut on the
# in-bin fee credit (the pro-rata depth_at share is an upper bound) and an
# adverse perp funding rate (negative = the short pays, so the hedge costs).
#
# Usage: tools/pp_soak_2h.sh <swaps.jsonl> <depth.jsonl> [out.json]
# Env:   WINDOW_H=2 HAIRCUT=0.5 FUNDING_APR=-0.05 POOL=<addr> PY=.venv/bin/python
set -euo pipefail

SWAPS="${1:?usage: pp_soak_2h.sh <swaps.jsonl> <depth.jsonl> [out.json]}"
DEPTH="${2:?missing depth-sampler jsonl}"
OUT="${3:-logs/calibration/pp-2h-$(date -u +%Y%m%dT%H%M%SZ).json}"

WINDOW_H="${WINDOW_H:-2}"
HAIRCUT="${HAIRCUT:-0.5}"
FUNDING_APR="${FUNDING_APR:--0.05}"
POOL="${POOL:-5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6}"
PY="${PY:-.venv/bin/python}"

cd "$(dirname "$0")/.."
mkdir -p "$(dirname "$OUT")"

"$PY" tools/pingpong_soak.py "$SWAPS" \
  --pool "$POOL" --bin-step 4 --base-decimals 9 --quote-decimals 6 \
  --capital 1000 --shift 3 --split 0.5 \
  --window-hours "$WINDOW_H" \
  --width 5,10,20,40 --tau 900,3600,14400 \
  --in-bin-haircut "$HAIRCUT" --funding-apr "$FUNDING_APR" \
  --depth "$DEPTH" --json "$OUT"
