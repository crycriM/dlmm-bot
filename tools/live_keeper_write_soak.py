"""Write-enabled keeper soak: a real, signed dust position held and refreshed
against the sibling Solana executor for a bounded duration.

This is executor test plan §8.11 ("soak position"). It exists to accumulate
live fill/markout/refresh evidence under a real position, using parameters
chosen from the offline sweep (``dlmm-bot/tools/calibrate.py``) — it is not
a calibration gate and proves nothing about profitability by itself.
Read-only observation is ``live_keeper_soak.py``; this is its write-enabled
counterpart, kept a separate file on purpose so a reviewer scanning for
"never loads a signer" never has to read past a name that says otherwise.

Analysis is offline, after the run, with the tools that already do it:
  tools/verify_log.py   <run.jsonl>              # action/fill/state reconciliation
  tools/replay.py       <run.jsonl>               # deterministic replay
  ../solana-clmm-executor/tools/audit-swaps.ts     # independent swap-stream audit

Safety, mirrors executor test plan §10:
  - LIVE_WRITE_CONFIRM=yes and DRY_RUN=false must both be set in the sourced
    environment before this script will sign anything. Omit
    LIVE_WRITE_CONFIRM to rehearse the CLI/plumbing: it forces DRY_RUN=true
    locally and no verb is ever signed.
  - a fresh position by default; attaching to an existing one needs the
    explicit --position-id/--extra-position-ids (resume after a restart).
  - a unique run directory per launch holding the keeper's own hash-chained
    event log, the executor's audit JSONL, and the swap-stream capture.
  - SIGTERM/SIGINT and the --duration-seconds deadline both route through the
    same clean-stop path: every position this run opened is withdrawn
    (_stop_quoting) while the event log is still open, so the withdrawal is
    on the record — only then is the log finalized. A run that never opened
    a position (e.g. stopped before the gate first opened) finalizes with
    nothing to withdraw.
  - every cap (MAX_SOL_PER_TX, MAX_SOL_PER_RUN, MAX_SLIPPAGE_BPS, ...) comes
    from the sourced environment, not from this script, so the executor's
    own tested policy enforcement is the actual backstop.
  - the resolved plan (pool, mints, gamma/kappa, capital, duration, caps,
    wallet) is printed and written into the run directory before the
    subprocess starts.

Usage:
  set -a; . .env.test.write; set +a          # LIVE_WRITE_CONFIRM unset: rehearsal
  .venv/bin/python tools/live_keeper_write_soak.py \\
    --pool 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 \\
    --base-mint So11111111111111111111111111111111111111112 \\
    --quote-mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v \\
    --gamma 10 --kappa 20 --capital 5 --duration-seconds 14400

  # then, to actually sign:
  export LIVE_WRITE_CONFIRM=yes   # DRY_RUN=false already in .env.test.write
  .venv/bin/python tools/live_keeper_write_soak.py ...same args...
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dlmm_bot.config import DLMMConfig
from dlmm_bot.exec_bridge import ExecBridge
from dlmm_bot.grid import VenueGrid
from dlmm_bot.keeper import Keeper, KeeperConfig
from dlmm_bot.oracle import OracleBars
from dlmm_bot.risk_dlmm import PairType

sys.path.insert(0, os.path.dirname(__file__))
from live_keeper_soak import SECRET_ENV, close_bridge  # noqa: E402

logger = logging.getLogger(__name__)

EXECUTOR = Path(__file__).resolve().parents[2] / "solana-clmm-executor"

# The executor's own configuration surface (opms-spec.md §9 / test plan §7).
# ExecBridge inherits os.environ into the subprocess: forward exactly this
# allow-list, nothing an operator's shell happens to also export.
GATEWAY_ENV_KEYS = (
    "SOLANA_RPC_URL", "SOLANA_RPC_WRITE_URL", "SOLANA_WS_URL",
    "SOLANA_RPC_MAX_CU_PER_SECOND", "SOLANA_COMMITMENT", "WALLET_SIGNER",
    "KMS_KEY_ARN", "WALLET_SECRET_ARN", "WALLET_KEYPAIR_PATH", "WALLET_PUBKEY",
    "FILE_SIGNER_ALLOW_MAINNET", "POOL_ALLOWLIST", "MINT_ALLOWLIST",
    "MAX_SOL_PER_TX", "MAX_SOL_PER_RUN", "MAX_SLIPPAGE_BPS",
    "MAX_ACTIVE_BIN_SLIPPAGE_BINS", "MAX_PRIORITY_FEE_LAMPORTS",
    "JITO_ENABLED", "JITO_BLOCK_ENGINE_URL", "JITO_TIP_LAMPORTS",
    "JITO_TIP_ACCOUNT", "JITO_TIP_ACCOUNTS",
)
REQUIRED_WRITE_ENV = (
    "SOLANA_RPC_URL", "WALLET_PUBKEY", "MAX_SOL_PER_TX", "MAX_SOL_PER_RUN",
    "MAX_SLIPPAGE_BPS", "MAX_ACTIVE_BIN_SLIPPAGE_BINS", "MAX_PRIORITY_FEE_LAMPORTS",
)


def build_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--pool", required=True)
    ap.add_argument("--base-mint", required=True)
    ap.add_argument("--quote-mint", required=True)
    ap.add_argument("--bin-step-bps", type=int, default=4)
    ap.add_argument("--base-decimals", type=int, default=9)
    ap.add_argument("--quote-decimals", type=int, default=6)
    # No defaults: a money-moving script should force a deliberate choice
    # (test plan §7: "explicit live signer/custody/cap configuration, rather
    # than falling back to fixture values").
    ap.add_argument("--gamma", type=float, required=True)
    ap.add_argument("--kappa", type=float, required=True)
    ap.add_argument("--capital", type=float, required=True, help="quote units")
    ap.add_argument("--duration-seconds", type=float, required=True)
    ap.add_argument("--levels", type=int, default=5)
    ap.add_argument("--inner-offset", type=int, default=1)
    ap.add_argument("--level-weight", type=float, default=0.2)
    ap.add_argument("--drift-threshold-bins", type=int, default=3)
    ap.add_argument("--max-active-bin-slippage", type=int, default=2)
    ap.add_argument("--refresh-interval", type=float, default=5.0)
    ap.add_argument("--pair-type", default="bluechip")
    ap.add_argument("--no-regime-stop", action="store_true",
                    help="separate test arm (executor test plan §8.11): "
                         "other risk guards stay active")
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--oracle-pool", default=None,
                     help="regime/σ from this pool's GeckoTerminal minute bars")
    src.add_argument("--oracle-cross", default=None, metavar="POOL_A,POOL_B",
                     help="regime/σ from A/USD ÷ B/USD minute bars")
    ap.add_argument("--position-id", default=None,
                    help="resume: attach to an existing position instead of "
                         "starting fresh (never the default)")
    ap.add_argument("--extra-position-ids", default="",
                    help="comma-separated ask-side PDAs to resume alongside --position-id")
    ap.add_argument("--executor-cwd", default=str(EXECUTOR))
    ap.add_argument("--out", default=None, help="run directory (default: timestamped)")
    return ap.parse_args(argv)


def validate_env(confirm: bool) -> None:
    for key, value in os.environ.items():
        if (value and key.startswith(("SOLANA_", "WALLET_", "KMS_"))
                and SECRET_ENV.search(key)):
            raise ValueError(f"secret-bearing environment variable is forbidden: {key}")
    if not confirm:
        return
    if os.environ.get("DRY_RUN") != "false":
        raise ValueError("LIVE_WRITE_CONFIRM=yes requires DRY_RUN=false in the sourced env")
    for name in REQUIRED_WRITE_ENV:
        if not os.environ.get(name, "").strip():
            raise ValueError(f"LIVE_WRITE_CONFIRM=yes requires {name} in the environment")
    if not (EXECUTOR / "dist" / "bridge.js").is_file():
        raise ValueError("executor dist/bridge.js is missing; run npm run build")


def configure_executor(args: argparse.Namespace, run_dir: Path, confirm: bool) -> None:
    inherited = {k: v for k, v in os.environ.items()
                if k in ("PATH", "HOME", "LANG", "TZ") or k in GATEWAY_ENV_KEYS}
    os.environ.clear()
    os.environ.update(inherited)
    os.environ["DRY_RUN"] = "true" if not confirm else "false"
    os.environ.setdefault("POOL_ALLOWLIST", args.pool)
    os.environ.setdefault("MINT_ALLOWLIST", f"{args.base_mint},{args.quote_mint}")
    os.environ["SWAP_STREAM_PATH"] = str(run_dir / "swaps.jsonl")
    os.environ["EXECUTOR_LOG_DIR"] = str(run_dir / "executor")
    if not confirm:
        logger.warning("LIVE_WRITE_CONFIRM != yes: executor runs DRY_RUN=true (no signing)")


def _oracle_from_args(args: argparse.Namespace) -> OracleBars | None:
    if args.oracle_pool:
        return OracleBars(source={"pool": args.oracle_pool})
    if args.oracle_cross:
        pool_a, pool_b = args.oracle_cross.split(",")
        return OracleBars(source={"cross": [pool_a, pool_b]})
    return None


def build_keeper(args: argparse.Namespace, bridge: ExecBridge, run_dir: Path) -> Keeper:
    grid = VenueGrid(
        ref_price=10 ** (args.base_decimals - args.quote_decimals),
        bin_step_bps=args.bin_step_bps,
        base_decimals=args.base_decimals,
        quote_decimals=args.quote_decimals,
    )
    cfg = KeeperConfig(
        dlmm=DLMMConfig(
            gamma=args.gamma, kappa=args.kappa, bin_step_bps=args.bin_step_bps,
            ref_price=grid.ref_price, levels=args.levels, inner_offset=args.inner_offset,
            capital=args.capital, level_weight=args.level_weight,
        ),
        grid=grid,
        pool_address=args.pool,
        drift_threshold_bins=args.drift_threshold_bins,
        max_active_bin_slippage=args.max_active_bin_slippage,
        refresh_interval=args.refresh_interval,
        pair_type=PairType(args.pair_type),
        regime_stop=not args.no_regime_stop,
        dry_run=False,
        position_id=args.position_id,
        extra_position_ids=tuple(
            p for p in args.extra_position_ids.split(",") if p
        ),
        log_dir=str(run_dir / "keeper"),
        base_mint=args.base_mint,
        quote_mint=args.quote_mint,
        executor_version="solana-clmm-executor/write-soak",
    )
    return Keeper(cfg=cfg, exec_bridge=bridge, oracle=_oracle_from_args(args))


async def run_soak(keeper: Keeper, duration: float, stop_event: asyncio.Event) -> str:
    """Cycle until `duration` elapses or `stop_event` fires (SIGTERM/SIGINT),
    then withdraw everything this run opened — while the event log is still
    open — and only then finalize it.

    ``keeper.run()``'s own ``finally`` closes the log the instant its loop
    exits, which would silently drop the clean-stop withdrawal from the
    audit trail; driving cycles here instead keeps withdrawal-before-close
    order under our control.
    """
    deadline = time.monotonic() + duration
    stop_reason = "duration_elapsed"
    keeper._running = True
    try:
        while time.monotonic() < deadline and not stop_event.is_set():
            try:
                await keeper._cycle()
            except Exception:
                logger.exception("Keeper cycle error (soak continues)")
            remaining = min(deadline - time.monotonic(), keeper.cfg.refresh_interval)
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=remaining)
                stop_reason = "signal"
            except TimeoutError:
                pass
    finally:
        keeper._running = False
        if keeper._current_position_id or keeper._extra_position_ids:
            try:
                await keeper._stop_quoting()
            except Exception:
                logger.exception("Clean-stop withdrawal failed — reconcile manually "
                                 "via verify_log.py before treating the wallet as drained")
        keeper.stop()
    return stop_reason


def main(argv=None) -> int:
    args = build_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    confirm = os.environ.get("LIVE_WRITE_CONFIRM") == "yes"
    validate_env(confirm)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = Path(args.out) if args.out else (
        EXECUTOR / "logs" / "test-artifacts" / f"write-soak-{stamp}"
    )
    run_dir.mkdir(parents=True, exist_ok=False)
    configure_executor(args, run_dir, confirm)

    plan = {
        "confirm": confirm, "pool": args.pool, "base_mint": args.base_mint,
        "quote_mint": args.quote_mint, "gamma": args.gamma, "kappa": args.kappa,
        "capital": args.capital, "levels": args.levels, "duration_seconds": args.duration_seconds,
        "regime_stop": not args.no_regime_stop,
        "oracle": args.oracle_pool or args.oracle_cross,
        "wallet_pubkey": os.environ.get("WALLET_PUBKEY", ""),
        "max_sol_per_run": os.environ.get("MAX_SOL_PER_RUN", ""),
        "max_sol_per_tx": os.environ.get("MAX_SOL_PER_TX", ""),
        "resume_position_id": args.position_id,
        "run_dir": str(run_dir),
    }
    logger.info("Plan: %s", json.dumps(plan))
    (run_dir / "plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    bridge = ExecBridge(cmd=["node", "dist/bridge.js"], cwd=args.executor_cwd)
    bridge.start()
    keeper = build_keeper(args, bridge, run_dir)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)
    try:
        stop_reason = loop.run_until_complete(run_soak(keeper, args.duration_seconds, stop_event))
    finally:
        loop.close()
        close_bridge(bridge)

    result = {
        "run_dir": str(run_dir), "stop_reason": stop_reason,
        "event_log": keeper.event_log_path,
        "final_position_id": keeper._current_position_id,
        "final_extra_position_ids": list(keeper._extra_position_ids),
    }
    print(json.dumps(result, indent=2))
    # A position surviving clean-stop means the withdrawal itself failed
    # on-chain (see the exception log above): fail loud, don't report success.
    return 0 if not (result["final_position_id"] or result["final_extra_position_ids"]) else 1


if __name__ == "__main__":
    sys.exit(main())
