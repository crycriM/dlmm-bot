#!/usr/bin/env python
"""Close explicitly named Meteora positions after verifying wallet and pool ownership."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path

from dlmm_bot.event_log import EventLog
from dlmm_bot.exec_bridge import ExecBridge

from live_keeper_soak import close_bridge
from live_keeper_write_soak import GATEWAY_ENV_KEYS
from live_lp_experiment import EXECUTOR, validate_environment


def recover(bridge, args, run_dir):
    log = EventLog(str(run_dir / "run.jsonl"))
    remaining = list(args.position_ids)
    closed = []
    fees = 0
    unknown = False
    error = None
    try:
        candidates = []
        for pid in list(remaining):
            result = bridge.get_position(pid)
            log.emit("position_observation", **(asdict(result) | {"position_id": pid}))
            if not result.ok and result.error == "unknown_position":
                remaining.remove(pid)
                continue
            if not result.ok or not result.data or (
                result.data.get("owner"), result.data.get("pool")
            ) != (args.wallet, args.pool):
                raise ValueError(f"cannot verify owned pool position {pid}")
            candidates.append(pid)
        log.emit("recovery_plan", position_ids=candidates, wallet=args.wallet, pool=args.pool)
        for pid in candidates:
            log.emit("withdraw_request", position_id=pid, percent=100)
            with open(log.path, "rb") as durable:
                os.fsync(durable.fileno())
            try:
                result = bridge.withdraw(pid, 100)
            except Exception:
                unknown = True
                raise ValueError(f"withdraw transport failed for {pid}; outcome unknown")
            log.emit("withdraw_result", **(asdict(result) | {"position_id": pid}))
            if not result.ok:
                unknown = bool(result.unknown_outcome or result.tx_signatures
                               or result.tx_receipts or result.total_fee_lamports is not None)
                raise ValueError(f"withdraw failed for {pid}: {result.error}")
            if (result.position_id or (result.data or {}).get("position_id")) != pid:
                unknown = True
                raise ValueError(f"withdraw returned unexpected position for {pid}")
            if not (result.data or {}).get("closed") or not result.tx_signatures or result.total_fee_lamports is None:
                unknown = True
                raise ValueError(f"withdraw receipt incomplete for {pid}")
            try:
                after = bridge.get_position(pid)
            except Exception:
                unknown = True
                raise ValueError(f"position closure read failed for {pid}; outcome unknown")
            log.emit("position_observation", **(asdict(after) | {"position_id": pid}))
            if after.ok or after.error != "unknown_position":
                unknown = True
                raise ValueError(f"position closure cannot be verified for {pid}")
            fees += result.total_fee_lamports
            remaining.remove(pid)
            closed.append(pid)
    except Exception as exc:
        error = str(exc)
        log.emit("recovery_error", error=error, unresolved_outcome=unknown)
    report = {"closed_position_ids": closed, "remaining_position_ids": remaining,
              "cleanup_complete": not remaining and not unknown and error is None,
              "unresolved_outcome": unknown, "fee_lamports": fees, "error": error}
    log.emit("recovery_stopped", **report)
    log.close()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool", required=True)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--base-mint", required=True)
    parser.add_argument("--quote-mint", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("position_ids", nargs="+")
    args = parser.parse_args(argv)
    if len(args.position_ids) != len(set(args.position_ids)):
        parser.error("position ids must be distinct")
    args.live = True
    validate_environment(args)
    run_dir = Path(args.out).resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    inherited = {k: v for k, v in os.environ.items()
                 if k in ("PATH", "HOME", "LANG", "TZ") or k in GATEWAY_ENV_KEYS}
    os.environ.clear()
    os.environ.update(inherited)
    os.environ.update({"DRY_RUN": "false", "POOL_ALLOWLIST": args.pool,
                       "MINT_ALLOWLIST": f"{args.base_mint},{args.quote_mint}",
                       "SWAP_STREAM_PATH": str(run_dir / "swaps.jsonl"),
                       "EXECUTOR_LOG_DIR": str(run_dir / "executor")})
    bridge = ExecBridge(["node", "dist/bridge.js"], cwd=str(EXECUTOR), timeout=120)
    try:
        report = recover(bridge, args, run_dir)
    finally:
        close_bridge(bridge)
    (run_dir / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"run_dir": str(run_dir), **report}, indent=2))
    return 0 if report["cleanup_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
