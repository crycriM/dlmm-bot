"""Read-only M2 keeper soak against the sibling Solana executor.

Source solana-clmm-executor/.env.m3 into the shell first. This launcher never
loads a signer, forces both keeper and executor dry-run, and rejects secret
environment variables. It is an observation gate, not strategy calibration.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from dlmm_bot.config import DLMMConfig
from dlmm_bot.event_log import ReplayLog
from dlmm_bot.exec_bridge import ExecBridge
from dlmm_bot.grid import VenueGrid
from dlmm_bot.keeper import Keeper, KeeperConfig


EXECUTOR = Path(__file__).resolve().parents[2] / "solana-clmm-executor"
SECRET_ENV = re.compile(r"PRIVATE_KEY|WALLET_SECRET(?!_ARN)|MNEMONIC|SEED|KMS_PLAINTEXT", re.I)
READ_METHODS = {"get_state", "get_position"}
MAX_STATE_P95_MS = 2_000
MINIMUM_SOAK_SECONDS = 30 * 60


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def validate_read_only() -> None:
    if os.environ.get("RUN_LIVE") != "1":
        raise ValueError("RUN_LIVE=1 is required")
    if os.environ.get("DRY_RUN", "true").lower() != "true":
        raise ValueError("DRY_RUN must be true")
    if os.environ.get("LIVE_WRITE_CONFIRM"):
        raise ValueError("LIVE_WRITE_CONFIRM must be unset")
    for key, value in os.environ.items():
        if (value and key.startswith(("SOLANA_", "WALLET_", "KMS_"))
                and SECRET_ENV.search(key)):
            raise ValueError(f"secret-bearing environment variable is forbidden: {key}")
    for name in (
        "SOLANA_RPC_URL", "LIVE_POOL", "LIVE_POSITION_ID", "WALLET_PUBKEY",
        "LIVE_BASE_MINT", "LIVE_QUOTE_MINT",
    ):
        required(name)
    if not (EXECUTOR / "dist" / "bridge.js").is_file():
        raise ValueError("executor dist/bridge.js is missing; run npm run build")


def configure_executor(output: Path) -> None:
    """Set only read-only production bridge settings; retain RPC/WS from .env.m3."""
    allowed = (
        "PATH", "HOME", "LANG", "TZ", "RUN_LIVE", "SOLANA_RPC_URL",
        "SOLANA_RPC_WRITE_URL", "SOLANA_WS_URL", "SOLANA_COMMITMENT",
        "SOLANA_RPC_MAX_CU_PER_SECOND", "LIVE_POOL", "LIVE_POSITION_ID",
        "LIVE_BASE_MINT", "LIVE_QUOTE_MINT", "WALLET_PUBKEY",
    )
    inherited = {key: os.environ[key] for key in allowed if key in os.environ}
    os.environ.clear()  # ExecBridge inherits env; never forward unrelated credentials.
    os.environ.update(inherited)
    os.environ.update({
        "DRY_RUN": "true",
        "WALLET_SIGNER": "kms",
        "POOL_ALLOWLIST": required("LIVE_POOL"),
        "MINT_ALLOWLIST": ",".join((required("LIVE_BASE_MINT"), required("LIVE_QUOTE_MINT"))),
        "MAX_SOL_PER_TX": "0",
        "MAX_SOL_PER_RUN": "0",
        "MAX_SLIPPAGE_BPS": "0",
        "MAX_ACTIVE_BIN_SLIPPAGE_BINS": "0",
        "MAX_PRIORITY_FEE_LAMPORTS": "0",
        "JITO_ENABLED": "false",
        "JITO_TIP_LAMPORTS": "0",
        "SWAP_STREAM_PATH": str(output / "swaps.jsonl"),
        "EXECUTOR_LOG_DIR": str(output / "executor"),
    })


def grid_from_reads(state: dict, position: dict) -> VenueGrid:
    step = int(state["bin_step_bps"])
    if step <= 0:
        raise ValueError("live pool bin step must be positive")
    active = int(state["active_bin"])
    priced = [
        row for row in position.get("bins", [])
        if isinstance(row, dict) and float(row.get("bin_price") or 0) > 0
    ]
    if not priced:
        raise ValueError("owned position has no priced bins for grid calibration")
    nearest = min(priced, key=lambda row: abs(int(row["bin_id"]) - active))
    reference = float(nearest["bin_price"]) / math.exp(
        int(nearest["bin_id"]) * math.log1p(step / 10_000)
    )
    if not math.isfinite(reference) or reference <= 0:
        raise ValueError("derived grid reference price is invalid")
    return VenueGrid(
        ref_price=reference,
        bin_step_bps=step,
        base_decimals=int(state["token_x"]["decimals"]),
        quote_decimals=int(state["token_y"]["decimals"]),
    )


def close_bridge(bridge: ExecBridge) -> None:
    """Let the executor flush its JSONL before falling back to termination."""
    proc = bridge._proc
    if proc is None:
        return
    try:
        if proc.stdin and not proc.stdin.closed:
            proc.stdin.close()
        proc.wait(timeout=15)
    except Exception:
        bridge.stop()
        return
    for stream in (proc.stdout, proc.stderr):
        if stream:
            stream.close()
    bridge._proc = None


def executor_lines(directory: Path) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(directory.glob("*.jsonl")):
        with path.open(encoding="utf-8") as source:
            rows.extend(json.loads(line) for line in source if line.strip())
    return rows


def summarize(output: Path, elapsed: float, minimum_seconds: float) -> dict:
    minimum_seconds = max(minimum_seconds, MINIMUM_SOAK_SECONDS)
    keeper_paths = list((output / "keeper").glob("*.jsonl"))
    if len(keeper_paths) != 1:
        raise ValueError("expected exactly one keeper event log")
    events = ReplayLog(str(keeper_paths[0])).events()
    audit = executor_lines(output / "executor")
    state_events = [row for row in events if row["event_type"] == "state_observation"]
    position_events = [row for row in events if row["event_type"] == "position_observation"]
    verbs = [row for row in audit if row.get("kind") == "verb"]
    durations = sorted(
        float(row["duration_ms"]) for row in verbs if row.get("method") == "get_state"
    )
    p95 = durations[math.ceil(len(durations) * 0.95) - 1] if durations else None
    started = next((row for row in audit if row.get("kind") == "executor_started"), {})
    no_writes = all(row.get("method") in READ_METHODS for row in verbs)
    complete = (
        elapsed >= minimum_seconds
        and bool(state_events)
        and len(position_events) == len(state_events)
        and all(row.get("state") is not None for row in state_events)
        and all(
            row.get("ok") is True
            and isinstance(row.get("claimable_fee_x_raw"), str)
            and isinstance(row.get("claimable_fee_y_raw"), str)
            for row in position_events
        )
        and all(row.get("response", {}).get("ok") is True for row in verbs)
        and all(row.get("action") == "observation_only"
                for row in events if row["event_type"] == "decision")
        and no_writes
        and started.get("dry_run") is True
        and p95 is not None
        and p95 < MAX_STATE_P95_MS
    )
    return {
        "elapsed_seconds": round(elapsed, 3),
        "minimum_seconds": minimum_seconds,
        "state_observations": len(state_events),
        "position_observations": len(position_events),
        "executor_reads": len(verbs),
        "failed_executor_reads": sum(row.get("response", {}).get("ok") is not True for row in verbs),
        "get_state_p95_ms": p95,
        "max_get_state_p95_ms": MAX_STATE_P95_MS,
        "read_only": no_writes and started.get("dry_run") is True,
        "gate_pass": complete,
        "keeper_log": str(keeper_paths[0]),
    }


async def run_keeper(keeper: Keeper, duration: float) -> None:
    try:
        await asyncio.wait_for(keeper.run(), timeout=duration)
    except TimeoutError:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=float, default=1800)
    parser.add_argument("--interval-seconds", type=float, default=10)
    parser.add_argument("--revalidate-existing", type=Path,
                        help="reassess retained evidence without a new live run")
    args = parser.parse_args()
    if args.revalidate_existing is not None:
        output = args.revalidate_existing.resolve()
        previous = json.loads((output / "summary.json").read_text(encoding="utf-8"))
        summary = summarize(output, float(previous["elapsed_seconds"]),
                            float(previous["minimum_seconds"]))
        target = output / f"gate-validation-{MAX_STATE_P95_MS}ms.json"
        with target.open("x", encoding="utf-8") as artifact:
            json.dump({"source_summary": str(output / "summary.json"), **summary},
                      artifact, indent=2)
            artifact.write("\n")
        print(json.dumps({"artifact": str(target), **summary}))
        return 0 if summary["gate_pass"] else 1
    if args.duration_seconds <= 0 or args.interval_seconds <= 0:
        parser.error("duration and interval must be positive")
    validate_read_only()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = EXECUTOR / "logs" / "test-artifacts" / f"evidence-keeper-m2-{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    configure_executor(output)
    bridge = ExecBridge(["node", "dist/bridge.js"], cwd=str(EXECUTOR))
    keeper: Keeper | None = None
    started_at = 0.0
    try:
        state = bridge.get_state(required("LIVE_POOL"))
        position = bridge.get_position(required("LIVE_POSITION_ID"))
        if not state.ok or not isinstance(state.data, dict):
            raise RuntimeError("live get_state preflight failed")
        if not position.ok or not isinstance(position.data, dict):
            raise RuntimeError("live get_position preflight failed")
        grid = grid_from_reads(state.data, position.data)
        cfg = KeeperConfig(
            dlmm=DLMMConfig(
                gamma=0.0, kappa=0.0, bin_step_bps=grid.bin_step_bps,
                ref_price=grid.ref_price, levels=0, capital=0.0,
            ),
            grid=grid,
            refresh_interval=args.interval_seconds,
            pool_address=required("LIVE_POOL"),
            position_id=required("LIVE_POSITION_ID"),
            base_mint=required("LIVE_BASE_MINT"),
            quote_mint=required("LIVE_QUOTE_MINT"),
            dry_run=True,
            observation_only=True,
            log_dir=str(output / "keeper"),
            executor_version="solana-clmm-executor/observation-only",
        )
        keeper = Keeper(cfg, bridge)
        started_at = time.monotonic()
        asyncio.run(run_keeper(keeper, args.duration_seconds))
    finally:
        if keeper is not None:
            keeper.stop()
        close_bridge(bridge)
    elapsed = time.monotonic() - started_at
    summary = summarize(output, elapsed, args.duration_seconds)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"artifact_dir": str(output), **summary}))
    return 0 if summary["gate_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
