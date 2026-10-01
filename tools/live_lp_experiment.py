#!/usr/bin/env python
"""Bounded live SOL/USDC LP experiment, not a profitability rollout.

Two fixed, five-bin, one-sided positions; no token swaps, refreshes or perp
hedge. Actual position amounts and fees replace inferred crossing fills.
Default is a brief unsigned preflight. Signing additionally requires --live,
LIVE_WRITE_CONFIRM=yes and DRY_RUN=false. Timeout/ambiguous submission stops
all writes: known deterministic PDAs remain in the durable log for recovery.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
from threading import Event
import time

from mm_core.pnl import PnLLedger
from dlmm_bot.event_log import EventLog
from dlmm_bot.exec_bridge import ExecBridge

sys.path.insert(0, str(Path(__file__).parent))
from live_keeper_soak import close_bridge, grid_from_reads, SECRET_ENV
from live_keeper_write_soak import GATEWAY_ENV_KEYS

EXECUTOR = Path(__file__).resolve().parents[2] / "solana-clmm-executor"


class OpeningDrift(ValueError):
    """The executor rejected a deposit before signing; a fresh plan is safe."""


def build_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pool", required=True)
    p.add_argument("--base-mint", required=True)
    p.add_argument("--quote-mint", required=True)
    p.add_argument("--wallet", required=True)
    p.add_argument("--capital", type=float, default=10)
    p.add_argument("--loss-limit", type=float, default=1)
    p.add_argument("--fee-budget-sol", type=float, default=0.01)
    p.add_argument("--duration-seconds", type=float, default=7200)
    p.add_argument("--refresh-interval", type=float, default=30)
    p.add_argument("--width", type=int, default=5)
    p.add_argument("--live", action="store_true")
    p.add_argument("--out", required=True)
    args = p.parse_args(argv)
    for name, maximum in (("capital", 32), ("loss_limit", args.capital),
                          ("fee_budget_sol", .01), ("duration_seconds", 7200),
                          ("refresh_interval", 30), ("width", 20)):
        value = getattr(args, name)
        if not math.isfinite(value) or not 0 < value <= maximum:
            p.error(f"{name} must be finite and in (0, {maximum}]")
    return args


def validate_environment(args):
    for key, value in os.environ.items():
        if value and key.startswith(("SOLANA_", "WALLET_", "KMS_")) and SECRET_ENV.search(key):
            raise ValueError(f"secret-bearing environment variable forbidden: {key}")
    if not os.environ.get("SOLANA_RPC_URL") or os.environ.get("WALLET_PUBKEY") != args.wallet:
        raise ValueError("RPC and an exact WALLET_PUBKEY pin are required")
    if not args.live:
        return
    if os.environ.get("LIVE_WRITE_CONFIRM") != "yes" or os.environ.get("DRY_RUN") != "false":
        raise ValueError("--live requires LIVE_WRITE_CONFIRM=yes and DRY_RUN=false")
    for key, maximum in (("MAX_SOL_PER_TX", .23), ("MAX_SOL_PER_RUN", .25),
                         ("MAX_SLIPPAGE_BPS", 25), ("MAX_PRIORITY_FEE_LAMPORTS", 10000)):
        value = float(os.environ.get(key, "nan"))
        if not math.isfinite(value) or not 0 <= value <= maximum:
            raise ValueError(f"{key} must be finite and <= {maximum}")
    if os.environ.get("MAX_ACTIVE_BIN_SLIPPAGE_BINS") != "0":
        raise ValueError("experiment requires zero active-bin slippage")
    if os.environ.get("JITO_ENABLED", "false") != "false":
        raise ValueError("experiment requires JITO_ENABLED=false")


def inspect_accounts(args, legs):
    request = {"pool": args.pool, "wallet": args.wallet,
               "base_mint": args.base_mint, "quote_mint": args.quote_mint, "legs": legs}
    result = subprocess.run(
        ["node", "tools/live-experiment-accounts.mjs"], cwd=EXECUTOR,
        input=json.dumps(request), capture_output=True, text=True, timeout=120,
    )
    if result.returncode:
        raise ValueError(result.stderr.strip() or "account preflight failed")
    return json.loads(result.stdout)


def run_experiment(bridge, args, run_dir, stop, accounts=inspect_accounts):
    ledger = PnLLedger("meteora", "SOL/USDC-live-experiment")
    log = EventLog(str(run_dir / "run.jsonl"), config_hash=hashlib.sha256(
        json.dumps(vars(args), sort_keys=True).encode()).hexdigest())
    opened, proposed, legs = [], [], []
    checked_legs = []
    baseline = grid = None
    initial_base = initial_quote = mid0 = mid = 0.0
    targets = {"lp_inventory_mark": 0.0, "lp_fee": 0.0}
    claimed = {"base": 0, "quote": 0}
    fees = 0
    signed = False
    deployed = False
    unknown = False
    preflight_pass = False
    cleanup_complete = False
    reason, error = "preflight_failed", None

    def state():
        result = bridge.get_state(args.pool)
        log.emit("state_observation", ok=result.ok, state=result.data, error=result.error)
        if not result.ok or not result.data:
            raise ValueError("pool state unavailable")
        data = result.data
        if (data["token_x"]["mint"], data["token_y"]["mint"]) != (args.base_mint, args.quote_mint):
            raise ValueError("pool mint pair mismatch")
        if not 0 <= time.time() - float(data["fetched_at"]) <= 30:
            raise ValueError("pool state is stale or from the future")
        return data

    def action(verb, payload, pid):
        nonlocal fees, unknown, signed
        log.emit("action_request", verb=verb, payload=payload, expected_position_id=pid)
        # Persist deterministic recovery handles before the signer sees a request.
        with open(log.path, "rb") as durable:
            os.fsync(durable.fileno())
        try:
            result = getattr(bridge, verb)(**payload)
        except Exception:
            unknown = True
            raise ValueError("executor transport failed; outcome unknown")
        log.emit("action_result", verb=verb, **asdict(result))
        signed |= bool(result.tx_signatures)
        unsubmitted = {"policy_rejected", "active_bin_slippage_exceeded", "bins_cross_active",
                       "insufficient_balance", "bad_request", "slippage_exceeded",
                       "simulation_failed", "unknown_position"}
        unknown = bool(unknown or result.unknown_outcome or (not result.ok and (
            result.error not in unsubmitted or result.tx_signatures or result.tx_receipts
            or result.total_fee_lamports is not None
            or (result.data or {}).get("pending_signature"))))
        if result.total_fee_lamports is not None:
            fees += result.total_fee_lamports
            ledger.on_cash_flow("rebalance", time.time(),
                                -result.total_fee_lamports / 1e9 * mid, label=verb)
        if not result.ok:
            if verb == "deposit_single_sided" and not unknown:
                opened.remove(pid)  # proven pre-submission rejection: never adopt a later foreign position
                if result.error == "active_bin_slippage_exceeded":
                    raise OpeningDrift("deposit_single_sided failed: active_bin_slippage_exceeded")
            raise ValueError(f"{verb} failed: {result.error}")
        if (result.position_id or (result.data or {}).get("position_id")) != pid:
            unknown = True
            raise ValueError("executor returned an unexpected position address")
        if not result.tx_signatures or result.total_fee_lamports is None:
            raise ValueError("confirmed action has incomplete receipt evidence")
        return result

    def snapshot():
        nonlocal mid
        data = state()
        mid = grid.price_from_bin(int(data["active_bin"]))
        raw = {k: int(data["balances_raw"][k]) - int(baseline[k]) for k in claimed}
        pending = {"base": 0, "quote": 0}
        for pid in opened:
            result = bridge.get_position(pid)
            log.emit("position_observation", **(asdict(result) | {"position_id": pid}))
            pos = result.data or {}
            if not result.ok or (pos.get("owner"), pos.get("pool")) != (args.wallet, args.pool):
                raise ValueError("owned position read failed or identity mismatch")
            for row in pos["bins"]:
                for token in raw:
                    raw[token] += int(row[f"amount_{token}_raw"])
            pending["base"] += int(pos["claimable_fee_x_raw"])
            pending["quote"] += int(pos["claimable_fee_y_raw"])
        if time.time() - float(data["fetched_at"]) > 30:
            raise ValueError("combined custody snapshot took more than 30 seconds")
        # Wallet deltas + all live positions: partial in-bin conversions are real,
        # not manufactured fills. Claimed fees have moved into the wallet already.
        principal_base = initial_base + (raw["base"] - claimed["base"]) / 10**grid.base_decimals
        principal_quote = initial_quote + (raw["quote"] - claimed["quote"]) / 10**grid.quote_decimals
        if principal_base < -1e-9 or principal_quote < -1e-6:
            raise ValueError("wallet custody changed outside the experiment")
        fee_base = (pending["base"] + claimed["base"]) / 10**grid.base_decimals
        fee_quote = (pending["quote"] + claimed["quote"]) / 10**grid.quote_decimals
        values = {"lp_inventory_mark": principal_base * mid + principal_quote
                  - (initial_base * mid0 + initial_quote),
                  "lp_fee": fee_base * mid + fee_quote}
        for channel, target in values.items():
            ledger.on_cash_flow(channel, time.time(), target - targets[channel], label="custody_mark")
            targets[channel] = target
        ledger.mark(time.time(), mid)
        pnl = ledger.explain(include_events=False).to_dict()
        log.emit("custody_snapshot", principal_base=principal_base, principal_quote=principal_quote,
                 claimable_fee_base=fee_base, claimable_fee_quote=fee_quote,
                 idle_hold_pnl=initial_base * (mid - mid0), pnl=pnl,
                 accounting="shared PnLLedger custody marks, not fill-level attribution")
        return pnl

    try:
        data = state()
        grid = grid_from_reads(data)
        mid = mid0 = grid.price_from_bin(int(data["active_bin"]))
        ledger.mark(time.time(), mid)
        initial_quote = args.capital / 2
        initial_base = math.floor(initial_quote / mid * 10**grid.base_decimals) / 10**grid.base_decimals
        baseline = data["balances_raw"].copy()
        if (int(baseline["base"]) < round(initial_base * 10**grid.base_decimals)
                or int(baseline["quote"]) < round(initial_quote * 10**grid.quote_decimals)):
            raise ValueError("insufficient pre-funded base/quote; experiment never swaps to fund itself")
        active = int(data["active_bin"])
        legs = [{"side": side, "bin_ids": list(range(lo, lo + args.width)),
                 "amounts": [amount / args.width] * args.width}
                for side, lo, amount in (("bid", active - args.width, initial_quote),
                                        ("ask", active + 1, initial_base))]
        inspected = accounts(args, legs)
        proposed = [p["position_id"] for p in inspected["positions"]]
        log.emit("experiment_plan", args=vars(args), legs=legs,
                 caps={k: os.environ[k] for k in GATEWAY_ENV_KEYS
                       if k.startswith("MAX_") and k in os.environ}, **inspected)
        if (len(proposed) != 2 or len(set(proposed)) != 2
                or [p["side"] for p in inspected["positions"]] != [l["side"] for l in legs]
                or any(p["exists"] for p in inspected["positions"])):
            raise ValueError("deposit PDA already exists or preflight is incomplete; never adopt it")
        checked_legs = [dict(leg) for leg in legs]
        rent_sol = inspected["rent_lamports"] / 1e9
        if inspected["native_lamports"] / 1e9 < rent_sol + args.fee_budget_sol:
            raise ValueError("native SOL cannot cover temporary rent plus fee reserve")
        if args.live and rent_sol + args.fee_budget_sol > float(os.environ["MAX_SOL_PER_RUN"]):
            raise ValueError("run cap cannot fund both opening and closing")
        preflight_pass = True
        if not args.live:
            reason, cleanup_complete = "preflight_only", True
        else:
            deadline = time.monotonic() + args.duration_seconds
            for index, template in enumerate(legs):
                pid = inspected["positions"][index]["position_id"]
                leg = dict(template)
                for attempt in range(1, 4):
                    if stop.is_set() or time.monotonic() >= deadline:
                        raise ValueError("stopped before both positions opened")
                    if fees / 1e9 + .000045 >= args.fee_budget_sol:
                        raise ValueError("fee budget cannot cover another deposit plus both closes")
                    fresh = state()  # never reuse the pool read from before account/rent preflight
                    active = int(fresh["active_bin"])
                    mid = grid.price_from_bin(active)
                    if not deployed:
                        if fresh["balances_raw"] != baseline:
                            raise ValueError("wallet custody changed before opening")
                        mid0 = mid
                        initial_base = math.floor(initial_quote / mid * 10**grid.base_decimals) / 10**grid.base_decimals
                        if int(baseline["base"]) < round(initial_base * 10**grid.base_decimals):
                            raise ValueError("pre-funded base cannot cover the fresh opening price")
                        legs[1]["amounts"] = [initial_base / args.width] * args.width
                    lo = active - args.width if leg["side"] == "bid" else active + 1
                    bins = list(range(lo, lo + args.width))
                    if bins != leg["bin_ids"]:
                        leg = dict(leg, bin_ids=bins)
                        candidate = accounts(args, [leg])
                        log.emit("opening_plan", attempt=attempt, expected_active_bin=active,
                                 leg=leg, **candidate)
                        positions = candidate["positions"]
                        if (len(positions) != 1 or positions[0]["side"] != leg["side"]
                                or positions[0]["exists"]):
                            raise ValueError("fresh deposit PDA already exists or account preflight is incomplete")
                        if candidate["native_lamports"] / 1e9 < candidate["rent_lamports"] / 1e9 + args.fee_budget_sol:
                            raise ValueError("fresh deposit cannot retain rent and closing fee reserve")
                        pid = positions[0]["position_id"]
                        checked_legs.append(dict(leg))
                        if pid not in proposed:
                            proposed.append(pid)
                    # Persist each fresh recovery handle before a request reaches the signer.
                    opened.append(pid)
                    try:
                        action("deposit_single_sided", dict(pool=args.pool, **leg,
                               expected_active_bin=active, max_active_bin_slippage=0), pid)
                        deployed = True
                        break
                    except OpeningDrift:
                        log.emit("opening_retry", side=leg["side"], attempt=attempt,
                                 position_id=pid, reason="unsigned active-bin rejection")
                        if attempt == 3:
                            raise
            reason = "duration_elapsed"
            while True:
                pnl = snapshot()
                if pnl["total_pnl"] <= -args.loss_limit:
                    reason = "loss_limit"
                    break
                if fees / 1e9 + .00003 >= args.fee_budget_sol:
                    reason = "fee_budget"
                    break
                if stop.is_set():
                    reason = "signal"
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                stop.wait(min(args.refresh_interval, remaining))
    except Exception as exc:
        error = str(exc)
        if preflight_pass:
            reason = ("unresolved_outcome" if unknown else "opening_rejected"
                      if isinstance(exc, OpeningDrift) else "unsafe_read_or_action")
        log.emit("experiment_error", error=error, unresolved_outcome=unknown)
    finally:
        if args.live and preflight_pass and not unknown:
            for pid in list(opened):
                try:
                    before = bridge.get_position(pid)
                    log.emit("position_observation", **(asdict(before) | {"position_id": pid}))
                    if not before.ok and before.error == "unknown_position":
                        opened.remove(pid)  # rejected deposit; exact preflight PDA remains absent
                        continue
                    if not before.ok or not before.data or (before.data.get("owner"), before.data.get("pool")) != (args.wallet, args.pool):
                        raise ValueError("cleanup cannot verify owned position")
                    result = action("withdraw", {"position_id": pid, "percent": 100}, pid)
                    if not (result.data or {}).get("closed"):
                        raise ValueError("withdrawal did not confirm closure")
                    claim = result.data["fees_claimed"]
                    claimed["base"] += int(claim["x_raw"])
                    claimed["quote"] += int(claim["y_raw"])
                    opened.remove(pid)
                except Exception as exc:
                    error = str(exc)
                    log.emit("cleanup_error", position_id=pid, error=error, unresolved_outcome=unknown)
                    if unknown:
                        break  # do not restart the executor or blindly resubmit
            if not opened and not unknown:
                try:
                    final_accounts = accounts(args, checked_legs)
                    if any(p["exists"] for p in final_accounts["positions"]):
                        raise ValueError("position account still exists after closure")
                    if deployed:
                        snapshot()
                    native_delta = final_accounts["native_lamports"] - inspected["native_lamports"]
                    log.emit("native_reconciliation", delta_lamports=native_delta,
                             receipt_fee_lamports=fees, unexplained_lamports=native_delta + fees)
                    if native_delta + fees != 0:
                        raise ValueError("native rent/receipt balance does not reconcile")
                    cleanup_complete = True
                except Exception as exc:
                    error = str(exc)
                    log.emit("cleanup_error", error=error)
        report = {"preflight_pass": preflight_pass, "live_started": bool(signed or unknown),
                  "stop_reason": reason, "cleanup_complete": cleanup_complete,
                  "unresolved_outcome": unknown, "remaining_position_ids": opened,
                  "expected_position_ids": proposed, "fee_lamports": fees, "error": error,
                  "inventory_mark_pnl": targets["lp_inventory_mark"],
                  "idle_hold_pnl": initial_base * (mid - mid0) if deployed else 0.0,
                  "pnl": ledger.explain(mid=mid, include_events=False).to_dict() if mid > 0 else None,
                  "fee_attribution": "position entitlement; post-read withdrawal accrual may enter principal residual"}
        log.emit("run_stopped", **report)
        log.close()
    return report


def main(argv=None):
    args = build_args(argv)
    validate_environment(args)
    run_dir = Path(args.out).resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    inherited = {k: v for k, v in os.environ.items()
                 if k in ("PATH", "HOME", "LANG", "TZ") or k in GATEWAY_ENV_KEYS}
    os.environ.clear()
    os.environ.update(inherited)
    os.environ.update({"DRY_RUN": "false" if args.live else "true",
                       "POOL_ALLOWLIST": args.pool,
                       "MINT_ALLOWLIST": f"{args.base_mint},{args.quote_mint}",
                       "SWAP_STREAM_PATH": str(run_dir / "swaps.jsonl"),
                       "EXECUTOR_LOG_DIR": str(run_dir / "executor")})
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    bridge = ExecBridge(["node", "dist/bridge.js"], cwd=str(EXECUTOR), timeout=120)
    try:
        report = run_experiment(bridge, args, run_dir, stop)
    finally:
        close_bridge(bridge)
    (run_dir / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"run_dir": str(run_dir), **report}, indent=2))
    return 0 if report["preflight_pass"] and report["cleanup_complete"] and not report["error"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
