#!/usr/bin/env python
"""Bounded live Meteora DLMM LP experiment, not a profitability rollout.

Two initially one-sided positions; optional no-swap, one-sided range shifts.
Actual position amounts and fees replace inferred crossing fills.
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


# Bins the active bin may move between plan and execution. Zero rejected 2 of 3
# bid attempts in a fast hour (2026-10-02); one bin (4 bps here) only lets a
# move *away* from the range through, a move into it is still bins_cross_active.
ACTIVE_BIN_TOLERANCE = 1
# What the executor's run cap (MAX_SOL_PER_RUN) counts per verb, mirrored so a
# shift is skipped rather than rejected: deposit = new position rent + one
# token-account allowance + signature; withdraw = two allowances + signature.
TOKEN_ACCOUNT_LAMPORTS = 2_039_280
SIGNATURE_LAMPORTS = 5_000
WITHDRAW_CHARGE = 2 * TOKEN_ACCOUNT_LAMPORTS + SIGNATURE_LAMPORTS
# Unsigned, deterministic rejections caused by the price moving: re-plan.
DRIFT_ERRORS = ("active_bin_slippage_exceeded", "bins_cross_active")
WSOL = "So11111111111111111111111111111111111111112"


def sol_in_quote(args, mid: float) -> float:
    """Quote units per SOL, to book receipt lamports in the ledger's currency.

    ponytail: a non-SOL quote is taken as USD-pegged (USDC/USD1) and SOL is priced at
    the static --sol-usd launch value; gas is ~$0.005 a run, so its drift is noise.
    """
    if args.base_mint == WSOL:
        return mid
    if args.quote_mint == WSOL:
        return 1.0
    return args.sol_usd


def shift_status(active, ranges, last_adjustment_at, now, cooldown_seconds):
    lo = min(r[0] for r in ranges.values())
    hi = max(r[1] for r in ranges.values())
    return (max(lo - active, active - hi, 0),
            max(cooldown_seconds - (now - last_adjustment_at), 0.0))


def per_bin(amount: float, decimals: int, width: int) -> list[float]:
    """Equal per-bin amounts in whole raw token units.

    The executor rejects a deposit whose total is not an exact number of raw
    units (bad_request); floor like Keeper._quantized_side, never over budget.
    """
    return [math.floor(amount * 10**decimals / width) / 10**decimals] * width


# SIGHUP is what a dropped SSH session sends. Default action kills Python with no
# cleanup (live 2026-10-02: both positions left open for 3 h 20 min), so it must
# take the same graceful path as Ctrl-C: withdraw everything this run opened.
STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def install_stop_signals(stop) -> None:
    for sig in STOP_SIGNALS:
        signal.signal(sig, lambda *_: stop.set())


class ActionFailed(ValueError):
    """An executor verb answered ok=false; carries the result for triage."""

    def __init__(self, message: str, result):
        super().__init__(message)
        self.result = result


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
    p.add_argument("--shift-gap", type=int, default=0,
                   help="bins beyond the combined outer edge before liquidity shifts "
                        "one-sided next to the price; 0 = fixed ranges")
    p.add_argument("--shift-cooldown-seconds", type=float, default=0,
                   help="minimum time since opening or the last completed shift")
    p.add_argument("--sol-usd", type=float, default=None,
                   help="USD per SOL at launch; required unless the base is wSOL "
                        "(prices receipt gas, and the $32 cap when the quote is wSOL)")
    p.add_argument("--live", action="store_true")
    p.add_argument("--out", required=True)
    args = p.parse_args(argv)
    if args.base_mint != WSOL and not (args.sol_usd and math.isfinite(args.sol_usd) and args.sol_usd > 0):
        p.error("--sol-usd must be a positive price unless the base token is wSOL")
    # Capital and loss limit are quote units; the $32 cap is not.
    quote_usd = args.sol_usd if args.quote_mint == WSOL else 1.0
    for name, maximum in (("capital", 32 / quote_usd), ("loss_limit", args.capital),
                          ("fee_budget_sol", .01), ("duration_seconds", 7200),
                          ("refresh_interval", 30), ("width", 20)):
        value = getattr(args, name)
        if not math.isfinite(value) or not 0 < value <= maximum:
            p.error(f"{name} must be finite and in (0, {maximum}]")
    if not 0 <= args.shift_gap <= 200:
        p.error("shift_gap must be in [0, 200]")
    if not math.isfinite(args.shift_cooldown_seconds) or not 0 <= args.shift_cooldown_seconds <= 7200:
        p.error("shift_cooldown_seconds must be finite and in [0, 7200]")
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
    for key, maximum in (("MAX_SOL_PER_TX", .23), ("MAX_SOL_PER_RUN", .5),
                         ("MAX_SLIPPAGE_BPS", 25), ("MAX_PRIORITY_FEE_LAMPORTS", 50000)):
        value = float(os.environ.get(key, "nan"))
        if not math.isfinite(value) or not 0 <= value <= maximum:
            raise ValueError(f"{key} must be finite and <= {maximum}")
    if os.environ.get("MAX_ACTIVE_BIN_SLIPPAGE_BINS") != str(ACTIVE_BIN_TOLERANCE):
        raise ValueError(f"experiment requires active-bin slippage cap {ACTIVE_BIN_TOLERANCE}")
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


def inspect_stream(pool):
    result = subprocess.run(
        ["node", "tools/live-stream-preflight.mjs", pool], cwd=EXECUTOR,
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode:
        raise ValueError(result.stderr.strip() or "swap stream preflight failed")


def run_experiment(bridge, args, run_dir, stop, accounts=inspect_accounts):
    ledger = PnLLedger("meteora", f"{args.pool}-live-experiment")
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
    cleanup_error = None
    ranges = {}           # open position -> (lowest bin, highest bin)
    outlay = 0            # lamports the executor's run cap has counted so far
    last_active = None
    n_shifts = 0
    shift_skip_logged = False
    shift_deferred_logged = False
    last_adjustment_at = None

    def state():
        result = bridge.get_state(args.pool)
        log.emit("state_observation", ok=result.ok, state=result.data, error=result.error)
        if not result.ok or not result.data:
            raise ValueError(f"pool state unavailable: {result.error or 'empty response'}")
        data = result.data
        if (data["token_x"]["mint"], data["token_y"]["mint"]) != (args.base_mint, args.quote_mint):
            raise ValueError("pool mint pair mismatch")
        if not 0 <= time.time() - float(data["fetched_at"]) <= 30:
            raise ValueError("pool state is stale or from the future")
        return data

    def position_gone(pid) -> bool:
        try:
            result = bridge.get_position(pid)
        except Exception:
            return False
        log.emit("position_observation", **(asdict(result) | {"position_id": pid}))
        return not result.ok and result.error == "unknown_position"

    def action(verb, payload, pid):
        nonlocal fees, unknown, signed, outlay
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
        known_failed = (result.error == "transaction_failed" and bool(result.tx_receipts)
                        and all(r.get("status") == "failed" and r.get("slot") is not None
                                and r.get("fee_lamports") is not None
                                for r in result.tx_receipts))
        unknown = bool(unknown or result.unknown_outcome or (not result.ok and (
            not known_failed and (result.error not in unsubmitted
            or result.tx_signatures or result.tx_receipts
            or result.total_fee_lamports is not None
            or (result.data or {}).get("pending_signature")))))
        if result.total_fee_lamports is not None:
            fees += result.total_fee_lamports
            ledger.on_cash_flow("rebalance", time.time(),
                                -result.total_fee_lamports / 1e9 * sol_in_quote(args, mid), label=verb)
        if not result.ok:
            if verb == "deposit_single_sided" and not unknown:
                opened.remove(pid)  # proven rejection or failed receipt: no position was created
                chain_error = (result.data or {}).get("chain_error")
                instruction_error = (chain_error or {}).get("InstructionError") if isinstance(chain_error, dict) else None
                bin_drift = (known_failed and isinstance(instruction_error, list)
                             and len(instruction_error) == 2
                             and instruction_error[1] == {"Custom": 6004})
                if result.error in DRIFT_ERRORS or bin_drift:
                    raise OpeningDrift(f"deposit_single_sided failed: {result.error}")
            raise ActionFailed(f"{verb} failed: {result.error}", result)
        if (result.position_id or (result.data or {}).get("position_id")) != pid:
            unknown = True
            raise ValueError("executor returned an unexpected position address")
        if not result.tx_signatures or result.total_fee_lamports is None:
            raise ValueError("confirmed action has incomplete receipt evidence")
        if verb == "withdraw":
            outlay += WITHDRAW_CHARGE
        return result

    def snapshot():
        nonlocal mid, last_active
        data = state()
        last_active = int(data["active_bin"])
        mid = grid.price_from_bin(last_active)
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

    def close(pid):
        """Withdraw 100 % of one verified position this run opened."""
        before = bridge.get_position(pid)
        log.emit("position_observation", **(asdict(before) | {"position_id": pid}))
        if not before.ok and before.error == "unknown_position":
            opened.remove(pid)  # rejected deposit; exact preflight PDA remains absent
            ranges.pop(pid, None)
            return
        if not before.ok or not before.data or (before.data.get("owner"), before.data.get("pool")) != (args.wallet, args.pool):
            raise ValueError("cleanup cannot verify owned position")
        result = action("withdraw", {"position_id": pid, "percent": 100}, pid)
        if not (result.data or {}).get("closed"):
            raise ValueError("withdrawal did not confirm closure")
        claim = result.data["fees_claimed"]
        claimed["base"] += int(claim["x_raw"])
        claimed["quote"] += int(claim["y_raw"])
        opened.remove(pid)
        ranges.pop(pid, None)

    def shift():
        """Move everything one-sided next to the price; claimed fees compound into it.

        Every range is past the price by then, so it is all one token: no swap.
        """
        nonlocal n_shifts, outlay, last_adjustment_at
        active_now = int(state()["active_bin"])
        gap, _ = shift_status(active_now, ranges, last_adjustment_at, time.monotonic(),
                              args.shift_cooldown_seconds)
        if gap < args.shift_gap:
            log.emit("shift_cancelled", reason="price returned before withdrawal",
                     active_bin=active_now, gap_bins=gap)
            return False
        side = "bid" if active_now > max(hi for _, hi in ranges.values()) else "ask"
        log.emit("shift_started", side=side, active_bin=active_now, ranges=ranges)
        for pid in list(opened):
            close(pid)
        token, decimals = (("quote", grid.quote_decimals) if side == "bid"
                           else ("base", grid.base_decimals))
        initial_raw = round((initial_quote if side == "bid" else initial_base) * 10**decimals)
        # ponytail: mirrors the opening loop (proven live) rather than refactoring it.
        for attempt in range(1, 4):
            fresh = state()
            active = int(fresh["active_bin"])
            # Experiment-owned holdings only: principal plus claimed fees of this token.
            owned = int(fresh["balances_raw"][token]) - int(baseline[token]) + initial_raw
            if owned <= 0:
                raise ValueError("shift found no experiment-owned balance to redeposit")
            lo = active - args.width if side == "bid" else active + 1
            leg = {"side": side, "bin_ids": list(range(lo, lo + args.width)),
                   "amounts": per_bin(owned / 10**decimals, decimals, args.width)}
            candidate = accounts(args, [leg])
            log.emit("opening_plan", attempt=attempt, expected_active_bin=active, leg=leg,
                     shift=n_shifts + 1, **candidate)
            positions = candidate["positions"]
            if len(positions) != 1 or positions[0]["side"] != side or positions[0]["exists"]:
                raise ValueError("shift deposit PDA already exists or account preflight is incomplete")
            if candidate["native_lamports"] / 1e9 < candidate["rent_lamports"] / 1e9 + args.fee_budget_sol:
                raise ValueError("shift deposit cannot retain rent and closing fee reserve")
            pid = positions[0]["position_id"]
            checked_legs.append(dict(leg))
            if pid not in proposed:
                proposed.append(pid)
            opened.append(pid)
            try:
                action("deposit_single_sided", dict(pool=args.pool, **leg, expected_active_bin=active,
                       max_active_bin_slippage=ACTIVE_BIN_TOLERANCE), pid)
            except OpeningDrift:
                log.emit("opening_retry", side=side, attempt=attempt, position_id=pid,
                         reason="confirmed or unsigned active-bin rejection")
                if attempt == 3:
                    raise
                continue
            outlay += candidate["rent_lamports"] + TOKEN_ACCOUNT_LAMPORTS + SIGNATURE_LAMPORTS
            ranges[pid] = (leg["bin_ids"][0], leg["bin_ids"][-1])
            n_shifts += 1
            last_adjustment_at = time.monotonic()
            log.emit("shift_done", shift=n_shifts, side=side, position_id=pid, bins=ranges[pid],
                     amount_raw=owned)
            return True

    try:
        data = state()
        grid = grid_from_reads(data)
        mid = mid0 = grid.price_from_bin(int(data["active_bin"]))
        ledger.mark(time.time(), mid)
        initial_quote = args.capital / 2
        initial_base = math.floor(initial_quote / mid * 10**grid.base_decimals) / 10**grid.base_decimals
        baseline = data["balances_raw"].copy()
        # Token balances only: base is wSOL in its token account, not native SOL.
        need = {"base": round(initial_base * 10**grid.base_decimals),
                "quote": round(initial_quote * 10**grid.quote_decimals)}
        short = [f"{side} raw {baseline[side]} < {need[side]}"
                 for side in need if int(baseline[side]) < need[side]]
        if short:
            raise ValueError(f"insufficient pre-funded base/quote ({'; '.join(short)}); "
                             "experiment never swaps to fund itself")
        active = int(data["active_bin"])
        legs = [{"side": side, "bin_ids": list(range(lo, lo + args.width)),
                 "amounts": per_bin(amount, decimals, args.width)}
                for side, lo, amount, decimals in (
                    ("bid", active - args.width, initial_quote, grid.quote_decimals),
                    ("ask", active + 1, initial_base, grid.base_decimals))]
        inspected = accounts(args, legs)
        proposed = [p["position_id"] for p in inspected["positions"]]
        log.emit("experiment_plan", args=vars(args), legs=legs,
                 caps={k: os.environ[k] for k in GATEWAY_ENV_KEYS
                       if k.startswith("MAX_") and k in os.environ}, **inspected)
        existing = inspected.get("existing_position_ids")
        if not isinstance(existing, list) or any(not isinstance(pid, str) for pid in existing):
            raise ValueError("owned position inventory preflight is incomplete")
        if existing:
            raise ValueError(f"wallet already has {len(existing)} position(s) in this pool; "
                             "recover them before a new experiment")
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
        inspect_stream(args.pool)
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
                        legs[1]["amounts"] = per_bin(initial_base, grid.base_decimals, args.width)
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
                               expected_active_bin=active, max_active_bin_slippage=ACTIVE_BIN_TOLERANCE), pid)
                        deployed = True
                        ranges[pid] = (leg["bin_ids"][0], leg["bin_ids"][-1])
                        outlay += (inspected["rent_lamports"] // 2 + TOKEN_ACCOUNT_LAMPORTS
                                   + SIGNATURE_LAMPORTS)
                        break
                    except OpeningDrift:
                        log.emit("opening_retry", side=leg["side"], attempt=attempt,
                                 position_id=pid, reason="confirmed or unsigned active-bin rejection")
                        if attempt == 3:
                            raise
            last_adjustment_at = time.monotonic()
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
                if args.shift_gap and ranges:
                    gap, cooldown_left = shift_status(
                        last_active, ranges, last_adjustment_at, time.monotonic(),
                        args.shift_cooldown_seconds)
                    if gap >= args.shift_gap and cooldown_left > 0:
                        if not shift_deferred_logged:
                            log.emit("shift_deferred", reason="cooldown", gap_bins=gap,
                                     remaining_seconds=cooldown_left)
                            shift_deferred_logged = True
                    elif gap >= args.shift_gap:
                        shift_deferred_logged = False
                        # Closes now, the new deposit, and its own final close.
                        need = (len(opened) + 1) * WITHDRAW_CHARGE + (
                            inspected["rent_lamports"] // 2 + TOKEN_ACCOUNT_LAMPORTS + SIGNATURE_LAMPORTS)
                        if outlay + need <= float(os.environ["MAX_SOL_PER_RUN"]) * 1e9:
                            if shift():
                                continue
                        elif not shift_skip_logged:
                            log.emit("shift_skipped", reason="MAX_SOL_PER_RUN would be exceeded",
                                     outlay_lamports=outlay, need_lamports=need)
                            shift_skip_logged = True
                    else:
                        shift_deferred_logged = False
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
                    close(pid)
                except Exception as exc:
                    log.emit("cleanup_error", position_id=pid, error=str(exc), unresolved_outcome=unknown)
                    # The executor signed and answered, but could not confirm the close
                    # (live 2026-10-02: a lagging read). One read settles it: an absent
                    # account means the close landed. Transport failures stay unresolved.
                    if (unknown and isinstance(exc, ActionFailed)
                            and exc.result.error == "submission_ambiguous" and position_gone(pid)):
                        unknown = False
                        opened.remove(pid)
                        log.emit("cleanup_resolved", position_id=pid, evidence="position account absent",
                                 note="fees_claimed unknown; any claim lands in the principal residual")
                        continue
                    cleanup_error = str(exc)
                    error = error or cleanup_error
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
                    cleanup_error = str(exc)
                    error = error or cleanup_error
                    log.emit("cleanup_error", error=cleanup_error)
        report = {"preflight_pass": preflight_pass, "live_started": bool(signed or unknown),
                  "n_shifts": n_shifts,
                  "stop_reason": reason, "cleanup_complete": cleanup_complete,
                  "unresolved_outcome": unknown, "remaining_position_ids": opened,
                  "expected_position_ids": proposed, "fee_lamports": fees, "error": error,
                  "cleanup_error": cleanup_error,
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
    install_stop_signals(stop)
    bridge = ExecBridge(["node", "dist/bridge.js"], cwd=str(EXECUTOR), timeout=120)
    try:
        report = run_experiment(bridge, args, run_dir, stop)
    finally:
        close_bridge(bridge)
    (run_dir / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    try:
        print(json.dumps({"run_dir": str(run_dir), **report}, indent=2))
    except OSError:
        pass  # terminal already gone (SIGHUP); summary.json above is the record
    return 0 if report["preflight_pass"] and report["cleanup_complete"] and not report["error"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
