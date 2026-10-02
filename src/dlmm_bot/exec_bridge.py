"""
JSON stdin/stdout bridge to the TS Meteora executor.

The Python keeper owns sequencing and strategy; the TS shim owns signing
and SDK calls. Communication is JSON-lines: one request per stdin line,
one response per stdout line.

Verbs:
  deposit_single_sided(pool, side, bins, amounts, expected_active_bin,
                       max_active_bin_slippage, strategy_type) → tx signatures
  withdraw(position_id, percent)                               → tx signatures
  swap(pool, in_mint, out_mint, amount, max_slippage_bps)       → swap result
  refresh_bundle(withdraw_pos, swap_spec, deposit_spec)        → bundle result
  get_state(pool)                                              → activeBin, bins, balances
  get_position(position_id)                                    → per-bin amounts, claimable fees (raw)

This module provides a sync wrapper (for the keeper loop) and an async
wrapper (for tests with a subprocess). Both use the same JSON protocol.
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
import threading
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ExecResult:
    ok: bool
    data: dict | None = None
    error: str | None = None
    tx_signatures: list[str] = field(default_factory=list)
    # One normalized receipt per transaction. Scalar fields remain for
    # compatibility and mirror the final receipt.
    tx_receipts: list[dict] = field(default_factory=list)
    slot: int | None = None
    block_time: int | None = None
    fee_lamports: int | None = None
    compute_unit_price: int | None = None
    position_id: str | None = None
    # Timeout/EOF: the request may or may not have landed on chain. Never retry blindly.
    unknown_outcome: bool = False

    @property
    def succeeded(self) -> bool:
        return self.ok

    @property
    def total_fee_lamports(self) -> int | None:
        fees = [r.get("fee_lamports") for r in self.tx_receipts]
        known = [int(f) for f in fees if f is not None]
        if known:
            return sum(known)
        return self.fee_lamports

    @classmethod
    def from_payload(cls, raw: dict) -> "ExecResult":
        data = raw.get("data")
        nested = data if isinstance(data, dict) else {}

        def first(*keys):
            for source in (raw, nested):
                for key in keys:
                    if source.get(key) is not None:
                        return source[key]
            return None

        signatures = first("tx_signatures", "signatures")
        if signatures is None:
            signature = first("tx_signature", "signature", "txSignature")
            signatures = [signature] if signature else []
        elif isinstance(signatures, str):
            signatures = [signatures]
        else:
            signatures = list(signatures)

        receipt_rows = first("transactions", "tx_receipts", "receipts") or []
        if isinstance(receipt_rows, dict):
            receipt_rows = [receipt_rows]
        receipts: list[dict] = []
        for index, receipt in enumerate(receipt_rows):
            if not isinstance(receipt, dict):
                continue
            receipts.append({
                "signature": receipt.get("signature")
                    or receipt.get("tx_signature")
                    or (signatures[index] if index < len(signatures) else None),
                "slot": receipt.get("slot"),
                "block_time": receipt.get("block_time", receipt.get("blockTime")),
                "fee_lamports": receipt.get("fee_lamports", receipt.get("fee")),
                "compute_unit_price": receipt.get(
                    "compute_unit_price", receipt.get("computeUnitPrice")
                ),
            })
        if not receipts and signatures:
            for index, signature in enumerate(signatures):
                receipts.append({
                    "signature": signature,
                    "slot": first("slot") if index == len(signatures) - 1 else None,
                    "block_time": first("block_time", "blockTime")
                        if index == len(signatures) - 1 else None,
                    "fee_lamports": first("fee_lamports", "fee")
                        if index == len(signatures) - 1 else None,
                    "compute_unit_price": first("compute_unit_price", "computeUnitPrice")
                        if index == len(signatures) - 1 else None,
                })

        last = receipts[-1] if receipts else {}
        return cls(
            ok=bool(raw.get("ok", not raw.get("error"))),
            data=data,
            error=raw.get("error"),
            tx_signatures=[str(s) for s in signatures if s],
            tx_receipts=receipts,
            slot=last.get("slot", first("slot")),
            block_time=last.get("block_time", first("block_time", "blockTime")),
            fee_lamports=last.get("fee_lamports", first("fee_lamports", "fee")),
            compute_unit_price=last.get(
                "compute_unit_price", first("compute_unit_price", "computeUnitPrice")
            ),
            position_id=first("position_id", "positionAddress"),
        )


class ExecBridge:
    """Talks to the TS executor subprocess via JSON-lines."""

    def __init__(
        self,
        cmd: list[str],
        cwd: str | None = None,
        timeout: float = 120.0,
        refresh_timeout: float = 420.0,
    ):
        """
        cmd: e.g. ['node', 'executor/bridge.js']
        cwd: directory where the TS executor lives
        timeout: per-request cap in seconds for single-transaction verbs and
            reads. One confirmation is bounded by blockhash expiry (~150 blocks,
            <=90 s) plus receipt retries (<=10 s); the Jito bundle deadline is 60 s.
        refresh_timeout: cap for refresh_bundle. Sequentially (JITO_ENABLED=false,
            how live runs go) it confirms up to 4 legs: withdraw, swap, bid, ask.
        A kill below these budgets cuts a slow-but-fine confirmation mid-flight.
        """
        self.cmd = cmd
        self.cwd = cwd
        self.timeout = timeout
        self.refresh_timeout = refresh_timeout
        self._proc: subprocess.Popen | None = None
        self._stderr_tail: deque[str] = deque(maxlen=50)
        self._seq = 0

    def start(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            return
        self._proc = subprocess.Popen(
            self.cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.cwd,
            text=True,
            bufsize=1,
            # Own session: a terminal Ctrl-C reaches the keeper only, so its
            # graceful stop still has a live executor to withdraw through. The
            # child still exits when its stdin closes (keeper gone or stop()).
            start_new_session=True,
        )
        # The executor logs everything to stderr. If nobody reads it the OS pipe (64 KiB)
        # fills, node blocks on write and the keeper blocks on readline forever.
        threading.Thread(
            target=self._drain_stderr, args=(self._proc.stderr,), daemon=True,
        ).start()
        logger.info("ExecBridge subprocess started: %s", self.cmd)

    def _drain_stderr(self, stream) -> None:
        try:
            for line in stream:
                self._stderr_tail.append(line.rstrip())
                logger.debug("executor: %s", line.rstrip())
        except (ValueError, OSError):  # stream closed by stop()
            pass

    def stop(self) -> None:
        if self._proc is None:
            return
        try:
            self._proc.stdin.close()
        except Exception:
            pass
        try:
            self._proc.terminate()
            self._proc.wait(timeout=5)
        except Exception:
            self._proc.kill()
        self._proc = None

    def _send(self, request: dict) -> ExecResult:
        """Send a JSON request and read the JSON response.

        A failure here does NOT mean the action did not happen: on timeout or EOF a
        signed transaction may already be on chain, so callers must reconcile from
        chain state (get_state/get_position) before retrying a mutating verb.
        """
        self._seq += 1
        rid = f"x{self._seq}"  # echoed by the executor; joins its audit line to this call
        try:
            # NaN/inf must never reach the signer
            line = json.dumps({**request, "id": rid}, allow_nan=False) + "\n"
        except ValueError as e:
            return ExecResult(ok=False, error=f"refusing to send non-finite value: {e}")
        if self._proc is None or self._proc.poll() is not None:
            self.start()
        assert self._proc is not None and self._proc.stdin is not None
        proc = self._proc
        proc.stdin.write(line)
        proc.stdin.flush()
        # Killing the process unblocks readline (EOF) and guarantees a late reply can
        # never be misread as the answer to the NEXT request. Do not keep the child
        # alive to await it: the keeper reconciles and may write again while an
        # abandoned multi-leg refresh_bundle is still signing.
        timed_out = threading.Event()

        def _kill() -> None:
            timed_out.set()
            proc.kill()

        timeout = self.refresh_timeout if request.get("method") == "refresh_bundle" else self.timeout
        timer = threading.Timer(timeout, _kill)
        timer.start()
        try:
            resp_line = proc.stdout.readline()
        finally:
            timer.cancel()
        if timed_out.is_set():
            logger.critical("executor timed out after %.0fs on %s; killed, outcome UNKNOWN",
                            timeout, request.get("method"))
            return ExecResult(ok=False, unknown_outcome=True,
                              error=f"executor timeout after {timeout:.0f}s (killed; outcome unknown)")
        if not resp_line:
            tail = "\n".join(self._stderr_tail)
            return ExecResult(ok=False, unknown_outcome=True, error=f"executor EOF: {tail[-500:]}")
        try:
            raw = json.loads(resp_line)
        except json.JSONDecodeError:
            raw = None
        if not isinstance(raw, dict) or raw.get("id") != rid:
            # Not our reply (stray stdout, desync). Our real reply may still be in the
            # pipe for the next request to misread: fail closed exactly like a timeout.
            proc.kill()
            logger.critical("executor reply does not match %s on %s; killed, outcome UNKNOWN",
                            rid, request.get("method"))
            return ExecResult(ok=False, unknown_outcome=True,
                              error=f"executor reply mismatch for {rid}: {resp_line[:200]!r} "
                                    "(killed; outcome unknown)")
        return ExecResult.from_payload(raw)

    # -- Verb wrappers ------------------------------------------------

    def get_state(self, pool: str) -> ExecResult:
        return self._send({"method": "get_state", "pool": pool})

    def get_position(self, position_id: str) -> ExecResult:
        """Return per-bin amounts and raw claimable fees."""
        return self._send({"method": "get_position", "position_id": position_id})

    def deposit_single_sided(
        self,
        pool: str,
        side: str,
        bin_ids: list[int],
        amounts: list[float],
        strategy_type: str = "Spot",
        *,
        expected_active_bin: int,
        max_active_bin_slippage: int,
    ) -> ExecResult:
        """Deposit target allocations; confirmed position readback is authoritative."""
        return self._send({
            "method": "deposit_single_sided",
            "pool": pool,
            "side": side,
            "bin_ids": bin_ids,
            "amounts": amounts,
            "expected_active_bin": expected_active_bin,
            "max_active_bin_slippage": max_active_bin_slippage,
            "strategy_type": strategy_type,
        })

    def withdraw(self, position_id: str, percent: int = 100) -> ExecResult:
        # Integer percent of the position (100 = full exit), not basis points. The
        # executor rejects anything outside 1..100; refuse it here before spawning.
        if not isinstance(percent, int) or not 1 <= percent <= 100:
            return ExecResult(ok=False, error=f"refusing withdraw: percent={percent!r} outside 1..100")
        return self._send({
            "method": "withdraw",
            "position_id": position_id,
            "percent": percent,
        })

    def swap(
        self,
        in_mint: str,
        out_mint: str,
        amount: float,
        pool: str,
        max_slippage_bps: int = 50,
    ) -> ExecResult:
        if not (isinstance(amount, (int, float)) and math.isfinite(amount) and amount > 0):
            return ExecResult(ok=False, error=f"refusing swap: amount={amount!r} must be finite and > 0")
        if not 0 < max_slippage_bps <= 1_000:
            return ExecResult(ok=False, error=f"refusing swap: max_slippage_bps={max_slippage_bps!r} outside (0, 1000]")
        return self._send({
            "method": "swap",
            "in_mint": in_mint,
            "out_mint": out_mint,
            "amount": amount,
            "max_slippage_bps": max_slippage_bps,
            "pool": pool,
        })

    def refresh_bundle(
        self,
        withdraw_position_id: str,
        swap_spec: dict | None,
        deposit_spec: dict,
    ) -> ExecResult:
        return self._send({
            "method": "refresh_bundle",
            "withdraw_position_id": withdraw_position_id,
            "swap_spec": swap_spec,
            "deposit_spec": deposit_spec,
        })


#
# FakeExecBridge — for tests and shadow/dry-run mode (no subprocess).
#

class FakeExecBridge:
    """In-memory bridge for tests and shadow mode. Records all calls."""

    def __init__(self):
        self.calls: list[dict] = []
        self._state: dict[str, dict] = {}
        self._positions: dict[str, dict] = {}
        self._receipt: dict = {}
        self._next_ok = True

    def set_state(self, pool: str, active_bin: int, balances: dict | None = None, tvl_usd: float | None = None):
        self._state[pool] = {
            "active_bin": active_bin,
            "balances": balances or {"base": 0.0, "quote": 0.0},
            "tvl_usd": tvl_usd,
        }

    def set_position(self, position_id: str, data: dict):
        self._positions[position_id] = data

    def set_receipt(self, **fields):
        """Inject on-chain receipt fields (slot, fee_lamports, ...) into every
        successful action result returned by the fake executor."""
        self._receipt.update(fields)

    def set_next_result(self, ok: bool):
        self._next_ok = ok

    def start(self):
        pass

    def stop(self):
        pass

    def _record(self, method: str, **kwargs) -> ExecResult:
        call = {"method": method, **kwargs}
        self.calls.append(call)
        if not self._next_ok:
            return ExecResult(ok=False, error="fake error")
        if method == "get_state":
            pool = kwargs.get("pool", "")
            st = self._state.get(pool, {"active_bin": 0, "balances": {}})
            return ExecResult(ok=True, data=st)
        if method == "get_position":
            pid = kwargs.get("position_id", "")
            pos = self._positions.get(pid)
            if pos is None:
                return ExecResult(ok=False, error=f"unknown position {pid}")
            return ExecResult(ok=True, data=pos)
        receipt = {
            "signature": "fake_tx_001",
            "slot": self._receipt.get("slot"),
            "block_time": self._receipt.get("block_time"),
            "fee_lamports": self._receipt.get("fee_lamports"),
            "compute_unit_price": self._receipt.get("compute_unit_price"),
        }
        return ExecResult(
            ok=True, data={"method": method}, tx_signatures=["fake_tx_001"],
            tx_receipts=[receipt],
            slot=receipt["slot"], block_time=receipt["block_time"],
            fee_lamports=receipt["fee_lamports"],
            compute_unit_price=receipt["compute_unit_price"],
            position_id=self._receipt.get("position_id"),
        )

    def get_position(self, position_id: str) -> ExecResult:
        return self._record("get_position", position_id=position_id)

    def get_state(self, pool: str) -> ExecResult:
        return self._record("get_state", pool=pool)

    def deposit_single_sided(
        self, pool, side, bin_ids, amounts, strategy_type="Spot", *,
        expected_active_bin, max_active_bin_slippage,
    ) -> ExecResult:
        return self._record(
            "deposit_single_sided", pool=pool, side=side,
            bin_ids=bin_ids, amounts=amounts,
            expected_active_bin=expected_active_bin,
            max_active_bin_slippage=max_active_bin_slippage,
            strategy_type=strategy_type,
        )

    def withdraw(self, position_id: str, percent: int = 100) -> ExecResult:
        return self._record("withdraw", position_id=position_id, percent=percent)

    def swap(self, in_mint, out_mint, amount, pool, max_slippage_bps=50) -> ExecResult:
        return self._record(
            "swap", in_mint=in_mint, out_mint=out_mint, amount=amount,
            max_slippage_bps=max_slippage_bps, pool=pool,
        )

    def refresh_bundle(self, withdraw_position_id, swap_spec, deposit_spec) -> ExecResult:
        return self._record(
            "refresh_bundle",
            withdraw_position_id=withdraw_position_id,
            swap_spec=swap_spec, deposit_spec=deposit_spec,
        )


#
# ReplayExecBridge — deterministic rerun from recorded events.
#
class ReplayExecBridge:
    """ExecBridge that replays a recorded run log.

    Implements the same verb interface as ``FakeExecBridge``/``ExecBridge`` and
    returns the logged results a keeper recorded, so a re-instantiated Keeper
    (fed the recorded ``state_observation`` sequence with a frozen clock)
    re-derives identical decisions and actuations:

    - ``get_state``      → the next recorded ``state_observation`` (raw ``state``)
    - ``get_position``   → the next recorded ``position_observation`` (``data``)
    - mutating verbs     → the next recorded ``action_result`` for that verb
      (``deposit_single_sided`` / ``withdraw`` / ``swap`` / ``refresh_bundle``),
      reconstructed with its on-chain receipt fields.

    Results are consumed in recorded seq order, which matches the replayed
    keeper's call order because the decision sequence is deterministic.
    """

    def __init__(self, events: "list[dict]"):
        self._states = [e for e in events if e.get("event_type") == "state_observation"]
        self._positions = [
            e for e in events if e.get("event_type") == "position_observation"
        ]
        results_by_req = {
            str(e.get("req_id")): e
            for e in events if e.get("event_type") == "action_result"
        }
        self._actions: list[tuple[dict, dict]] = []
        for request in events:
            if request.get("event_type") != "action_request":
                continue
            req_id = str(request.get("req_id"))
            result = results_by_req.get(req_id)
            if result is None:
                raise ValueError(f"replay: request {req_id} has no action_result")
            self._actions.append((request, result))
        self._state_idx = 0
        self._position_idx = 0
        self._action_idx = 0
        self.calls: list[dict] = []

    def _next_state(self) -> ExecResult:
        if self._state_idx >= len(self._states):
            return ExecResult(ok=False, error="replay: exhausted state sequence")
        e = self._states[self._state_idx]
        self._state_idx += 1
        if e.get("ok", True) is False:
            return ExecResult(ok=False, error=e.get("error"), data=None)
        return ExecResult(ok=True, data=e.get("state"))

    def _next_position(self) -> ExecResult:
        if self._position_idx >= len(self._positions):
            return ExecResult(ok=False, error="replay: exhausted position sequence")
        e = self._positions[self._position_idx]
        self._position_idx += 1
        return ExecResult(
            ok=bool(e.get("ok", True)),
            data=e.get("data"),
            error=e.get("error"),
        )

    def _next_result(self, verb: str, payload: dict) -> ExecResult:
        if self._action_idx >= len(self._actions):
            raise ValueError(f"replay: no recorded action for {verb}")
        request, e = self._actions[self._action_idx]
        self._action_idx += 1
        if request.get("verb") != verb or request.get("payload") != payload:
            raise ValueError(
                f"replay action mismatch: recorded={request.get('verb')} "
                f"{request.get('payload')} replayed={verb} {payload}"
            )
        return ExecResult(
            ok=bool(e.get("ok", False)),
            data=e.get("data"),
            error=e.get("error"),
            tx_signatures=list(e.get("tx_signatures") or []),
            tx_receipts=list(e.get("transactions") or []),
            position_id=e.get("position_id"),
            slot=e.get("slot"),
            block_time=e.get("block_time"),
            fee_lamports=e.get("fee_lamports"),
            compute_unit_price=e.get("compute_unit_price"),
        )

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def get_state(self, pool: str) -> ExecResult:
        self.calls.append({"method": "get_state", "pool": pool})
        return self._next_state()

    def get_position(self, position_id: str) -> ExecResult:
        self.calls.append({"method": "get_position", "position_id": position_id})
        return self._next_position()

    def deposit_single_sided(
        self, pool, side, bin_ids, amounts, strategy_type="Spot", *,
        expected_active_bin, max_active_bin_slippage,
    ) -> ExecResult:
        self.calls.append({
            "method": "deposit_single_sided", "pool": pool, "side": side,
            "bin_ids": list(bin_ids), "amounts": list(amounts),
            "expected_active_bin": expected_active_bin,
            "max_active_bin_slippage": max_active_bin_slippage,
            "strategy_type": strategy_type,
        })
        return self._next_result("deposit_single_sided", {
            "pool": pool, "side": side, "bin_ids": list(bin_ids),
            "amounts": list(amounts),
            "expected_active_bin": expected_active_bin,
            "max_active_bin_slippage": max_active_bin_slippage,
            "strategy_type": strategy_type,
        })

    def withdraw(self, position_id: str, percent: int = 100) -> ExecResult:
        self.calls.append({"method": "withdraw", "position_id": position_id, "percent": percent})
        return self._next_result(
            "withdraw", {"position_id": position_id, "percent": percent}
        )

    def swap(
        self, in_mint, out_mint, amount, pool, max_slippage_bps=50,
    ) -> ExecResult:
        self.calls.append({
            "method": "swap", "in_mint": in_mint, "out_mint": out_mint,
            "amount": amount, "max_slippage_bps": max_slippage_bps, "pool": pool,
        })
        return self._next_result("swap", {
            "in_mint": in_mint, "out_mint": out_mint, "amount": amount,
            "max_slippage_bps": max_slippage_bps, "pool": pool,
        })

    def refresh_bundle(self, withdraw_position_id, swap_spec, deposit_spec) -> ExecResult:
        self.calls.append({
            "method": "refresh_bundle", "withdraw_position_id": withdraw_position_id,
            "swap_spec": swap_spec, "deposit_spec": deposit_spec,
        })
        return self._next_result("refresh_bundle", {
            "withdraw_position_id": withdraw_position_id,
            "swap_spec": swap_spec, "deposit_spec": deposit_spec,
        })


__all__ = ["ExecResult", "ExecBridge", "FakeExecBridge", "ReplayExecBridge"]
