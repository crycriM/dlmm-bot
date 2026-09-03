"""
JSON stdin/stdout bridge to the TS Meteora executor.

The Python keeper owns sequencing and strategy; the TS shim owns signing
and SDK calls. Communication is JSON-lines: one request per stdin line,
one response per stdout line.

Verbs:
  deposit_single_sided(pool, side, bins, amounts, strategy_type) → tx signatures
  withdraw(position_id, bps)                                   → tx signatures
  swap(pool_or_jupiter, in_mint, out_mint, amount, max_slippage_bps) → swap result
  refresh_bundle(withdraw_pos, swap_spec, deposit_spec)        → bundle result
  get_state(pool)                                              → activeBin, bins, balances
  get_position(position_id)                                    → per-bin amounts, claimable fees (raw)

This module provides a sync wrapper (for the keeper loop) and an async
wrapper (for tests with a subprocess). Both use the same JSON protocol.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExecResult:
    ok: bool
    data: dict | None = None
    error: str | None = None
    tx_signatures: list[str] = field(default_factory=list)
    # On-chain receipt fields (dlmm-logging-plan §3); None when the executor
    # does not report them (older TS shims keep working unchanged).
    slot: int | None = None
    block_time: int | None = None
    fee_lamports: int | None = None
    compute_unit_price: int | None = None
    position_id: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.ok


class ExecBridge:
    """Talks to the TS executor subprocess via JSON-lines."""

    def __init__(self, cmd: list[str], cwd: str | None = None, timeout: float = 60.0):
        """
        cmd: e.g. ['node', 'executor/bridge.js']
        cwd: directory where the TS executor lives
        """
        self.cmd = cmd
        self.cwd = cwd
        self.timeout = timeout
        self._proc: subprocess.Popen | None = None

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
        )
        logger.info("ExecBridge subprocess started: %s", self.cmd)

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
        """Send a JSON request and read the JSON response."""
        if self._proc is None or self._proc.poll() is not None:
            self.start()
        assert self._proc is not None and self._proc.stdin is not None
        line = json.dumps(request) + "\n"
        self._proc.stdin.write(line)
        self._proc.stdin.flush()
        resp_line = self._proc.stdout.readline()
        if not resp_line:
            stderr = self._proc.stderr.read() if self._proc.stderr else ""
            return ExecResult(ok=False, error=f"executor EOF: {stderr[:500]}")
        try:
            raw = json.loads(resp_line)
        except json.JSONDecodeError as e:
            return ExecResult(ok=False, error=f"bad JSON from executor: {e}")
        data = raw.get("data")
        # Receipt fields may sit at the top level or be nested in data.
        def _receipt(key):
            val = raw.get(key)
            if val is None and isinstance(data, dict):
                val = data.get(key)
            return val
        return ExecResult(
            ok=bool(raw.get("ok", False)),
            data=data,
            error=raw.get("error"),
            tx_signatures=raw.get("tx_signatures", data.get("tx_signatures", []) if isinstance(data, dict) else []),
            slot=_receipt("slot"),
            block_time=_receipt("block_time"),
            fee_lamports=_receipt("fee_lamports"),
            compute_unit_price=_receipt("compute_unit_price"),
            position_id=_receipt("position_id"),
        )

    # -- Verb wrappers ------------------------------------------------

    def get_state(self, pool: str) -> ExecResult:
        return self._send({"method": "get_state", "pool": pool})

    def get_position(self, position_id: str) -> ExecResult:
        """dlmm-logging-plan §3: per-bin amounts + claimable fees (raw)."""
        return self._send({"method": "get_position", "position_id": position_id})

    def deposit_single_sided(
        self,
        pool: str,
        side: str,
        bin_ids: list[int],
        amounts: list[float],
        strategy_type: str = "Spot",
    ) -> ExecResult:
        return self._send({
            "method": "deposit_single_sided",
            "pool": pool,
            "side": side,
            "bin_ids": bin_ids,
            "amounts": amounts,
            "strategy_type": strategy_type,
        })

    def withdraw(self, position_id: str, bps: int = 100) -> ExecResult:
        return self._send({
            "method": "withdraw",
            "position_id": position_id,
            "bps": bps,
        })

    def swap(
        self,
        in_mint: str,
        out_mint: str,
        amount: float,
        max_slippage_bps: int = 50,
        pool: str | None = None,
    ) -> ExecResult:
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
        self._withdraw_side_effect = None

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
        successful action result — mirrors the TS executor extension (§3)."""
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
        return ExecResult(
            ok=True, data={"method": method}, tx_signatures=["fake_tx_001"],
            slot=self._receipt.get("slot"),
            block_time=self._receipt.get("block_time"),
            fee_lamports=self._receipt.get("fee_lamports"),
            compute_unit_price=self._receipt.get("compute_unit_price"),
            position_id=self._receipt.get("position_id"),
        )

    def get_position(self, position_id: str) -> ExecResult:
        return self._record("get_position", position_id=position_id)

    def get_state(self, pool: str) -> ExecResult:
        return self._record("get_state", pool=pool)

    def deposit_single_sided(
        self, pool, side, bin_ids, amounts, strategy_type="Spot",
    ) -> ExecResult:
        return self._record(
            "deposit_single_sided", pool=pool, side=side,
            bin_ids=bin_ids, amounts=amounts, strategy_type=strategy_type,
        )

    def withdraw(self, position_id: str, bps: int = 100) -> ExecResult:
        return self._record("withdraw", position_id=position_id, bps=bps)

    def swap(self, in_mint, out_mint, amount, max_slippage_bps=50, pool=None) -> ExecResult:
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
# ReplayExecBridge — deterministic rerun (dlmm-logging-plan §7).
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
        self._results: dict[str, list[dict]] = {}
        for e in events:
            if e.get("event_type") == "action_result":
                self._results.setdefault(str(e.get("verb", "")), []).append(e)
        self._state_idx = 0
        self._position_idx = 0
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
        return ExecResult(ok=True, data=e.get("data"))

    def _next_result(self, verb: str) -> ExecResult:
        queue = self._results.get(verb)
        if not queue:
            return ExecResult(ok=False, error=f"replay: no recorded result for {verb}")
        e = queue.pop(0)
        return ExecResult(
            ok=bool(e.get("ok", False)),
            data=e.get("data"),
            error=e.get("error"),
            tx_signatures=list(e.get("tx_signatures") or []),
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
        self, pool, side, bin_ids, amounts, strategy_type="Spot",
    ) -> ExecResult:
        self.calls.append({
            "method": "deposit_single_sided", "pool": pool, "side": side,
            "bin_ids": list(bin_ids), "amounts": list(amounts),
            "strategy_type": strategy_type,
        })
        return self._next_result("deposit_single_sided")

    def withdraw(self, position_id: str, bps: int = 100) -> ExecResult:
        self.calls.append({"method": "withdraw", "position_id": position_id, "bps": bps})
        return self._next_result("withdraw")

    def swap(
        self, in_mint, out_mint, amount, max_slippage_bps=50, pool=None,
    ) -> ExecResult:
        self.calls.append({
            "method": "swap", "in_mint": in_mint, "out_mint": out_mint,
            "amount": amount, "max_slippage_bps": max_slippage_bps, "pool": pool,
        })
        return self._next_result("swap")

    def refresh_bundle(self, withdraw_position_id, swap_spec, deposit_spec) -> ExecResult:
        self.calls.append({
            "method": "refresh_bundle", "withdraw_position_id": withdraw_position_id,
            "swap_spec": swap_spec, "deposit_spec": deposit_spec,
        })
        return self._next_result("refresh_bundle")


__all__ = ["ExecResult", "ExecBridge", "FakeExecBridge", "ReplayExecBridge"]