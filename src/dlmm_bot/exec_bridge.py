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
        return ExecResult(
            ok=bool(raw.get("ok", False)),
            data=raw.get("data"),
            error=raw.get("error"),
            tx_signatures=raw.get("tx_signatures", raw.get("data", {}).get("tx_signatures", []) if isinstance(raw.get("data"), dict) else []),
        )

    # -- Verb wrappers ------------------------------------------------

    def get_state(self, pool: str) -> ExecResult:
        return self._send({"method": "get_state", "pool": pool})

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
        self._next_ok = True
        self._withdraw_side_effect = None

    def set_state(self, pool: str, active_bin: int, balances: dict | None = None, tvl_usd: float | None = None):
        self._state[pool] = {
            "active_bin": active_bin,
            "balances": balances or {"base": 0.0, "quote": 0.0},
            "tvl_usd": tvl_usd,
        }

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
        return ExecResult(ok=True, data={"method": method}, tx_signatures=["fake_tx_001"])

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


__all__ = ["ExecResult", "ExecBridge", "FakeExecBridge"]