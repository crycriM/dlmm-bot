#!/usr/bin/env python
"""
Deterministic rerun of a recorded dlmm-bot run (dlmm-logging-plan §7).

Load the log → rebuild the config from ``run_started`` (verifying its
``config_hash``) → re-instantiate a ``Keeper`` over a ``ReplayExecBridge`` fed
the recorded ``state_observation`` sequence, with ``time.time()`` frozen to each
observation's logged ``ts``. The replayed run is written to a scratch log, then
compared event-for-event against the original.

Zero diffs on the ``decision`` and ``action_request`` events prove the run is
fully reproducible from the log: any diff is nondeterminism or an unlogged
input (the diff payload points at the input that must be logged).

Usage:
    python tools/replay.py <run.jsonl> [--allow-hash-mismatch]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from dataclasses import dataclass, field
from typing import Any

from dlmm_bot.clock import FrozenClock
from dlmm_bot.event_log import EventLog, ReplayLog
from dlmm_bot.exec_bridge import ReplayExecBridge
from dlmm_bot.keeper import Keeper, hash_keeper_config, rebuild_keeper_config
from dlmm_bot.swap_observer import SwapObserver

# Envelope fields auto-added by EventLog.emit — differ by construction and are
# excluded from the decision/action comparison.
ENVELOPE_KEYS = {
    "seq", "prev_hash", "run_id", "schema_version", "ts_wall",
    "config_hash", "cycle", "event_type",
}


def _payload(event: dict) -> dict:
    return {k: v for k, v in event.items() if k not in ENVELOPE_KEYS}


@dataclass
class ReplayReport:
    config_hash_ok: bool = False
    n_decisions: int = 0
    n_actions: int = 0
    decision_diffs: list[dict] = field(default_factory=list)
    action_diffs: list[dict] = field(default_factory=list)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return (
            self.error is None
            and self.config_hash_ok
            and not self.decision_diffs
            and not self.action_diffs
        )

    def to_dict(self) -> dict:
        return {
            "config_hash_ok": self.config_hash_ok,
            "n_decisions": self.n_decisions,
            "n_actions": self.n_actions,
            "n_decision_diffs": len(self.decision_diffs),
            "n_action_diffs": len(self.action_diffs),
            "decision_diffs": self.decision_diffs,
            "action_diffs": self.action_diffs,
            "error": self.error,
            "ok": self.ok,
        }


def replay_session(
    log_path: str,
    allow_hash_mismatch: bool = False,
    replay_log_path: str | None = None,
) -> ReplayReport:
    """Replay a recorded log and diff decision/action_request against it."""
    report = ReplayReport()
    try:
        original = ReplayLog(log_path)  # raises on hash-chain break / seq gap
        started = original.run_started
        if started is None:
            raise ValueError("log has no run_started event")

        cfg = rebuild_keeper_config(started["config"])
        expected_hash = started.get("config_hash", "")
        actual_hash = hash_keeper_config(cfg)
        report.config_hash_ok = actual_hash == expected_hash
        if not report.config_hash_ok and not allow_hash_mismatch:
            raise ValueError(
                f"config hash mismatch (log={expected_hash[:12]}… "
                f"rebuilt={actual_hash[:12]}…); rerun aborted — "
                f"pass --allow-hash-mismatch to override"
            )

        events = original.events()
        states = [e for e in events if e.get("event_type") == "state_observation"]

        # Scratch log for the replayed run.
        if replay_log_path is None:
            fd, replay_log_path = tempfile.mkstemp(suffix=".jsonl", prefix="replay_")
            os.close(fd)
        replay_log = EventLog(replay_log_path, run_id=started.get("run_id"),
                              config_hash=actual_hash)
        try:
            observer = SwapObserver(replay_log, cfg.grid, cfg.pool_address)
            keeper = Keeper(cfg, ReplayExecBridge(events),
                            event_log=replay_log, swap_observer=observer)
            # Drive each cycle: freeze time.time to the observed ts, run _cycle.
            asyncio.run(_build_driver(keeper, states))
        finally:
            replay_log.close()

        replay_events = list(ReplayLog(replay_log_path))
        orig_decisions = [e for e in events if e.get("event_type") == "decision"]
        rep_decisions = [e for e in replay_events if e.get("event_type") == "decision"]
        orig_actions = [e for e in events if e.get("event_type") == "action_request"]
        rep_actions = [e for e in replay_events if e.get("event_type") == "action_request"]

        report.n_decisions = len(orig_decisions)
        report.n_actions = len(orig_actions)
        report.decision_diffs = [
            {"cycle": i, "original": _payload(a), "replayed": _payload(b)}
            for i, (a, b) in enumerate(zip(orig_decisions, rep_decisions))
            if _payload(a) != _payload(b)
        ] + [
            {"cycle": i, "original": _payload(a), "replayed": None}
            for i, a in enumerate(orig_decisions[len(rep_decisions):])
        ] + [
            {"cycle": i, "original": None, "replayed": _payload(b)}
            for i, b in enumerate(rep_decisions[len(orig_decisions):])
        ]
        report.action_diffs = [
            {"idx": i, "original": _payload(a), "replayed": _payload(b)}
            for i, (a, b) in enumerate(zip(orig_actions, rep_actions))
            if _payload(a) != _payload(b)
        ] + [
            {"idx": i, "original": _payload(a), "replayed": None}
            for i, a in enumerate(orig_actions[len(rep_actions):])
        ] + [
            {"idx": i, "original": None, "replayed": _payload(b)}
            for i, b in enumerate(rep_actions[len(orig_actions):])
        ]
    except Exception as e:  # noqa: BLE001 — report, don't crash the runner
        report.decision_diffs.clear()
        report.action_diffs.clear()
        report.error = f"{type(e).__name__}: {e}"
    finally:
        # Clean up the scratch log when we created it ourselves.
        if replay_log_path is not None and os.path.exists(replay_log_path) \
                and replay_log_path.startswith(tempfile.gettempdir()):
            try:
                os.remove(replay_log_path)
            except OSError:
                pass
    return report


def _build_driver(keeper: Keeper, states: list[dict]):
    """Return an async coro that drives the keeper, freezing the clock per
    observed ts (dlmm-logging-plan §7 step 3)."""

    async def drive() -> None:
        if not states:
            return
        last_ts = float(states[0].get("ts") or states[0].get("ts_wall") or 0.0)
        with FrozenClock(start=last_ts) as clock:
            for e in states:
                ts = float(e.get("ts") or e.get("ts_wall") or last_ts)
                clock.set(ts)
                await keeper._cycle()

    return drive()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("log", help="path to a run .jsonl log")
    ap.add_argument("--allow-hash-mismatch", action="store_true",
                    help="proceed even if the rebuilt config hash differs")
    args = ap.parse_args(argv)

    report = replay_session(args.log, allow_hash_mismatch=args.allow_hash_mismatch)
    print(json.dumps(report.to_dict(), indent=2, default=str))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
