"""
Append-only, event-sourced JSONL log for a dlmm-bot run.

Design (docs/dlmm-logging-plan.md §1):
- one JSONL file per run: every record carries seq (monotonic, gap-free),
  run_id, schema_version, event_type, ts_wall, cycle, config_hash
- serialized writer: emit() is guarded by one process-local lock, so keeper
  cycle and swap-stream events share one total order
- tamper-evidence: prev_hash = sha256(previous line bytes), making the log
  a self-certifying hash chain for audits
- ReplayLog reads a recorded log back for rerun/verify/explain readers
- to_bin_events() converts a log losslessly into backtest BinEvents so
  live and backtest share one replay path

No third-party dependencies; pure stdlib.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import threading
import time
from typing import Any, Iterable, Iterator, Optional

SCHEMA_VERSION = 2


def _jsonable(obj: Any) -> Any:
    """Recursively replace inf/nan (invalid in strict JSON) with None."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    return obj


def git_sha(repo_path: str | None = None) -> str | None:
    """HEAD SHA of a git repo, None when unavailable (never raises)."""
    if not repo_path:
        return None
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path, capture_output=True, text=True, timeout=5,
            check=True,
        )
        return out.stdout.strip()
    except Exception:
        return None


def _pkg_dir(module_name: str) -> str | None:
    try:
        import importlib
        mod = importlib.import_module(module_name)
        return os.path.dirname(os.path.abspath(mod.__file__))
    except Exception:
        return None


class EventLog:
    """Single-writer append-only JSONL event log with a hash chain."""

    def __init__(
        self,
        path: str,
        run_id: str | None = None,
        config_hash: str = "",
        schema_version: int = SCHEMA_VERSION,
        resume: bool = False,
    ):
        self.path = path
        self.run_id = run_id or os.path.splitext(os.path.basename(path))[0]
        self.config_hash = config_hash
        self.schema_version = schema_version
        self._seq = 0
        self._cycle = 0
        self._prev_line: str | None = None
        self._lock = threading.RLock()
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)

        nonempty = os.path.exists(path) and os.path.getsize(path) > 0
        if nonempty and not resume:
            raise FileExistsError(
                f"event log already exists: {path}; pass resume=True to append"
            )
        if nonempty:
            prior = ReplayLog(path)
            last = prior.events()[-1]
            if last.get("event_type") == "run_stopped":
                raise ValueError("cannot resume a completed event-log run")
            prior_run_id = str(last.get("run_id", ""))
            prior_config_hash = str(last.get("config_hash", ""))
            if run_id is not None and prior_run_id != run_id:
                raise ValueError("run_id does not match existing event log")
            if config_hash and prior_config_hash != config_hash:
                raise ValueError("config_hash does not match existing event log")
            if int(last.get("schema_version")) != schema_version:
                raise ValueError("schema_version does not match existing event log")
            self.run_id = prior_run_id
            self.config_hash = prior_config_hash
            self._seq = int(last["seq"])
            self._cycle = int(last.get("cycle", 0))
            with open(path, "r", encoding="utf-8") as existing:
                lines = [line.rstrip("\n") for line in existing if line.strip()]
            self._prev_line = lines[-1]
        self._fh = open(path, "a", encoding="utf-8")

    def set_cycle(self, cycle: int) -> None:
        with self._lock:
            self._cycle = cycle

    def emit(self, event_type: str, **fields: Any) -> int:
        """Append one event, return its seq. Sync: call from the event loop."""
        with self._lock:
            self._seq += 1
            prev_hash = (
                hashlib.sha256((self._prev_line + "\n").encode("utf-8")).hexdigest()
                if self._prev_line is not None else ""
            )
            record = {
                **_jsonable(fields),
                "event_type": event_type,
                "prev_hash": prev_hash,
                "run_id": self.run_id,
                "schema_version": self.schema_version,
                "ts_wall": time.time(),
                "config_hash": self.config_hash,
                "cycle": self._cycle,
                "seq": self._seq,
            }
            line = json.dumps(record, sort_keys=True, separators=(",", ":"))
            self._fh.write(line + "\n")
            self._fh.flush()
            self._prev_line = line
            return record["seq"]

    def close(self) -> None:
        with self._lock:
            if self._fh and not self._fh.closed:
                self._fh.close()

    def __enter__(self) -> "EventLog":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class ReplayLog:
    """Reads a recorded JSONL log back, with hash-chain verification."""

    def __init__(self, path: str):
        self.path = path
        self._events: list[dict] = []
        self._load()

    def _load(self) -> None:
        prev_line: str | None = None
        with open(self.path, "r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh):
                line = line.rstrip("\n")
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("seq") != len(self._events) + 1:
                    raise ValueError(
                        f"seq gap at line {lineno + 1}: {rec.get('seq')}"
                    )
                if prev_line is None:
                    if rec.get("prev_hash") != "":
                        raise ValueError("first event must have an empty prev_hash")
                else:
                    expect = hashlib.sha256((prev_line + "\n").encode("utf-8")).hexdigest()
                    if rec.get("prev_hash") != expect:
                        raise ValueError(f"hash chain broken at line {lineno + 1}")
                    first = self._events[0]
                    for key in ("run_id", "schema_version", "config_hash"):
                        if rec.get(key) != first.get(key):
                            raise ValueError(f"{key} changed at line {lineno + 1}")
                self._events.append(rec)
                prev_line = line

    def __iter__(self) -> Iterator[dict]:
        return iter(self._events)

    def __len__(self) -> int:
        return len(self._events)

    def events(self) -> list[dict]:
        return list(self._events)

    def by_type(self, event_type: str) -> list[dict]:
        return [e for e in self._events if e.get("event_type") == event_type]

    @property
    def run_started(self) -> dict | None:
        started = self.by_type("run_started")
        return started[0] if started else None


def load_events(path: str) -> list[dict]:
    """Load all events from a log path without chain verification."""
    out: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def to_bin_events(
    events: Iterable[dict],
    default_fee_bps: float = 25.0,
) -> "list":
    """Convert a run log into backtest.BinEvent objects (lossless for the
    backtester's fields: ts, pool, active/prev bin, direction,
    trade_size_usd, fee_bps, tvl_usd).

    Only decoded ``observed_trade`` events are converted. Poll-derived active
    bin changes contain no trade size and cannot be represented losslessly as
    backtest fills.
    """
    from dlmm_bot.backtest import BinEvent

    evs = list(events)
    start = next((e for e in evs if e.get("event_type") == "run_started"), None)
    pool = (start or {}).get("pool_address", "")

    last_tvl: float | None = None
    last_fee = default_fee_bps

    rows: list[tuple[float, int, dict]] = []  # (ts, is_poll, payload)
    for e in evs:
        t = e.get("event_type")
        if t == "state_observation":
            tvl = e.get("tvl_usd")
            if tvl is not None:
                last_tvl = tvl
        elif t == "observed_trade":
            fee = e.get("fee_bps")
            if fee is not None:
                last_fee = fee
            rows.append((
                float(e.get("ts") or e.get("block_time") or e.get("ts_wall") or 0.0),
                0,
                {
                    "pool": pool,
                    "active_bin": e.get("new_active_bin", 0),
                    "prev_active_bin": e.get("prev_active_bin", 0),
                    "direction": e.get("direction", "up"),
                    "trade_size_usd": e.get("trade_size_usd", 0.0),
                    "fee_bps": last_fee,
                    "tvl_usd": e.get("tvl_usd", last_tvl),
                },
            ))

    rows.sort(key=lambda r: (r[0], r[1]))
    return [
        BinEvent(ts=ts, **payload)
        for ts, _is_poll, payload in rows
    ]


__all__ = [
    "SCHEMA_VERSION", "EventLog", "ReplayLog", "load_events",
    "to_bin_events", "git_sha", "_jsonable",
]
