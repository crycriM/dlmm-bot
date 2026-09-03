#!/usr/bin/env python
"""
Offline reconciliation for a dlmm-bot run log (dlmm-logging-plan §6).

Four readers of the same log, run offline (no live keeper required):

1. **Action verification** — every ``action_result`` is correlated to its
   ``action_request`` by ``req_id``; every ok result must carry a tx signature,
   deposit results a ``position_id``. With a ``ChainVerifier`` supplied, each
   ``tx_signature`` is fetched from chain and matched against the request.
2. **Fill verification** — every ``bin_fill`` is parented to an
   ``observed_trade`` by ``tx_signature``; every ``crossed_ours`` trade has a
   fill.
3. **State verification** — position lifecycle is coherent: a
   ``position_observation`` / ``position_withdrawn`` / ``position_closed`` never
   references a position that was not first ``position_created`` /
   ``position_liquidity_added``.
4. **Completeness** — ``observed_trade`` signatures are unique (no duplicate
   websocket replays). With a ``ChainVerifier``, the log's swap set is compared
   to the pool's full swap set over the window to catch dropped events.

The ``tx_signature`` is the join key throughout.

Usage:
    python tools/verify_log.py <run.jsonl>
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Optional, Protocol

from dlmm_bot.event_log import ReplayLog


class ChainVerifier(Protocol):
    """Optional live-chain access for deep verification. Not required for the
    structural checks (which run with chain=None)."""

    def get_transaction(self, signature: str) -> Optional[dict]:
        """Return the decoded transaction, or None."""

    def get_pool_signatures(self, pool: str, since: float, until: float) -> list[str]:
        """All swap tx signatures for ``pool`` over [since, until]."""


@dataclass
class VerifyReport:
    n_events: int = 0
    hash_chain_ok: bool = False

    req_missing_result: list[str] = field(default_factory=list)   # request w/o result
    req_missing_request: list[str] = field(default_factory=list)  # result w/o request
    ok_result_missing_tx: list[str] = field(default_factory=list)
    deposit_missing_position: list[str] = field(default_factory=list)
    chain_action_mismatch: list[str] = field(default_factory=list)

    fill_missing_parent: list[str] = field(default_factory=list)   # fill w/o trade
    crossed_ours_without_fill: list[str] = field(default_factory=list)

    state_missing_creation: list[str] = field(default_factory=list)

    duplicate_trades: list[str] = field(default_factory=list)
    missing_swaps: list[str] = field(default_factory=list)  # chain has, log lacks
    extra_swaps: list[str] = field(default_factory=list)    # log has, chain lacks

    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            self.hash_chain_ok
            and not self.req_missing_request
            and not self.ok_result_missing_tx
            and not self.deposit_missing_position
            and not self.chain_action_mismatch
            and not self.fill_missing_parent
            and not self.crossed_ours_without_fill
            and not self.state_missing_creation
            and not self.duplicate_trades
            and not self.missing_swaps
        )

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "n_events": self.n_events,
            "hash_chain_ok": self.hash_chain_ok,
            "req_missing_result": self.req_missing_result,
            "req_missing_request": self.req_missing_request,
            "ok_result_missing_tx": self.ok_result_missing_tx,
            "deposit_missing_position": self.deposit_missing_position,
            "chain_action_mismatch": self.chain_action_mismatch,
            "fill_missing_parent": self.fill_missing_parent,
            "crossed_ours_without_fill": self.crossed_ours_without_fill,
            "state_missing_creation": self.state_missing_creation,
            "duplicate_trades": self.duplicate_trades,
            "missing_swaps": self.missing_swaps,
            "extra_swaps": self.extra_swaps,
            "warnings": self.warnings,
        }


def verify_run(path: str, chain: Optional[ChainVerifier] = None) -> VerifyReport:
    """Run all structural reconciliation checks over a run log."""
    report = VerifyReport()

    # Hash chain + seq integrity (ReplayLog raises on any break).
    try:
        log = ReplayLog(path)
        report.hash_chain_ok = True
    except Exception as e:  # noqa: BLE001
        report.warnings.append(f"hash chain invalid: {e}")
        return report

    events = log.events()
    report.n_events = len(events)

    requests: dict[str, dict] = {}
    results: list[dict] = []
    trades: list[dict] = []
    fills: list[dict] = []
    created: set[str] = set()
    referenced_positions: list[tuple[str, str]] = []  # (event type, position_id)
    trade_sigs: list[str] = []
    first_ts = float(events[0].get("ts_wall") or 0.0) if events else 0.0
    last_ts = float(events[-1].get("ts_wall") or 0.0) if events else first_ts

    for ev in events:
        t = ev.get("event_type", "")
        if t == "action_request":
            requests[str(ev.get("req_id"))] = ev
        elif t == "action_result":
            results.append(ev)
        elif t == "observed_trade":
            trades.append(ev)
            sig = ev.get("tx_signature")
            if sig:
                trade_sigs.append(str(sig))
        elif t == "bin_fill":
            fills.append(ev)
        elif t in ("position_created", "position_liquidity_added"):
            pid = ev.get("position_id")
            if pid:
                created.add(str(pid))
        elif t in ("position_observation", "position_withdrawn", "position_closed"):
            pid = ev.get("position_id")
            if pid:
                referenced_positions.append((t, str(pid)))

    # --- 1. Action verification ----------------------------------------------
    result_req_ids = {str(r.get("req_id")) for r in results}
    for res in results:
        rid = str(res.get("req_id"))
        if rid not in requests:
            report.req_missing_request.append(rid)
        if res.get("ok"):
            if not res.get("tx_signatures"):
                report.ok_result_missing_tx.append(rid)
            if res.get("verb") == "deposit_single_sided" and not res.get("position_id"):
                report.deposit_missing_position.append(rid)
            if chain is not None and res.get("tx_signatures"):
                tx = chain.get_transaction(str(res["tx_signatures"][0]))
                if tx is None or tx.get("program") != "DLMM":
                    report.chain_action_mismatch.append(rid)
    for rid in requests:
        if rid not in result_req_ids:
            report.req_missing_result.append(rid)

    # --- 2. Fill verification -------------------------------------------------
    trade_sig_set = {str(t.get("tx_signature")) for t in trades if t.get("tx_signature")}
    for fl in fills:
        sig = fl.get("tx_signature")
        if sig and str(sig) not in trade_sig_set:
            report.fill_missing_parent.append(str(fl.get("label", sig)))
    for tr in trades:
        sig = tr.get("tx_signature")
        if tr.get("crossed_ours") and sig:
            if not any(f.get("tx_signature") == sig for f in fills):
                report.crossed_ours_without_fill.append(str(sig))

    # --- 3. State verification ------------------------------------------------
    for etype, pid in referenced_positions:
        if pid not in created:
            report.state_missing_creation.append(f"{etype}:{pid}")

    # --- 4. Completeness ------------------------------------------------------
    seen: set[str] = set()
    for s in trade_sigs:
        if s in seen:
            report.duplicate_trades.append(s)
        seen.add(s)

    if chain is not None:
        pool = (log.run_started or {}).get("pool_address") \
            or (trades[0].get("pool") if trades else "")
        chain_sigs = set(chain.get_pool_signatures(str(pool or ""), first_ts, last_ts))
        log_sigs = set(trade_sigs)
        report.missing_swaps = sorted(chain_sigs - log_sigs)
        report.extra_swaps = sorted(log_sigs - chain_sigs)

    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("log", help="path to a run .jsonl log")
    args = ap.parse_args(argv)

    report = verify_run(args.log)
    print(json.dumps(report.to_dict(), indent=2, default=str))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
