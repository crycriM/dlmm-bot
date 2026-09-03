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
    python tools/verify_log.py <run.jsonl> --rpc-url URL --dlmm-program-id ID
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol

from dlmm_bot.event_log import ReplayLog


class ChainVerifier(Protocol):
    """Optional live-chain access for deep verification. Not required for the
    structural checks (which run with chain=None)."""

    def get_transaction(self, signature: str) -> Optional[dict]:
        """Return the decoded transaction, or None."""

    def get_pool_signatures(self, pool: str, since: float, until: float) -> list[str]:
        """All swap tx signatures for ``pool`` over [since, until]."""


class SolanaRpcVerifier:
    """Concrete stdlib Solana JSON-RPC reader for program/fee/completeness.

    Instruction payload and per-bin fill decoding remain adapter-specific; a
    richer ChainVerifier may add ``verb``, ``payload``, and ``bin_fills`` to
    the normalized transaction returned here.
    """

    def __init__(
        self,
        rpc_url: str,
        dlmm_program_id: str,
        rpc: Callable[[str, list], object] | None = None,
        timeout: float = 30.0,
    ):
        self.rpc_url = rpc_url
        self.dlmm_program_id = dlmm_program_id
        self.timeout = timeout
        self._rpc_override = rpc
        self._request_id = 0

    def _rpc(self, method: str, params: list):
        if self._rpc_override is not None:
            return self._rpc_override(method, params)
        self._request_id += 1
        body = json.dumps({
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
            "params": params,
        }).encode("utf-8")
        request = urllib.request.Request(
            self.rpc_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            envelope = json.loads(response.read().decode("utf-8"))
        if envelope.get("error"):
            raise RuntimeError(f"Solana RPC {method}: {envelope['error']}")
        return envelope.get("result")

    @staticmethod
    def _account_keys(result: dict) -> list[str]:
        message = result.get("transaction", {}).get("message", {})
        keys = []
        for row in message.get("accountKeys", []) or []:
            value = row.get("pubkey") if isinstance(row, dict) else row
            if value is not None:
                keys.append(str(value))
        return keys

    def get_transaction(self, signature: str) -> Optional[dict]:
        result = self._rpc("getTransaction", [signature, {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "commitment": "finalized",
        }])
        if not isinstance(result, dict):
            return None
        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
        programs = self._account_keys(result)
        logs = list(meta.get("logMessages") or [])
        return {
            "program": "DLMM" if self.dlmm_program_id in programs else "",
            "programs": programs,
            "slot": result.get("slot"),
            "block_time": result.get("blockTime"),
            "fee_lamports": meta.get("fee"),
            "logs": logs,
            "raw": result,
        }

    def get_pool_signatures(
        self, pool: str, since: float, until: float
    ) -> list[str]:
        swaps: list[str] = []
        before: str | None = None
        for _ in range(100):
            options: dict = {"limit": 1000, "commitment": "finalized"}
            if before:
                options["before"] = before
            rows = self._rpc("getSignaturesForAddress", [pool, options])
            if not isinstance(rows, list) or not rows:
                break
            reached_start = False
            for row in rows:
                block_time = row.get("blockTime")
                if block_time is not None and float(block_time) < since:
                    reached_start = True
                    continue
                if row.get("err") is not None:
                    continue
                if block_time is not None and float(block_time) > until:
                    continue
                signature = row.get("signature")
                if not signature:
                    continue
                tx = self.get_transaction(str(signature))
                logs = tx.get("logs", []) if tx else []
                if tx and _is_dlmm_transaction(tx) and any(
                    "swap" in str(line).lower() for line in logs
                ):
                    swaps.append(str(signature))
            if reached_start or len(rows) < 1000:
                break
            before = rows[-1].get("signature")
            if not before:
                break
        return swaps


@dataclass
class VerifyReport:
    n_events: int = 0
    hash_chain_ok: bool = False

    req_missing_result: list[str] = field(default_factory=list)   # request w/o result
    req_missing_request: list[str] = field(default_factory=list)  # result w/o request
    ok_result_missing_tx: list[str] = field(default_factory=list)
    deposit_missing_position: list[str] = field(default_factory=list)
    chain_action_mismatch: list[str] = field(default_factory=list)
    missing_receipt_fields: list[str] = field(default_factory=list)
    duplicate_req_ids: list[str] = field(default_factory=list)
    req_result_mismatch: list[str] = field(default_factory=list)
    missing_run_anchors: list[str] = field(default_factory=list)

    fill_missing_parent: list[str] = field(default_factory=list)   # fill w/o trade
    crossed_ours_without_fill: list[str] = field(default_factory=list)
    fill_chain_mismatch: list[str] = field(default_factory=list)

    state_missing_creation: list[str] = field(default_factory=list)

    duplicate_trades: list[str] = field(default_factory=list)
    missing_swaps: list[str] = field(default_factory=list)  # chain has, log lacks
    extra_swaps: list[str] = field(default_factory=list)    # log has, chain lacks

    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            self.hash_chain_ok
            and not self.req_missing_result
            and not self.req_missing_request
            and not self.ok_result_missing_tx
            and not self.deposit_missing_position
            and not self.chain_action_mismatch
            and not self.missing_receipt_fields
            and not self.duplicate_req_ids
            and not self.req_result_mismatch
            and not self.missing_run_anchors
            and not self.fill_missing_parent
            and not self.crossed_ours_without_fill
            and not self.fill_chain_mismatch
            and not self.state_missing_creation
            and not self.duplicate_trades
            and not self.missing_swaps
            and not self.extra_swaps
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
            "missing_receipt_fields": self.missing_receipt_fields,
            "duplicate_req_ids": self.duplicate_req_ids,
            "req_result_mismatch": self.req_result_mismatch,
            "missing_run_anchors": self.missing_run_anchors,
            "fill_missing_parent": self.fill_missing_parent,
            "crossed_ours_without_fill": self.crossed_ours_without_fill,
            "fill_chain_mismatch": self.fill_chain_mismatch,
            "state_missing_creation": self.state_missing_creation,
            "duplicate_trades": self.duplicate_trades,
            "missing_swaps": self.missing_swaps,
            "extra_swaps": self.extra_swaps,
            "warnings": self.warnings,
        }


def _is_dlmm_transaction(tx: dict) -> bool:
    program = str(tx.get("program", "")).lower()
    programs = [str(value).lower() for value in tx.get("programs", [])]
    return program in ("dlmm", "meteora") or any(
        value in ("dlmm", "meteora") for value in programs
    )


def _fill_matches(logged: dict, chain_fill: dict, tolerance: float = 1e-9) -> bool:
    for key in ("amount_base", "amount_quote", "fee_accrued"):
        if chain_fill.get(key) is None or logged.get(key) is None:
            continue
        if abs(float(chain_fill[key]) - float(logged[key])) > tolerance:
            return False
    return True


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
    started = log.run_started or {}
    bootstrap = (
        started.get("config", {}).get("keeper", {}).get("position_id")
        if isinstance(started.get("config"), dict) else None
    )
    created: set[str] = {str(bootstrap)} if bootstrap else set()
    trade_sigs: list[str] = []
    first_ts = float(events[0].get("ts_wall") or 0.0) if events else 0.0
    last_ts = float(events[-1].get("ts_wall") or 0.0) if events else first_ts

    for ev in events:
        t = ev.get("event_type", "")
        if t == "action_request":
            req_id = str(ev.get("req_id"))
            if req_id in requests:
                report.duplicate_req_ids.append(req_id)
            requests[req_id] = ev
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
            if pid and str(pid) not in created:
                report.state_missing_creation.append(f"{t}:{pid}")

    if len(log.by_type("run_started")) != 1:
        report.missing_run_anchors.append("run_started")
    if len(log.by_type("run_stopped")) != 1:
        report.missing_run_anchors.append("run_stopped")
    if any(e.get("event_type") == "swap_stream_unavailable" for e in events):
        report.missing_swaps.append("swap_stream_unavailable")

    # --- 1. Action verification ----------------------------------------------
    result_req_ids: set[str] = set()
    for res in results:
        rid = str(res.get("req_id"))
        if rid in result_req_ids:
            report.duplicate_req_ids.append(rid)
        result_req_ids.add(rid)
        request = requests.get(rid)
        if request is None:
            report.req_missing_request.append(rid)
        elif res.get("verb") != request.get("verb"):
            report.req_result_mismatch.append(f"{rid}:verb")
        if res.get("ok"):
            if not res.get("tx_signatures"):
                report.ok_result_missing_tx.append(rid)
            if res.get("verb") == "deposit_single_sided" and not res.get("position_id"):
                report.deposit_missing_position.append(rid)
            receipts = res.get("transactions") or []
            receipt_by_sig = {
                str(receipt.get("signature")): receipt
                for receipt in receipts if isinstance(receipt, dict)
            }
            for signature in res.get("tx_signatures") or []:
                receipt = receipt_by_sig.get(str(signature))
                if receipt is None:
                    report.missing_receipt_fields.append(f"{rid}:{signature}:receipt")
                else:
                    for field_name in ("slot", "block_time", "fee_lamports"):
                        if receipt.get(field_name) is None:
                            report.missing_receipt_fields.append(
                                f"{rid}:{signature}:{field_name}"
                            )
                if chain is None:
                    continue
                tx = chain.get_transaction(str(signature))
                if tx is None or not _is_dlmm_transaction(tx):
                    report.chain_action_mismatch.append(f"{rid}:{signature}:program")
                    continue
                request = requests.get(rid, {})
                chain_verb = tx.get("verb") or tx.get("method")
                chain_payload = tx.get("payload") or tx.get("request")
                if chain_verb is not None and chain_verb != request.get("verb"):
                    report.chain_action_mismatch.append(f"{rid}:{signature}:verb")
                if chain_payload is not None and chain_payload != request.get("payload"):
                    report.chain_action_mismatch.append(f"{rid}:{signature}:payload")
                chain_fee = tx.get("fee_lamports", tx.get("fee"))
                if receipt is not None and chain_fee is not None \
                        and int(chain_fee) != int(receipt.get("fee_lamports") or 0):
                    report.chain_action_mismatch.append(f"{rid}:{signature}:fee")
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
        if chain is not None and sig:
            tx = chain.get_transaction(str(sig))
            chain_fills = tx.get("bin_fills", []) if isinstance(tx, dict) else []
            for fill in (f for f in fills if f.get("tx_signature") == sig):
                match = next((
                    row for row in chain_fills
                    if row.get("position_id") == fill.get("position_id")
                    and row.get("bin_id") == fill.get("bin_id")
                ), None)
                if match is None or not _fill_matches(fill, match):
                    report.fill_chain_mismatch.append(str(fill.get("label") or sig))

    # --- 3. State verification happened in sequence during collection. -------

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
        report.missing_swaps.extend(sorted(chain_sigs - log_sigs))
        report.extra_swaps = sorted(log_sigs - chain_sigs)

    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("log", help="path to a run .jsonl log")
    ap.add_argument("--rpc-url", help="Solana JSON-RPC URL for chain checks")
    ap.add_argument(
        "--dlmm-program-id",
        help="expected Meteora DLMM program address (required with --rpc-url)",
    )
    args = ap.parse_args(argv)
    if args.rpc_url and not args.dlmm_program_id:
        ap.error("--dlmm-program-id is required with --rpc-url")
    chain = (
        SolanaRpcVerifier(args.rpc_url, args.dlmm_program_id)
        if args.rpc_url else None
    )

    report = verify_run(args.log, chain=chain)
    print(json.dumps(report.to_dict(), indent=2, default=str))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
