"""Deterministic replay and offline reconciliation tests.

``record_session`` (conftest) records a full keeper run over a frozen clock;
the replay tool must re-derive every decision and action from the log alone,
and the verify tool must structurally accept a clean log and reject a broken
hash chain.
"""

from __future__ import annotations

import json

from dlmm_bot.event_log import EventLog, ReplayLog, load_events

# 8 cycles: regime warms up (stop quoting) → initial deposit → refreshes
ACTIVES = [100, 100, 101, 101, 102, 102, 103, 103]


def _record(record_session, tmp_path):
    path = str(tmp_path / "run.jsonl")
    return record_session(ACTIVES, path)


def _tamper_decision(path):
    """Corrupt one decision event — breaks the hash chain at the next line."""
    lines = open(path).read().splitlines()
    for i, line in enumerate(lines):
        rec = json.loads(line)
        if rec["event_type"] == "decision":
            rec["mid"] = rec["mid"] + 1.0
            lines[i] = json.dumps(rec, sort_keys=True, separators=(",", ":"))
            open(path, "w").write("\n".join(lines) + "\n")
            return
    raise AssertionError("no decision event in log")


class TestRecordedSession:
    """Structural invariants of a recorded keeper session."""

    def test_run_anchors_and_cycle_counts(self, record_session, tmp_path):
        path, _ = _record(record_session, tmp_path)
        evs = load_events(path)
        types = [e["event_type"] for e in evs]
        assert types[0] == "run_started"
        assert types[-1] == "run_stopped"
        assert types.count("state_observation") == len(ACTIVES)
        assert types.count("decision") == len(ACTIVES)

    def test_request_result_correlation(self, record_session, tmp_path):
        path, _ = _record(record_session, tmp_path)
        evs = load_events(path)
        reqs = [e for e in evs if e["event_type"] == "action_request"]
        res = [e for e in evs if e["event_type"] == "action_result"]
        assert len(reqs) == len(res) > 0
        assert {r["req_id"] for r in reqs} == {r["req_id"] for r in res}
        # every ok mutating result carries a tx signature (chain join key)
        assert all(r["tx_signatures"] for r in res if r["ok"])

    def test_action_request_payloads_match_bridge_kwargs(self, record_session, tmp_path):
        """The replay contract: each action_request payload == the kwargs of
        the next matching bridge call (request and call interleave 1:1)."""
        path, bridge = _record(record_session, tmp_path)
        evs = load_events(path)
        iters = {}
        for req in (e for e in evs if e["event_type"] == "action_request"):
            verb = req["verb"]
            if verb not in iters:
                iters[verb] = iter(
                    c for c in bridge.calls if c["method"] == verb
                )
            call = next(iters[verb])
            kw = {k: v for k, v in call.items() if k != "method"}
            assert req["payload"] == kw, f"{verb}: {req['payload']} != {kw}"

    def test_position_lifecycle(self, record_session, tmp_path):
        path, _ = _record(record_session, tmp_path)
        evs = load_events(path)
        created = [e for e in evs if e["event_type"] == "position_created"]
        deposits = [
            e for e in evs
            if e["event_type"] == "decision" and e["action"] == "initial_deposit"
        ]
        # exactly one position_created per initial deposit
        assert len(created) == len(deposits) >= 1
        created_seq = created[0]["seq"]
        n_obs = 0
        for e in evs:
            if e["event_type"] in ("position_observation", "position_withdrawn"):
                n_obs += e["event_type"] == "position_observation"
                assert e["position_id"] == "POS_1"
                assert e["seq"] > created_seq
        # observations happen on cycles that begin with a live position; in
        # this session every such cycle then ends in a stop-quoting withdraw,
        # while the final open position (deposited on the last cycle) is never
        # observed — so observation and withdrawal counts are equal
        withdrawals = [e for e in evs if e["event_type"] == "position_withdrawn"]
        assert n_obs >= 1
        assert len(withdrawals) == n_obs


class TestDeterministicReplay:
    def test_zero_diff_rerun(self, record_session, replay_tool, tmp_path):
        path, _ = _record(record_session, tmp_path)
        report = replay_tool.replay_session(path)
        assert report.error is None, report.error
        assert report.config_hash_ok
        assert report.n_decisions == len(ACTIVES)
        assert report.n_actions > 0
        assert report.decision_diffs == []
        assert report.action_diffs == []
        assert report.ok

    def test_replayed_decisions_identical_field_by_field(
        self, record_session, replay_tool, tmp_path
    ):
        # The zero-diff report already proves payload equality; also pin the
        # decision *sequence* so a silent permutation would fail.
        path, _ = _record(record_session, tmp_path)
        report = replay_tool.replay_session(path)
        assert report.ok, report.to_dict()
        decisions = [
            e for e in load_events(path) if e["event_type"] == "decision"
        ]
        actions = [d["action"] for d in decisions]
        # golden shape: regime gate holds the first 3 cycles, then the
        # deposit/stop-quoting alternation on a trending price series
        assert actions[0:4] == ["stop_quoting"] * 3 + ["initial_deposit"]
        assert all(a in ("stop_quoting", "initial_deposit", "refresh")
                   for a in actions[4:])
        assert all(d["decision"] in ("stop_quoting", "quote") for d in decisions)

    def test_tampered_log_fails_replay(self, record_session, replay_tool, tmp_path):
        path, _ = _record(record_session, tmp_path)
        _tamper_decision(path)
        report = replay_tool.replay_session(path)
        assert report.ok is False
        assert report.error is not None


class TestVerify:
    def test_clean_session_verifies(self, record_session, verify_tool, tmp_path):
        path, _ = _record(record_session, tmp_path)
        rep = verify_tool.verify_run(path)
        assert rep.ok, rep.to_dict()
        assert rep.hash_chain_ok
        assert rep.n_events == len(ReplayLog(path))
        assert rep.req_missing_request == []
        assert rep.ok_result_missing_tx == []
        assert rep.deposit_missing_position == []
        assert rep.duplicate_trades == []
        assert rep.state_missing_creation == []

    def test_tampered_log_fails_verification(self, record_session, verify_tool,
        tmp_path):
        path, _ = _record(record_session, tmp_path)
        _tamper_decision(path)
        rep = verify_tool.verify_run(path)
        assert rep.ok is False
        assert rep.hash_chain_ok is False

    def test_isolated_fill_lacks_parent(self, verify_tool, tmp_path):
        path = str(tmp_path / "iso.jsonl")
        with EventLog(path, run_id="r") as log:
            # observed_trade with a signature, but the child bin_fill references
            # a different signature → fill_missing_parent
            log.emit("observed_trade", tx_signature="S1", crossed_ours=False)
            log.emit("bin_fill", bin_id=100, side_filled="buy", amount_base=1.0,
                     bin_price=1.0, tx_signature="S2")
        rep = verify_tool.verify_run(path)
        assert "S2" in rep.fill_missing_parent
        assert not rep.ok

    def test_crossed_trade_without_fill_flagged(self, verify_tool, tmp_path):
        path = str(tmp_path / "x.jsonl")
        with EventLog(path, run_id="r") as log:
            log.emit("observed_trade", tx_signature="S1", crossed_ours=True,
                     prev_active_bin=100, new_active_bin=101)
        rep = verify_tool.verify_run(path)
        assert rep.crossed_ours_without_fill == ["S1"]
        assert not rep.ok

    def test_request_without_result_is_not_ok(self, verify_tool, tmp_path):
        path = str(tmp_path / "orphan.jsonl")
        with EventLog(path, run_id="r") as log:
            log.emit("action_request", req_id="req_1", verb="withdraw", payload={})
        rep = verify_tool.verify_run(path)
        assert rep.req_missing_result == ["req_1"]
        assert not rep.ok

    def test_extra_chain_swap_is_not_ok(self, verify_tool, tmp_path):
        class Chain:
            def get_transaction(self, signature):
                return {"program": "DLMM"}

            def get_pool_signatures(self, pool, since, until):
                return []

        path = str(tmp_path / "extra.jsonl")
        with EventLog(path, run_id="r") as log:
            log.emit("run_started", pool_address="p")
            log.emit(
                "observed_trade",
                tx_signature="unexpected",
                crossed_ours=False,
            )
        rep = verify_tool.verify_run(path, chain=Chain())
        assert rep.extra_swaps == ["unexpected"]
        assert not rep.ok


class TestVerifierOrdering:
    def test_position_reference_before_creation_is_rejected(
        self, verify_tool, tmp_path
    ):
        path = str(tmp_path / "out_of_order.jsonl")
        with EventLog(path, run_id="r") as log:
            log.emit("run_started", pool_address="p")
            log.emit("position_observation", position_id="P1", ok=True)
            log.emit("position_created", position_id="P1")
            log.emit("run_stopped", reason="stop")
        rep = verify_tool.verify_run(path)
        assert rep.state_missing_creation == ["position_observation:P1"]
        assert not rep.ok

    def test_bootstrapped_position_is_valid_lifecycle_root(
        self, verify_tool, tmp_path
    ):
        path = str(tmp_path / "bootstrap.jsonl")
        with EventLog(path, run_id="r") as log:
            log.emit(
                "run_started",
                pool_address="p",
                config={"keeper": {"position_id": "P0"}},
            )
            log.emit("position_observation", position_id="P0", ok=True)
            log.emit("run_stopped", reason="stop")
        rep = verify_tool.verify_run(path)
        assert rep.state_missing_creation == []
        assert rep.ok, rep.to_dict()

    def test_result_verb_must_match_request(self, verify_tool, tmp_path):
        path = str(tmp_path / "verb.jsonl")
        with EventLog(path, run_id="r") as log:
            log.emit("run_started", pool_address="p")
            log.emit(
                "action_request", req_id="q1", verb="withdraw", payload={}
            )
            log.emit(
                "action_result", req_id="q1", verb="swap", ok=False
            )
            log.emit("run_stopped", reason="stop")
        rep = verify_tool.verify_run(path)
        assert rep.req_result_mismatch == ["q1:verb"]
        assert not rep.ok


class TestSolanaRpcVerifier:
    def test_low_budget_waits_for_full_method_cost(self, verify_tool, monkeypatch):
        now = 0.0
        sleeps = []

        def monotonic():
            return now

        def sleep(seconds):
            nonlocal now
            sleeps.append(seconds)
            now += seconds

        monkeypatch.setattr(verify_tool.time, "monotonic", monotonic)
        monkeypatch.setattr(verify_tool.time, "sleep", sleep)
        verifier = verify_tool.SolanaRpcVerifier(
            "http://rpc.invalid", "DLMM_PROGRAM", max_cu_per_second=20
        )
        verifier._acquire_cu("getTransaction")
        verifier._acquire_cu("getTransaction")
        assert sleeps == [1.0, 2.0]

    def test_normalizes_transaction_and_filters_pool_swaps(self, verify_tool):
        calls = []
        pool = verify_tool._b58encode(bytes(range(32)))
        event_cpi = verify_tool._b58encode(
            b"eventcpi" + verify_tool._SWAP_EVENT_DISCRIMINATOR + bytes(range(32)) + b"payload"
        )

        def rpc(method, params):
            calls.append((method, params))
            if method == "getSignaturesForAddress":
                return [{"signature": "S1", "blockTime": 15, "err": None}]
            if method == "getTransaction":
                return {
                    "slot": 7,
                    "blockTime": 15,
                    "meta": {
                        "fee": 5000,
                        "logMessages": ["Program log: Instruction: Swap"],
                        "innerInstructions": [{
                            "index": 0,
                            "instructions": [{
                                "programId": "DLMM_PROGRAM",
                                "data": event_cpi,
                            }],
                        }],
                    },
                    "transaction": {
                        "message": {
                            "accountKeys": [
                                {"pubkey": "DLMM_PROGRAM"},
                                {"pubkey": pool},
                            ]
                        }
                    },
                }
            raise AssertionError(method)

        verifier = verify_tool.SolanaRpcVerifier(
            "http://rpc.invalid", "DLMM_PROGRAM", rpc=rpc
        )
        tx = verifier.get_transaction("S1")
        assert tx["program"] == "DLMM"
        assert tx["fee_lamports"] == 5000
        assert tx["slot"] == 7
        assert verifier.get_pool_signatures(pool, 10, 20) == ["S1"]
        assert any(method == "getSignaturesForAddress" for method, _ in calls)
