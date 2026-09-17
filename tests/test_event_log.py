"""EventLog, ReplayLog, converter, and config round-trip tests."""

from __future__ import annotations

import hashlib
import json

import pytest

from dlmm_bot.event_log import (
    EventLog,
    ReplayLog,
    _jsonable,
    load_events,
    to_bin_events,
)
from dlmm_bot.keeper import (
    dump_keeper_config,
    hash_keeper_config,
    rebuild_keeper_config,
)

ENVELOPE = {
    "event_type", "prev_hash", "run_id", "schema_version",
    "ts_wall", "config_hash", "cycle", "seq",
}


class TestEventLog:
    def test_monotonic_gap_free_seq(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path, run_id="r", config_hash="h") as log:
            seqs = [log.emit("e", i=i) for i in range(5)]
        assert seqs == [1, 2, 3, 4, 5]

    def test_envelope_and_payload(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path, run_id="r", config_hash="h") as log:
            log.set_cycle(3)
            log.emit("evt", foo="bar")
        rec = load_events(path)[0]
        assert set(ENVELOPE).issubset(rec.keys())
        assert rec["foo"] == "bar"
        assert rec["cycle"] == 3
        assert rec["seq"] == 1
        assert rec["prev_hash"] == ""
        assert rec["config_hash"] == "h"

    def test_hash_chain_links_previous_lines(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path, run_id="r", config_hash="h") as log:
            log.emit("a")
            log.emit("b")
            log.emit("c")
        lines = [l for l in open(path).read().splitlines() if l]
        recs = [json.loads(l) for l in lines]
        assert recs[0]["prev_hash"] == ""
        for i in range(1, len(recs)):
            expect = hashlib.sha256((lines[i - 1] + "\n").encode()).hexdigest()
            assert recs[i]["prev_hash"] == expect

    def test_nonempty_log_requires_explicit_resume(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path, run_id="r", config_hash="h") as log:
            log.emit("a")

        with pytest.raises(FileExistsError):
            EventLog(path, run_id="r", config_hash="h")

        with EventLog(path, run_id="r", config_hash="h", resume=True) as log:
            assert log.emit("b") == 2

        assert [e["seq"] for e in ReplayLog(path)] == [1, 2]

    def test_completed_run_cannot_be_resumed(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path, run_id="r", config_hash="h") as log:
            log.emit("run_started")
            log.emit("run_stopped", reason="stop")
        with pytest.raises(ValueError, match="completed"):
            EventLog(path, run_id="r", config_hash="h", resume=True)

    def test_payload_cannot_override_envelope(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path, run_id="r", config_hash="h") as log:
            log.emit("real", seq=99, run_id="spoofed")
        event = ReplayLog(path).events()[0]
        assert event["seq"] == 1
        assert event["run_id"] == "r"

    def test_jsonable_replaces_non_finite(self):
        assert _jsonable(float("inf")) is None
        assert _jsonable(float("-nan")) is None
        assert _jsonable(1.5) == 1.5
        assert _jsonable({"a": float("inf"), "b": [float("nan"), 2]}) == {
            "a": None, "b": [None, 2],
        }


class TestReplayLog:
    def test_detects_seq_gap(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path) as log:
            log.emit("a")
            log.emit("b")
        lines = open(path).read().splitlines()
        rec = json.loads(lines[1])
        rec["seq"] = 99
        open(path, "w").write(lines[0] + "\n" + json.dumps(rec, sort_keys=True) + "\n")
        with pytest.raises(ValueError):
            ReplayLog(path)

    def test_detects_hash_break(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path) as log:
            log.emit("a")
            log.emit("b")
        lines = open(path).read().splitlines()
        rec = json.loads(lines[0])
        rec["foo"] = "tampered"  # changes line 1, so line 2's prev_hash no longer matches
        open(path, "w").write(json.dumps(rec, sort_keys=True) + "\n" + lines[1] + "\n")
        with pytest.raises(ValueError):
            ReplayLog(path)

    def test_detects_missing_hash_link(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path) as log:
            log.emit("a")
            log.emit("b")
        lines = open(path).read().splitlines()
        second = json.loads(lines[1])
        second.pop("prev_hash")
        open(path, "w").write(
            lines[0] + "\n" + json.dumps(second, sort_keys=True, separators=(",", ":")) + "\n"
        )
        with pytest.raises(ValueError):
            ReplayLog(path)

    def test_by_type_and_run_started(self, tmp_path):
        path = str(tmp_path / "run.jsonl")
        with EventLog(path, run_id="r") as log:
            log.emit("run_started", pool_address="p")
            log.emit("state_observation", mid=1)
            log.emit("state_observation", mid=2)
        rep = ReplayLog(path)
        assert len(rep) == 3
        assert len(rep.by_type("state_observation")) == 2
        assert rep.run_started["pool_address"] == "p"


class TestToBinEvents:
    def test_swap_takes_precedence_over_poll(self):
        evs = [
            {"event_type": "run_started", "pool_address": "p"},
            {"event_type": "state_observation", "tvl_usd": 50000, "mid": 1.0, "ts": 1.0},
            {"event_type": "price_change", "prev_active_bin": 100, "new_active_bin": 103,
             "direction": "up", "ts_wall": 2.0},
            {"event_type": "observed_trade", "prev_active_bin": 100, "new_active_bin": 103,
             "direction": "up", "fee_bps": 25.0, "ts": 2.5, "trade_size_usd": 100.0},
        ]
        bts = to_bin_events(evs)
        moves = [(b.prev_active_bin, b.active_bin) for b in bts]
        # the up 100->103 crossing appears exactly once (swap wins the dedupe)
        assert moves.count((100, 103)) == 1
        b = next(b for b in bts if (b.prev_active_bin, b.active_bin) == (100, 103))
        assert b.trade_size_usd == 100.0

    def test_poll_only_is_not_converted_to_a_trade(self):
        evs = [
            {"event_type": "run_started", "pool_address": "p"},
            {"event_type": "state_observation", "tvl_usd": 42, "ts": 1.0},
            {"event_type": "price_change", "prev_active_bin": 100, "new_active_bin": 101,
             "direction": "up", "ts_wall": 2.0},
        ]
        bts = to_bin_events(evs)
        assert bts == []


class TestConfigRoundTrip:
    def test_dump_rebuild_preserves_hash(self, cfg):
        h1 = hash_keeper_config(cfg)
        dump = json.loads(json.dumps(dump_keeper_config(cfg), sort_keys=True))
        rebuilt = rebuild_keeper_config(dump)
        assert hash_keeper_config(rebuilt) == h1
        assert rebuilt.dlmm.gamma == cfg.dlmm.gamma
        assert rebuilt.grid.ref_price == cfg.grid.ref_price
        assert rebuilt.dry_run == cfg.dry_run
