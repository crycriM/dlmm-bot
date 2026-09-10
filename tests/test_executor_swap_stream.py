"""M3 gate 3 (spec §6): the TS swap stream feeds the Python observer.

The executor writes decoded swaps to ``SWAP_STREAM_PATH``; ``dlmm_bot``'s
``JsonlSwapEventSource`` tails that same file and ``SwapObserver`` turns each
row into ``observed_trade`` / ``bin_fill`` events. This test drives the real
``dist/bridge.js`` to produce the file, then reads it with the real Python
reader — the seam spec §6 exists to close.

Rows are produced through the offline fixture entrypoint, so no RPC, websocket,
or mainnet access is involved; what is exercised is the file format and the
field mapping across the language boundary.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from dlmm_bot.event_log import EventLog, load_events
from dlmm_bot.grid import VenueGrid
from dlmm_bot.swap_observer import JsonlSwapEventSource, SwapObserver, SwapStreamRunner

EXECUTOR_DIR = Path(__file__).resolve().parents[2] / "solana-clmm-executor"
FIXTURE = EXECUTOR_DIR / "fixtures" / "rpc" / "dlmm-swap-logs.json"


def _build_rows(out_path: Path) -> list[dict]:
    """Ask the built TS executor to decode recorded DLMM logs into rows."""
    script = EXECUTOR_DIR / "fixtures" / "swap-stream-dump.mjs"
    result = subprocess.run(
        [shutil.which("node"), str(script), str(FIXTURE), str(out_path)],
        capture_output=True, text=True, timeout=120, cwd=str(EXECUTOR_DIR),
    )
    if result.returncode != 0:
        pytest.fail(f"TS swap-stream dump failed: {result.stderr[-2000:]}")
    rows = json.loads(result.stdout)
    persisted = [json.loads(line) for line in out_path.read_text().splitlines() if line]
    assert persisted == rows
    return rows


@pytest.fixture(scope="module")
def ts_stream_file(tmp_path_factory):
    if not FIXTURE.is_file():
        pytest.skip("dlmm-swap-logs.json fixture missing")
    if shutil.which("node") is None or not (EXECUTOR_DIR / "dist" / "bridge.js").is_file():
        pytest.skip("node and a built solana-clmm-executor/dist are required")
    path = tmp_path_factory.mktemp("m3-executor") / "swaps.jsonl"
    _build_rows(path)
    return path


@pytest.fixture(scope="module")
def ts_rows(ts_stream_file):
    return [json.loads(line) for line in ts_stream_file.read_text().splitlines() if line]


def test_executor_produces_rows_with_the_observed_contract(ts_rows):
    """Every row carries the fields SwapObserver reads by name (spec §6)."""
    assert ts_rows, "the TS stream must decode at least one swap"
    for row in ts_rows:
        assert row["tx_signature"], "tx_signature is the dedupe key; never synthesized"
        assert isinstance(row["slot"], int)
        assert isinstance(row["block_time"], int), "block_time must never be null"
        assert isinstance(row["prev_active_bin"], int)
        assert isinstance(row["new_active_bin"], int)
        assert row["pool"]
        # Raw u64s are strings: they exceed JS safe-integer range (spec §5).
        for key in ("amount_in_raw", "amount_out_raw"):
            if key in row:
                assert isinstance(row[key], str) and row[key].isdigit()


def test_python_reader_tails_the_executor_file(ts_rows, ts_stream_file):
    """JsonlSwapEventSource reads what the TS writer appended, byte for byte."""
    source = JsonlSwapEventSource(str(ts_stream_file))
    pool = ts_rows[0]["pool"]
    read = _drain(source, pool)
    assert [r["tx_signature"] for r in read] == [r["tx_signature"] for r in ts_rows]
    # A second pass returns nothing: the byte offset advanced, no double count.
    assert _drain(source, pool) == []


def test_observer_emits_observed_trade_and_bin_fill(ts_rows, tmp_path):
    """The keeper's fill/markout pipeline is unblocked by the executor's rows."""
    path = tmp_path / "swaps.jsonl"
    with open(path, "w", encoding="utf-8") as fh:
        for row in ts_rows:
            fh.write(json.dumps(row) + "\n")

    grid = VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=9, quote_decimals=6)
    log = EventLog(str(tmp_path / "keeper.jsonl"), run_id="m3", config_hash="h")
    observer = SwapObserver(log, grid, ts_rows[0]["pool"])
    observer.set_last_mid(150.0, 0.0)

    # Rest liquidity on a bin the recorded swaps actually cross.
    crossed = _crossed_bins(ts_rows[0])
    if crossed:
        observer.register_ladder({crossed[0]: 1.0}, position_id="PID")

    runner = SwapStreamRunner(observer, JsonlSwapEventSource(str(path)), retry_delay=0.0)
    _run(runner)
    log.close()

    events = load_events(log.path)
    trades = [e for e in events if e["event_type"] == "observed_trade"]
    assert len(trades) == len(ts_rows)
    for row, event in zip(ts_rows, trades):
        assert event["tx_signature"] == row["tx_signature"]
        assert event["prev_active_bin"] == row["prev_active_bin"]
        assert event["new_active_bin"] == row["new_active_bin"]
        assert event["source"] == "swap_stream"
    # bin_fill is what feeds fee accrual; it must be produced by the same rule
    # the backtester uses, not by the executor guessing.
    if crossed:
        fills = [e for e in events if e["event_type"] == "bin_fill"]
        assert fills and fills[0]["bin_id"] == crossed[0]
        assert fills[0]["tx_signature"] == ts_rows[0]["tx_signature"]


def test_dedupe_across_backfill_overlap(ts_rows, tmp_path):
    """Spec §6: overlap is free because the observer dedupes by signature."""
    path = tmp_path / "swaps.jsonl"
    rows = ts_rows + ts_rows  # a reconnect replays what the tail already saw
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")

    grid = VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=9, quote_decimals=6)
    log = EventLog(str(tmp_path / "keeper.jsonl"), run_id="m3", config_hash="h")
    observer = SwapObserver(log, grid, rows[0]["pool"])
    runner = SwapStreamRunner(observer, JsonlSwapEventSource(str(path)), retry_delay=0.0)
    _run(runner)
    log.close()

    trades = [e for e in load_events(log.path) if e["event_type"] == "observed_trade"]
    assert len(trades) == len(ts_rows), "duplicate rows must not duplicate trades"


def test_verify_log_completeness_is_green(ts_rows, ts_stream_file, tmp_path, verify_tool):
    """Gate 3: the fixture's complete chain swap set matches the keeper log."""
    pool = ts_rows[0]["pool"]
    log = EventLog(str(tmp_path / "verified.jsonl"), run_id="m3", config_hash="h")
    log.emit("run_started", pool_address=pool)
    observer = SwapObserver(
        log,
        VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=9, quote_decimals=6),
        pool,
    )
    _run(SwapStreamRunner(observer, JsonlSwapEventSource(str(ts_stream_file)), retry_delay=0.0))
    log.emit("run_stopped", reason="fixture_complete")
    log.close()

    class FixtureChain:
        def get_transaction(self, signature):
            return {"program": "DLMM", "bin_fills": []}

        def get_pool_signatures(self, requested_pool, since, until):
            assert requested_pool == pool
            return [row["tx_signature"] for row in ts_rows]

    report = verify_tool.verify_run(log.path, chain=FixtureChain())
    assert report.ok, report.to_dict()
    assert report.missing_swaps == []
    assert report.extra_swaps == []


def _crossed_bins(row: dict) -> list[int]:
    prev, new = row["prev_active_bin"], row["new_active_bin"]
    return list(range(prev, new)) if new > prev else list(range(new + 1, prev + 1))


def _drain(source: JsonlSwapEventSource, pool: str) -> list[dict]:
    import asyncio

    return asyncio.run(source.backfill(pool, None))


def _run(runner: SwapStreamRunner) -> None:
    """Run the stream until the file is drained, then cancel (no live tail)."""
    import asyncio

    async def once():
        await runner._backfill()

    asyncio.run(once())
