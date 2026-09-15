"""Gate 1: built bridge with explicitly injected offline handlers."""

import asyncio
import json
from pathlib import Path

import pytest

from dlmm_bot.event_log import ReplayLog
from dlmm_bot.keeper import Keeper
from dlmm_bot.ladder import LadderLevel

pytestmark = pytest.mark.executor_subprocess
FIXTURES = Path(__file__).resolve().parents[2] / "solana-clmm-executor" / "fixtures"


def test_all_verbs_match_fixtures_over_real_cli(executor_cli_bridge):
    requests = json.loads((FIXTURES / "requests.json").read_text())
    for verb, request in requests.items():
        response = executor_cli_bridge._send(request)
        expected = json.loads((FIXTURES / "responses" / f"{verb}.ok.json").read_text())
        assert response.ok
        assert response.data.pop("stub") is True
        assert response.data == expected["data"]
        assert response.tx_signatures == expected["tx_signatures"]
        if expected["transactions"]:
            assert response.total_fee_lamports == sum(tx["fee_lamports"] for tx in expected["transactions"])


def test_execbridge_restarts_after_child_exit(executor_cli_bridge):
    bridge = executor_cli_bridge
    assert bridge.get_state("test_pool").ok
    previous = bridge._proc
    previous.terminate()
    previous.wait(timeout=5)
    assert bridge.get_state("test_pool").ok
    assert bridge._proc.pid != previous.pid
    for stream in (previous.stdin, previous.stdout, previous.stderr):
        stream.close()


def test_keeper_position_lifecycle_and_receipt_logs(cfg, executor_cli_bridge, tmp_path):
    cfg.dry_run = False  # Keeper sends writes; executor independently stays DRY_RUN=true.
    cfg.log_dir = str(tmp_path / "keeper-logs")
    cfg.base_mint = "So11111111111111111111111111111111111111112"
    cfg.quote_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    keeper = Keeper(cfg, executor_cli_bridge)
    # `_deposit_ladder` is normally entered after `_cycle` has observed state.
    keeper._active_bin = 100
    ladder = [LadderLevel(bin_id=98, side="bid", size=75), LadderLevel(bin_id=102, side="ask", size=0.5)]

    async def lifecycle():
        await keeper._deposit_ladder(ladder)
        assert keeper._current_position_id == "stub_position_001"
        # Observe through the keeper without its empty-history risk decision
        # withdrawing the position before the explicit refresh below.
        cfg.dry_run = True
        await keeper._cycle()
        cfg.dry_run = False
        await keeper._refresh_ladder(ladder)
        assert keeper._current_position_id == "stub_position_001"
        keeper._inventory_base = 1
        await keeper._emergency_exit()
        assert keeper._current_position_id is None
        assert keeper._halted

    try:
        asyncio.run(lifecycle())
    finally:
        keeper.stop()
    paths = list((tmp_path / "keeper-logs").glob("*.jsonl"))
    events = ReplayLog(str(paths[0])).events()
    results = [e for e in events if e["event_type"] == "action_result"]
    requests = [e for e in events if e["event_type"] == "action_request"]
    assert {e["verb"] for e in results} >= {"deposit_single_sided", "refresh_bundle", "withdraw", "swap"}
    assert all(e["ok"] and e["data"]["stub"] for e in results)
    assert all(e["transactions"] and all(tx["fee_lamports"] is not None for tx in e["transactions"]) for e in results)
    for event in requests:
        if event["verb"] == "deposit_single_sided":
            assert event["payload"]["expected_active_bin"] == 100
            assert event["payload"]["max_active_bin_slippage"] == 0
        if event["verb"] == "refresh_bundle":
            assert event["payload"]["deposit_spec"]["expected_active_bin"] == 100
            assert event["payload"]["deposit_spec"]["max_active_bin_slippage"] == 0
    assert any(e["event_type"] == "position_observation" and e["data"]["claimable_fee_x_raw"] == "3000000" for e in events)
    gas = [e for e in events if e["event_type"] == "cash_flow" and e.get("actual_fee")]
    assert gas
