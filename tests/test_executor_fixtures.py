"""Canonical TS response envelopes, consumed by the production Python parser."""

import json
from pathlib import Path

import pytest

from dlmm_bot.exec_bridge import ExecResult

FIXTURES = Path(__file__).resolve().parents[2] / "solana-clmm-executor" / "fixtures"
VERBS = ("get_state", "get_position", "deposit_single_sided", "withdraw", "swap", "refresh_bundle")
pytestmark = pytest.mark.skipif(not FIXTURES.is_dir(), reason="sibling executor fixtures not installed")


@pytest.mark.parametrize("verb", VERBS)
@pytest.mark.parametrize("outcome", ("ok", "error"))
def test_executor_response_fixture(verb, outcome):
    payload = json.loads((FIXTURES / "responses" / f"{verb}.{outcome}.json").read_text())
    result = ExecResult.from_payload(payload)
    assert result.ok is (outcome == "ok")
    assert result.error == payload["error"]
    assert result.data == payload["data"]
    assert set(payload) <= {"ok", "data", "error", "tx_signatures", "transactions", "position_id"}
    assert result.tx_signatures == payload["tx_signatures"]
    assert len(result.tx_receipts) == len(payload["transactions"]) == len(result.tx_signatures)
    for signature, receipt, parsed in zip(result.tx_signatures, payload["transactions"], result.tx_receipts):
        assert receipt["signature"] == parsed["signature"] == signature
        for field in ("slot", "block_time", "fee_lamports", "compute_unit_price"):
            assert receipt[field] is not None
            assert parsed[field] == receipt[field]
        assert receipt["status"] in ("confirmed", "finalized")
    if payload["transactions"]:
        assert result.total_fee_lamports == sum(r["fee_lamports"] for r in payload["transactions"])
        assert result.slot == payload["transactions"][-1]["slot"]
    if outcome == "ok" and verb in ("deposit_single_sided", "refresh_bundle"):
        assert result.position_id == payload["data"]["position_id"]
        assert result.position_id
    if outcome == "ok" and verb == "get_state":
        assert all(isinstance(raw, str) for raw in result.data["balances_raw"].values())
    if outcome == "ok" and verb == "get_position":
        assert isinstance(result.data["claimable_fee_x_raw"], str)
        assert isinstance(result.data["claimable_fee_y_raw"], str)
