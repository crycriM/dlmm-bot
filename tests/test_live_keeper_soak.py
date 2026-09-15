"""Offline safety and geometry checks for the opt-in mainnet read soak."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from unittest.mock import patch

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "live_keeper_soak.py"
SPEC = importlib.util.spec_from_file_location("live_keeper_soak", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
soak = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(soak)


def test_grid_uses_owned_position_price_and_live_pool_decimals():
    state = {
        "active_bin": 1000,
        "bin_step_bps": 25,
        "token_x": {"decimals": 9},
        "token_y": {"decimals": 6},
    }
    position = {"bins": [
        {"bin_id": 1001, "bin_price": 150.0},
        {"bin_id": 1100, "bin_price": 200.0},
    ]}
    grid = soak.grid_from_reads(state, position)
    assert grid.price_from_bin(1001) == pytest.approx(150.0)
    assert (grid.base_decimals, grid.quote_decimals) == (9, 6)


def test_executor_environment_drops_unrelated_credentials(tmp_path):
    with patch.dict(os.environ, {
        "PATH": "/usr/bin",
        "RUN_LIVE": "1",
        "SOLANA_RPC_URL": "https://rpc.test",
        "LIVE_POOL": "pool",
        "LIVE_BASE_MINT": "base",
        "LIVE_QUOTE_MINT": "quote",
        "HYPERLIQUID_PRIVATE_KEY": "must-not-pass",
    }, clear=True):
        soak.configure_executor(tmp_path)
        assert "HYPERLIQUID_PRIVATE_KEY" not in os.environ
        assert os.environ["DRY_RUN"] == "true"
        assert os.environ["MAX_SOL_PER_TX"] == "0"
        assert os.environ["JITO_ENABLED"] == "false"
        assert os.environ["POOL_ALLOWLIST"] == "pool"


@pytest.mark.parametrize("unsafe", [
    {"RUN_LIVE": "0"},
    {"DRY_RUN": "false"},
    {"LIVE_WRITE_CONFIRM": "yes"},
    {"WALLET_PRIVATE_KEY": "must-not-pass"},
])
def test_live_guard_rejects_unsafe_settings(tmp_path, monkeypatch, unsafe):
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / "bridge.js").touch()
    monkeypatch.setattr(soak, "EXECUTOR", tmp_path)
    env = {
        "RUN_LIVE": "1",
        "DRY_RUN": "true",
        "SOLANA_RPC_URL": "https://rpc.test",
        "LIVE_POOL": "pool",
        "LIVE_POSITION_ID": "position",
        "WALLET_PUBKEY": "wallet",
        "LIVE_BASE_MINT": "base",
        "LIVE_QUOTE_MINT": "quote",
    } | unsafe
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(ValueError):
            soak.validate_read_only()


@pytest.mark.parametrize("p95_ms, expected", [(1851, True), (2000, False)])
def test_read_only_gate_uses_strict_two_second_p95(tmp_path, monkeypatch, p95_ms, expected):
    keeper_dir = tmp_path / "keeper"
    keeper_dir.mkdir()
    (keeper_dir / "run.jsonl").touch()
    events = [
        {"event_type": "state_observation", "state": {"active_bin": 1}},
        {"event_type": "position_observation", "ok": True,
         "claimable_fee_x_raw": "0", "claimable_fee_y_raw": "0"},
        {"event_type": "decision", "action": "observation_only"},
    ]
    audit = [
        {"kind": "executor_started", "dry_run": True},
        {"kind": "verb", "method": "get_state", "duration_ms": p95_ms,
         "response": {"ok": True}},
        {"kind": "verb", "method": "get_position", "duration_ms": 10,
         "response": {"ok": True}},
    ]
    monkeypatch.setattr(soak, "ReplayLog", lambda _: type("Log", (), {"events": lambda self: events})())
    monkeypatch.setattr(soak, "executor_lines", lambda _: audit)

    summary = soak.summarize(tmp_path, 1800, 1800)

    assert summary["max_get_state_p95_ms"] == 2000
    assert summary["get_state_p95_ms"] == p95_ms
    assert summary["gate_pass"] is expected


def test_short_diagnostic_cannot_pass_soak_gate(tmp_path, monkeypatch):
    keeper_dir = tmp_path / "keeper"
    keeper_dir.mkdir()
    (keeper_dir / "run.jsonl").touch()
    monkeypatch.setattr(soak, "ReplayLog", lambda _: type("Log", (), {"events": lambda self: [
        {"event_type": "state_observation", "state": {}},
        {"event_type": "position_observation", "ok": True,
         "claimable_fee_x_raw": "0", "claimable_fee_y_raw": "0"},
        {"event_type": "decision", "action": "observation_only"},
    ]})())
    monkeypatch.setattr(soak, "executor_lines", lambda _: [
        {"kind": "executor_started", "dry_run": True},
        {"kind": "verb", "method": "get_state", "duration_ms": 100,
         "response": {"ok": True}},
    ])

    summary = soak.summarize(tmp_path, 60, 60)

    assert summary["minimum_seconds"] == 1800
    assert summary["gate_pass"] is False
