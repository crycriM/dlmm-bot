"""Real-position experiment safety, without RPC or a signer."""

import importlib.util
import os
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest

from dlmm_bot.event_log import ReplayLog
from dlmm_bot.exec_bridge import ExecResult


SPEC = importlib.util.spec_from_file_location(
    "live_lp_experiment", Path(__file__).parents[1] / "tools/live_lp_experiment.py",
)
experiment = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(experiment)


class Bridge:
    def __init__(self, failure=None):
        self.calls = []
        self.positions = {}
        self.wallet = {"base": 1_000_000_000, "quote": 20_000_000}
        self.failure = failure
        self.native = 1_000_000_000

    def get_state(self, pool):
        return ExecResult(ok=True, data={
            "active_bin": 0, "bin_step_bps": 4, "slot": 1,
            "fetched_at": experiment.time.time(),
            "token_x": {"mint": "base", "decimals": 9},
            "token_y": {"mint": "quote", "decimals": 6},
            "balances_raw": {k: str(v) for k, v in self.wallet.items()},
        })

    def get_position(self, pid):
        if pid not in self.positions:
            return ExecResult(ok=False, error="unknown_position")
        return ExecResult(ok=True, data=self.positions[pid])

    def deposit_single_sided(self, **kwargs):
        self.calls.append(("deposit", kwargs))
        side = kwargs["side"]
        pid = side
        token, decimals = ("base", 9) if side == "ask" else ("quote", 6)
        raw = round(sum(kwargs["amounts"]) * 10**decimals)
        self.wallet[token] -= raw
        self.positions[pid] = {
            "position_id": pid, "pool": "pool", "owner": "wallet", "slot": 1,
            "total_base": raw / 1e9 if side == "ask" else 0,
            "total_quote": raw / 1e6 if side == "bid" else 0,
            "claimable_fee_x_raw": "0", "claimable_fee_y_raw": "0",
            "bins": [{"amount_base_raw": str(raw if side == "ask" else 0),
                      "amount_quote_raw": str(raw if side == "bid" else 0)}],
        }
        if self.failure == side:
            return ExecResult(ok=False, unknown_outcome=True, error="timeout")
        self.native -= 5000
        return ExecResult(ok=True, position_id=pid, fee_lamports=5000,
                          tx_signatures=[f"deposit-{side}"])

    def withdraw(self, position_id, bps):
        self.calls.append(("withdraw", position_id, bps))
        pos = self.positions.pop(position_id)
        self.native -= 5000
        self.wallet["base"] += round(pos["total_base"] * 1e9)
        self.wallet["quote"] += round(pos["total_quote"] * 1e6)
        self.wallet["base"] += int(pos["claimable_fee_x_raw"])
        self.wallet["quote"] += int(pos["claimable_fee_y_raw"])
        return ExecResult(ok=True, position_id=position_id, fee_lamports=5000,
                          tx_signatures=[f"withdraw-{position_id}"],
                          data={"closed": True, "fees_claimed": {
                              "x_raw": pos["claimable_fee_x_raw"], "y_raw": pos["claimable_fee_y_raw"]}})


def args(tmp_path):
    return experiment.build_args([
        "--pool", "pool", "--base-mint", "base", "--quote-mint", "quote",
        "--wallet", "wallet", "--out", str(tmp_path), "--live",
        "--duration-seconds", "0.02", "--refresh-interval", "0.001",
    ])


def accounts(*_):
    return {"native_lamports": 1_000_000_000, "rent_lamports": 120_000_000,
            "positions": [{"side": "bid", "position_id": "bid", "exists": False},
                          {"side": "ask", "position_id": "ask", "exists": False}]}


def account_reader(bridge):
    return lambda *a: accounts(*a) | {"native_lamports": bridge.native}


@pytest.fixture(autouse=True)
def cap(monkeypatch):
    monkeypatch.setenv("MAX_SOL_PER_RUN", "0.25")


def test_real_run_opens_two_closes_both_and_records_receipt_costs(tmp_path):
    bridge = Bridge()
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert report["cleanup_complete"] is True
    assert report["stop_reason"] == "duration_elapsed"
    assert report["fee_lamports"] == 20_000
    assert report["pnl"]["total_pnl"] == pytest.approx(-0.02)
    assert [c[0] for c in bridge.calls] == ["deposit", "deposit", "withdraw", "withdraw"]
    log = ReplayLog(str(tmp_path / "run.jsonl"))
    assert len(log.by_type("action_request")) == 4
    assert log.by_type("experiment_plan")[0]["positions"][0]["position_id"] == "bid"
    assert log.events()[-1]["event_type"] == "run_stopped"


def test_ambiguous_deposit_preserves_known_addresses_and_never_retries(tmp_path):
    bridge = Bridge(failure="ask")
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert report["cleanup_complete"] is False
    assert report["unresolved_outcome"] is True
    assert report["remaining_position_ids"] == ["bid", "ask"]
    assert [c[0] for c in bridge.calls] == ["deposit", "deposit"]


def test_existing_pda_fails_before_any_mutation(tmp_path):
    bridge = Bridge()
    def occupied(*a):
        data = accounts(*a)
        data["positions"][0]["exists"] = True
        return data
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), occupied)
    assert bridge.calls == []
    assert report["stop_reason"] == "preflight_failed"
    assert report["cleanup_complete"] is False  # never claim the existing PDA was closed


def test_preflight_only_never_calls_a_write_verb(tmp_path):
    a = args(tmp_path)
    a.live = False
    bridge = Bridge()
    report = experiment.run_experiment(bridge, a, tmp_path, Event(), accounts)
    assert bridge.calls == []
    assert report["preflight_pass"] is True
    assert report["live_started"] is False


@pytest.mark.parametrize("override", [
    {"DRY_RUN": "true"}, {"LIVE_WRITE_CONFIRM": ""},
    {"MAX_SOL_PER_RUN": "10"}, {"JITO_ENABLED": "true"},
    {"MAX_PRIORITY_FEE_LAMPORTS": "10001"},
])
def test_live_environment_fails_closed(tmp_path, override):
    env = {"SOLANA_RPC_URL": "https://rpc.test", "WALLET_PUBKEY": "wallet",
           "LIVE_WRITE_CONFIRM": "yes", "DRY_RUN": "false",
           "MAX_SOL_PER_TX": "0.23", "MAX_SOL_PER_RUN": "0.25",
           "MAX_SLIPPAGE_BPS": "25", "MAX_ACTIVE_BIN_SLIPPAGE_BINS": "0",
           "MAX_PRIORITY_FEE_LAMPORTS": "10000", "JITO_ENABLED": "false"} | override
    with patch.dict(os.environ, env, clear=True), pytest.raises(ValueError):
        experiment.validate_environment(args(tmp_path))


def test_snapshot_accounts_real_in_bin_conversion_and_claimable_fees(tmp_path):
    bridge = Bridge()
    original = bridge.deposit_single_sided
    def deposit(**kwargs):
        result = original(**kwargs)
        if kwargs["side"] == "ask":
            pos = bridge.positions["bid"]
            pos["bins"] = [{"amount_base_raw": "1000000", "amount_quote_raw": "4000000"}]
            pos["claimable_fee_y_raw"] = "10000"
            pos["total_base"], pos["total_quote"] = 0.001, 4
        return result
    bridge.deposit_single_sided = deposit
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    snapshots = ReplayLog(str(tmp_path / "run.jsonl")).by_type("custody_snapshot")
    assert snapshots[0]["principal_base"] == pytest.approx(0.006)
    assert snapshots[0]["principal_quote"] == pytest.approx(4)
    assert snapshots[0]["claimable_fee_quote"] == pytest.approx(0.01)
    assert snapshots[0]["pnl"]["lp_fee_income"] == pytest.approx(0.01)
    assert report["pnl"]["lp_fee_income"] == pytest.approx(0.01)


def test_loss_trigger_closes_both_positions_early(tmp_path):
    bridge = Bridge()
    original = bridge.deposit_single_sided
    def deposit(**kwargs):
        result = original(**kwargs)
        if kwargs["side"] == "ask":
            pos = bridge.positions["bid"]
            pos["total_quote"] = 3
            pos["bins"][0]["amount_quote_raw"] = "3000000"
        return result
    bridge.deposit_single_sided = deposit
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert report["stop_reason"] == "loss_limit"
    assert report["cleanup_complete"] is True
    assert report["pnl"]["total_pnl"] == pytest.approx(-2.02)


def test_protocol_ambiguous_error_without_transport_flag_stops_all_writes(tmp_path):
    bridge = Bridge()
    bridge.deposit_single_sided = lambda **_: ExecResult(ok=False, error="submission_ambiguous")
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert report["unresolved_outcome"] is True
    assert report["cleanup_complete"] is False
    assert report["remaining_position_ids"] == ["bid"]
    assert bridge.calls == []
