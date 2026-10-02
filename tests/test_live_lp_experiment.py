"""Real-position experiment safety, without RPC or a signer."""

import importlib.util
import os
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest
from decimal import Decimal

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
        self.active_bin = 0
        self.address = lambda side, bins: side

    def get_state(self, pool):
        return ExecResult(ok=True, data={
            "active_bin": self.active_bin, "bin_step_bps": 4, "slot": 1,
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
        pid = self.address(side, kwargs["bin_ids"])
        token, decimals = ("base", 9) if side == "ask" else ("quote", 6)
        # Mirror the executor: the total must be an exact count of raw units.
        total = sum(Decimal(str(a)) for a in kwargs["amounts"]) * 10**decimals
        if total != total.to_integral_value():
            return ExecResult(ok=False, error="bad_request")
        raw = int(total)
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

    def withdraw(self, position_id, percent):
        self.calls.append(("withdraw", position_id, percent))
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
        "--duration-seconds", "0.05", "--refresh-interval", "0.001",
    ])


def accounts(_, legs):
    return {"native_lamports": 1_000_000_000, "rent_lamports": 60_000_000 * len(legs),
            "positions": [{"side": leg["side"], "position_id": leg["side"], "exists": False}
                          for leg in legs]}


def account_reader(bridge):
    def read(a, legs):
        return accounts(a, legs) | {"native_lamports": bridge.native,
            "positions": [{"side": leg["side"],
                           "position_id": bridge.address(leg["side"], leg["bin_ids"]),
                           "exists": False} for leg in legs]}
    return read


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


def test_opening_amounts_are_whole_raw_units_at_a_real_price(tmp_path):
    bridge = Bridge()
    bridge.active_bin = -5277  # live 2026-10-02: ~121.2 USDC/SOL, $5 of wSOL over 5 bins
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert report["error"] is None and report["cleanup_complete"] is True
    assert [c[0] for c in bridge.calls][:2] == ["deposit", "deposit"]


def test_underfunded_base_fails_preflight_naming_the_short_side(tmp_path):
    bridge = Bridge()
    bridge.wallet["base"] = 0  # e.g. wSOL unwrapped to native SOL
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert bridge.calls == []
    assert report["stop_reason"] == "preflight_failed"
    assert "base raw 0 <" in report["error"] and "quote" not in report["error"].split("(")[1]


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


class MovingBridge(Bridge):
    def __init__(self, rejects=1, side="bid", ambiguous=False):
        super().__init__()
        self.rejects, self.side, self.ambiguous = rejects, side, ambiguous
        self.address = lambda side, bins: f"{side}:{bins[0]}"

    def deposit_single_sided(self, **kwargs):
        if kwargs["side"] == self.side and self.rejects:
            self.calls.append(("rejected", kwargs))
            self.rejects -= 1
            self.active_bin += 1
            return ExecResult(ok=False, error="active_bin_slippage_exceeded")
        if self.ambiguous:
            self.calls.append(("ambiguous", kwargs))
            return ExecResult(ok=False, unknown_outcome=True, error="timeout")
        return super().deposit_single_sided(**kwargs)


def test_opening_replans_after_unsigned_drift_and_keeps_zero_tolerance(tmp_path):
    bridge = MovingBridge()
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert report["cleanup_complete"] is True
    assert report["error"] is None
    assert [c[0] for c in bridge.calls] == ["rejected", "deposit", "deposit", "withdraw", "withdraw"]
    assert bridge.calls[0][1]["expected_active_bin"] == 0
    assert bridge.calls[1][1]["expected_active_bin"] == 1
    assert bridge.calls[1][1]["bin_ids"] == [-4, -3, -2, -1, 0]
    assert all(c[1]["max_active_bin_slippage"] == 0 for c in bridge.calls[:3])
    assert len(ReplayLog(str(tmp_path / "run.jsonl")).by_type("opening_retry")) == 1
    assert "bid:-4" in report["expected_position_ids"]


def test_three_unsigned_rejections_stop_with_zero_experiment_pnl(tmp_path):
    bridge = MovingBridge(rejects=20)
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert len(bridge.calls) == 3
    assert all(c[0] == "rejected" for c in bridge.calls)
    assert report["cleanup_complete"] is True
    assert report["live_started"] is False
    assert report["fee_lamports"] == 0
    assert report["inventory_mark_pnl"] == 0
    assert report["idle_hold_pnl"] == 0
    assert report["pnl"]["total_pnl"] == 0


def test_ask_retry_exhaustion_closes_only_the_successful_bid(tmp_path):
    bridge = MovingBridge(rejects=20, side="ask")
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert [c[0] for c in bridge.calls] == ["deposit", "rejected", "rejected", "rejected", "withdraw"]
    assert report["cleanup_complete"] is True
    assert report["remaining_position_ids"] == []
    assert report["fee_lamports"] == 10000


def test_ambiguous_reply_after_unsigned_retry_stops_without_cleanup_writes(tmp_path):
    bridge = MovingBridge(ambiguous=True)
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert [c[0] for c in bridge.calls] == ["rejected", "ambiguous"]
    assert report["unresolved_outcome"] is True
    assert report["cleanup_complete"] is False
    assert report["remaining_position_ids"] == ["bid:-4"]


def test_pool_movement_during_account_preflight_is_replanned_before_first_deposit(tmp_path):
    bridge = MovingBridge(rejects=0)
    read = account_reader(bridge)
    first = True
    def delayed(a, legs):
        nonlocal first
        data = read(a, legs)
        if first:
            first = False
            bridge.active_bin = 2
        return data
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), delayed)
    assert report["error"] is None
    assert bridge.calls[0][0] == "deposit"
    assert bridge.calls[0][1]["expected_active_bin"] == 2
    assert bridge.calls[0][1]["bin_ids"] == [-3, -2, -1, 0, 1]
    assert report["cleanup_complete"] is True


def test_drift_error_with_receipt_evidence_is_never_retried(tmp_path):
    bridge = Bridge()
    bridge.deposit_single_sided = lambda **_: ExecResult(
        ok=False, error="active_bin_slippage_exceeded", tx_signatures=["maybe-landed"],
    )
    report = experiment.run_experiment(bridge, args(tmp_path), tmp_path, Event(), account_reader(bridge))
    assert report["unresolved_outcome"] is True
    assert report["cleanup_complete"] is False
    assert report["remaining_position_ids"] == ["bid"]
    assert not ReplayLog(str(tmp_path / "run.jsonl")).by_type("opening_retry")
