"""Recovery must verify ownership and stop after any ambiguous submission."""
from argparse import Namespace
from pathlib import Path

from dlmm_bot.exec_bridge import ExecResult


def recovery(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "tools"))
    from recover_live_lp import recover
    return recover


class Bridge:
    def __init__(self):
        self.positions = {pid: {"position_id": pid, "owner": "wallet", "pool": "pool"}
                          for pid in ("old-bid", "old-ask")}
        self.withdraws = []
        self.ambiguous = False

    def get_position(self, pid):
        if pid not in self.positions:
            return ExecResult(ok=False, error="unknown_position")
        return ExecResult(ok=True, data=self.positions[pid])

    def withdraw(self, pid, percent):
        self.withdraws.append((pid, percent))
        if self.ambiguous:
            return ExecResult(ok=False, error="submission_ambiguous", unknown_outcome=True)
        self.positions.pop(pid)
        return ExecResult(ok=True, position_id=pid,
                          data={"position_id": pid, "closed": True},
                          tx_signatures=[f"sig-{pid}"], fee_lamports=5000)


def args():
    return Namespace(wallet="wallet", pool="pool", position_ids=["old-bid", "old-ask"])


def test_recovery_closes_only_verified_positions(monkeypatch, tmp_path):
    bridge = Bridge()
    report = recovery(monkeypatch)(bridge, args(), tmp_path)
    assert report["cleanup_complete"] is True
    assert report["closed_position_ids"] == ["old-bid", "old-ask"]
    assert report["fee_lamports"] == 10_000
    assert bridge.withdraws == [("old-bid", 100), ("old-ask", 100)]


def test_recovery_refuses_foreign_position_before_any_write(monkeypatch, tmp_path):
    bridge = Bridge()
    bridge.positions["old-ask"]["owner"] = "other-wallet"
    report = recovery(monkeypatch)(bridge, args(), tmp_path)
    assert report["cleanup_complete"] is False
    assert bridge.withdraws == []


def test_recovery_never_retries_or_continues_after_ambiguous_withdraw(monkeypatch, tmp_path):
    bridge = Bridge()
    bridge.ambiguous = True
    report = recovery(monkeypatch)(bridge, args(), tmp_path)
    assert report["unresolved_outcome"] is True
    assert report["remaining_position_ids"] == ["old-bid", "old-ask"]
    assert bridge.withdraws == [("old-bid", 100)]


def test_recovery_skips_already_closed_id_without_skipping_next(monkeypatch, tmp_path):
    bridge = Bridge()
    bridge.positions.pop("old-bid")
    report = recovery(monkeypatch)(bridge, args(), tmp_path)
    assert report["cleanup_complete"] is True
    assert bridge.withdraws == [("old-ask", 100)]
