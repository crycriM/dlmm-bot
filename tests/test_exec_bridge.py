"""Tests for the exec bridge — JSON protocol to the TS Meteora executor."""

import pytest
from dlmm_bot.exec_bridge import ExecResult, ExecBridge, FakeExecBridge


class TestExecResult:
    def test_succeeded(self):
        assert ExecResult(ok=True).succeeded
        assert not ExecResult(ok=False).succeeded

    def test_tx_signatures_default(self):
        r = ExecResult(ok=True)
        assert r.tx_signatures == []


class TestFakeExecBridge:
    def setup_method(self):
        self.bridge = FakeExecBridge()
        self.bridge.start()

    def test_get_state_records_call(self):
        self.bridge.set_state("pool123", active_bin=100)
        result = self.bridge.get_state("pool123")
        assert result.ok
        assert result.data["active_bin"] == 100
        assert self.bridge.calls[0]["method"] == "get_state"
        assert self.bridge.calls[0]["pool"] == "pool123"

    def test_deposit_single_sided(self):
        result = self.bridge.deposit_single_sided(
            "pool", "bid", [100, 101, 102], [10, 20, 30], "Spot",
        )
        assert result.ok
        assert result.tx_signatures == ["fake_tx_001"]
        call = self.bridge.calls[0]
        assert call["method"] == "deposit_single_sided"
        assert call["side"] == "bid"
        assert call["bin_ids"] == [100, 101, 102]
        assert call["amounts"] == [10, 20, 30]

    def test_withdraw(self):
        result = self.bridge.withdraw("pos_001", bps=50)
        assert result.ok
        assert self.bridge.calls[0]["position_id"] == "pos_001"
        assert self.bridge.calls[0]["bps"] == 50

    def test_swap(self):
        result = self.bridge.swap(
            in_mint="So111", out_mint="EPjFWd", amount=1.5, max_slippage_bps=100,
        )
        assert result.ok
        call = self.bridge.calls[0]
        assert call["in_mint"] == "So111"
        assert call["amount"] == 1.5

    def test_refresh_bundle(self):
        result = self.bridge.refresh_bundle(
            "pos_001", {"in": "base", "out": "quote", "amount": 5.0},
            {"pool": "test", "bid_bins": [100, 101], "ask_bins": [102, 103]},
        )
        assert result.ok
        call = self.bridge.calls[0]
        assert call["withdraw_position_id"] == "pos_001"
        assert call["deposit_spec"]["pool"] == "test"

    def test_set_next_result_failure(self):
        self.bridge.set_next_result(False)
        result = self.bridge.get_state("pool")
        assert not result.ok
        assert result.error == "fake error"

    def test_stop_is_noop(self):
        self.bridge.stop()  # should not raise


class TestExecBridgeProtocol:
    """Test that ExecBridge constructs the right JSON for each verb."""

    def test_send_json_format(self, monkeypatch):
        """Verify _send produces valid JSON and parses the response."""
        import json

        bridge = ExecBridge(cmd=["echo"], timeout=1)

        captured_stdin = []
        class FakeStdin:
            def write(self, s): captured_stdin.append(s)
            def flush(self): pass
        class FakeStdout:
            def readline(self): return '{"ok": true, "data": {"active_bin": 50}}'
        class FakeStderr:
            def read(self): return ""
        class FakeProc:
            stdin = FakeStdin()
            stdout = FakeStdout()
            stderr = FakeStderr()
            def poll(self): return None

        bridge._proc = FakeProc()
        result = bridge._send({"method": "get_state", "pool": "test"})
        assert result.ok
        assert result.data["active_bin"] == 50
        sent = json.loads(captured_stdin[0])
        assert sent["method"] == "get_state"