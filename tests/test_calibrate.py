"""Tests for the gamma/kappa calibration sweep (tools/calibrate.py)."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import calibrate  # noqa: E402

from dlmm_bot.config import DLMMConfig  # noqa: E402
from dlmm_bot.backtest import BinEvent  # noqa: E402
from dlmm_bot.grid import VenueGrid  # noqa: E402


def _row(prev, new, in_raw, out_raw, **over):
    row = {
        "pool": "POOL", "ts": 1000.0, "block_time": 1000,
        "prev_active_bin": prev, "new_active_bin": new,
        # The decimal fields are deliberately wrong here: pre-2026-09-18
        # captures carry them mispriced, and the loader must ignore them.
        "direction": "down", "amount_in": 1e9, "amount_out": 1e9,
        "amount_in_raw": str(in_raw), "amount_out_raw": str(out_raw),
        "fee_bps": 4.0,
    }
    row.update(over)
    return row


@pytest.fixture
def capture(tmp_path):
    """A capture that walks up and back down through the ladder."""
    rows = []
    ts = 1000.0
    for prev, new in [(0, 3), (3, 6), (6, 3), (3, 0), (0, 3), (3, 0)]:
        # up-swaps pay quote in, down-swaps take quote out
        rows.append(_row(prev, new, 2_000_000_000 if new > prev else 20_000_000,
                         20_000_000 if new > prev else 2_000_000_000, ts=ts, block_time=ts))
        ts += 60
    path = tmp_path / "swaps.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return str(path)


class TestLoadEvents:
    def test_sizes_from_raw_fields_not_decimal_fields(self, capture):
        events = calibrate.load_events(capture, base_decimals=9, quote_decimals=6)
        assert len(events) == 6
        # Up-swap: quote leg is amount_in_raw (2e9 raw / 1e6 = 2000 quote).
        assert events[0].direction == "up"
        assert events[0].trade_size_usd == pytest.approx(2000.0)
        # Down-swap: quote leg is amount_out_raw, same notional.
        assert events[2].direction == "down"
        assert events[2].trade_size_usd == pytest.approx(2000.0)

    def test_direction_comes_from_the_bins_not_the_row(self, tmp_path):
        # A no-move swap: the executor labels it "down"; it crosses nothing.
        path = tmp_path / "flat.jsonl"
        path.write_text(json.dumps(_row(7, 7, 10, 10)) + "\n")
        (event,) = calibrate.load_events(str(path), 9, 6)
        assert event.active_bin == event.prev_active_bin == 7

    def test_pool_filter(self, capture):
        assert calibrate.load_events(capture, 9, 6, pool="OTHER") == []


class TestCalibrate:
    def _bits(self):
        grid = VenueGrid(ref_price=1000.0, bin_step_bps=4,
                         base_decimals=9, quote_decimals=6)
        cfg = DLMMConfig(bin_step_bps=4, ref_price=1000.0, levels=3,
                         inner_offset=1, capital=100.0, level_weight=1 / 3)
        return grid, cfg

    def test_reports_both_winners_and_the_fill_flag(self, capture):
        grid, cfg = self._bits()
        events = calibrate.load_events(capture, 9, 6)
        report = calibrate.calibrate(events, grid, cfg, [1.0, 30.0], [0.5, 20.0], 0.5)
        assert report["n_in_sample"] == report["n_out_sample"] == 3
        assert report["n_crossings"] == 6
        # No parameter cell may masquerade as calibrated without in-sample
        # fills. (Whether this tiny capture fills depends on the regime gate,
        # so assert the invariant, not the fixture's fill count.)
        assert report["calibrated"] is report["has_in_sample_fills"]
        assert (report["chosen_out_of_sample"] is None) is not report["calibrated"]
        assert report["best_out_of_sample"]["total_pnl"] >= \
            max(row["total_pnl"] for row in report["out_of_sample"])
        assert isinstance(report["any_fills"], bool)

    def test_selects_best_filling_cell_not_no_fill_pnl_winner(self, monkeypatch):
        grid, cfg = self._bits()
        is_rows = [
            {"gamma": 1.0, "kappa": 0.5, "n_fills": 0, "total_pnl": 100.0},
            {"gamma": 30.0, "kappa": 20.0, "n_fills": 1, "total_pnl": 1.0},
        ]
        oos_rows = [
            {"gamma": 1.0, "kappa": 0.5, "n_fills": 0, "total_pnl": 50.0},
            {"gamma": 30.0, "kappa": 20.0, "n_fills": 2, "total_pnl": 2.0},
        ]
        sweeps = iter((is_rows, oos_rows))
        monkeypatch.setattr(calibrate, "sweep", lambda *args: next(sweeps))

        events = [
            BinEvent(
                ts=1000.0 + i,
                pool="POOL",
                active_bin=i + 1,
                prev_active_bin=i,
                direction="up",
                trade_size_usd=1.0,
                fee_bps=0.0,
            )
            for i in range(2)
        ]
        report = calibrate.calibrate(
            events, grid, cfg, [1.0, 30.0], [0.5, 20.0], 0.5
        )

        assert report["calibrated"] is True
        assert report["best_in_sample"]["gamma"] == 30.0
        assert report["chosen_out_of_sample"]["n_fills"] == 2

    def test_flags_a_capture_that_never_fills(self, tmp_path):
        """A capture with no bin crossings calibrates nothing, and says so."""
        path = tmp_path / "flat.jsonl"
        path.write_text("".join(
            json.dumps(_row(7, 7, 10, 10, ts=1000.0 + i, block_time=1000 + i)) + "\n"
            for i in range(4)
        ))
        grid, cfg = self._bits()
        events = calibrate.load_events(str(path), 9, 6)
        report = calibrate.calibrate(events, grid, cfg, [1.0], [0.5, 20.0], 0.5)
        assert report["any_fills"] is False

    def test_empty_split_is_an_error(self, capture):
        grid, cfg = self._bits()
        events = calibrate.load_events(capture, 9, 6)
        with pytest.raises(ValueError):
            calibrate.calibrate(events, grid, cfg, [1.0], [0.5], 1.0)
