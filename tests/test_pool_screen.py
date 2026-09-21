"""Tests for the OHLCV pool screen (tools/pool_screen.py)."""

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import pool_screen  # noqa: E402


@pytest.fixture
def csv_path(tmp_path):
    """Two days of hourly bars, each spanning exactly 10 bins at 4 bps."""
    step = 1.0004
    rows = ["timestamp,datetime,open,high,low,close,volume,source"]
    price = 100.0
    for i in range(48):
        low = price
        high = price * step ** 10
        rows.append(f"{1_780_000_000 + i * 3600},d,{low},{high},{low},{high},1000,meteora")
        price = high
    (tmp_path / "bars.csv").write_text("\n".join(rows) + "\n")
    return str(tmp_path / "bars.csv")


class TestLoad:
    def test_sorts_oldest_first(self, tmp_path):
        p = tmp_path / "unsorted.csv"
        p.write_text(
            "timestamp,datetime,open,high,low,close,volume,source\n"
            "200,d,2,2,2,2,1,meteora\n"
            "100,d,1,1,1,1,1,meteora\n"
        )
        bars = pool_screen.load_bars(str(p))
        assert [b["ts"] for b in bars] == [100.0, 200.0]

    def test_bar_range_in_bins(self, csv_path):
        bars = pool_screen.load_bars(csv_path)
        ranges = pool_screen.bins_per_bar(bars, bin_step_bps=4)
        assert all(r == pytest.approx(10.0, abs=1e-6) for r in ranges)
        # A coarser grid puts the same price range in fewer bins.
        assert pool_screen.bins_per_bar(bars, bin_step_bps=20)[0] < 3.0


class TestScreen:
    def test_reports_travel_volume_and_fees(self, csv_path):
        s = pool_screen.screen(pool_screen.load_bars(csv_path), 4, fee_bps=4.0)
        assert s["n_bars"] == 48
        assert s["span_days"] == pytest.approx(47 / 24)
        assert s["bins_per_hour_median"] == pytest.approx(10.0, abs=1e-6)
        assert s["volume_per_hour_median"] == 1000.0
        assert s["pool_fees_per_hour_median"] == pytest.approx(1000.0 * 4 / 1e4)

    def test_sigma_is_annualized_off_the_hourly_interval(self, csv_path):
        """A steady 40 bps per hour is ~37% annualized, not ~0.7%."""
        s = pool_screen.screen(pool_screen.load_bars(csv_path), 4, fee_bps=4.0)
        # Constant drift has no dispersion, so use Parkinson (high/low based).
        assert s["sigma_annual_parkinson"] > 0.05


class TestInnerOffsets:
    def test_wider_kappa_tightens_the_ladder(self):
        table = pool_screen.inner_offsets(0.61, price=111.0, bin_step_bps=4)
        for g in pool_screen.GAMMAS:
            offsets = [table[(g, k)] for k in pool_screen.KAPPAS]
            assert offsets == sorted(offsets, reverse=True)

    def test_offsets_are_whole_bins_and_at_least_one(self):
        table = pool_screen.inner_offsets(0.61, price=111.0, bin_step_bps=4)
        assert all(isinstance(v, int) and v >= 1 for v in table.values())

    def test_sigma_widens_the_ladder(self):
        """The reason a mis-scaled sigma hid gamma: no sigma, no gamma effect."""
        calm = pool_screen.inner_offsets(0.01, 111.0, 4)
        wild = pool_screen.inner_offsets(0.61, 111.0, 4)
        assert wild[(100.0, 0.5)] > calm[(100.0, 0.5)]
        assert len(set(wild.values())) > len(set(calm.values()))


def test_main_runs_end_to_end(csv_path, capsys):
    assert pool_screen.main([csv_path, "--bin-step", "4", "--offsets"]) == 0
    out = capsys.readouterr().out
    assert "sigma annualized" in out
    assert "inner offset in bins" in out
