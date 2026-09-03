"""explain_run: pure-fold PnL breakdown over a run log (plan §5).

Events are synthetic in-memory dicts (no JSONL file) — explain_run is a pure
function of the folded facts, so this isolates the accounting math.
"""

from __future__ import annotations

import pytest

from dlmm_bot.pnl_explain import explain_run


def _ev(event_type: str, ts: float, **kw) -> dict:
    return {"event_type": event_type, "ts": ts, **kw}

ZERO = 1e-12


class TestBare:
    def test_empty_log(self):
        r = explain_run([])
        assert r.n_events == 0
        assert r.total_pnl == 0.0
        assert r.il_pct is None
        assert r.inventory_trace == []
        assert r.final_inventory is None
        assert r.avg_markout_bps_30s is None

    def test_counts_track_event_types(self):
        evs = [
            _ev("state_observation", 1.0, mid=150.0),
            _ev("observed_trade", 2.0, tx_signature="S1"),
            _ev("bin_fill", 2.0, side_filled="buy", bin_price=148.0,
                amount_base=1.0, mid_at_fill=150.0, tx_signature="S1"),
            _ev("action_result", 3.0, verb="refresh_bundle", ok=True,
                fee_lamports=50000, tx_signatures=["T"]),
        ]
        r = explain_run(evs)
        assert r.n_events == 4
        assert r.n_trades == 1
        assert r.n_fills == 1
        assert r.n_actions == 1


class TestLedger:
    def test_round_trip_spread_capture(self):
        # buy 1 @ 148 (mid 150), sell 1 @ 152 (mid 150):
        # realized = 1*(152-148) = 4; spread = +2 (buy) + +2 (sell) = 4;
        # markout = trading - spread = 0; position flat → unrealized 0.
        evs = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("bin_fill", 1.0, side_filled="buy", bin_price=148.0,
                amount_base=1.0, mid_at_fill=150.0),
            _ev("bin_fill", 2.0, side_filled="sell", bin_price=152.0,
                amount_base=1.0, mid_at_fill=150.0),
            _ev("state_observation", 3.0, mid=150.0),
        ]
        r = explain_run(evs)
        assert abs(r.realized_pnl - 4.0) < ZERO
        assert abs(r.spread_capture - 4.0) < ZERO
        assert abs(r.markout_pnl) < ZERO
        assert r.unrealized_pnl == 0.0
        assert abs(r.total_pnl - 4.0) < ZERO

    def test_unrealized_marks_open_position(self):
        evs = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("bin_fill", 1.0, side_filled="buy", bin_price=148.0,
                amount_base=1.0, mid_at_fill=150.0),
            _ev("state_observation", 2.0, mid=155.0),
        ]
        r = explain_run(evs)
        assert abs(r.realized_pnl) < ZERO
        assert abs(r.unrealized_pnl - (155.0 - 148.0) * 1.0) < ZERO
        assert abs(r.total_pnl - r.trading_pnl) < ZERO


class TestInventory:
    def _open_events(self):
        # ask side: 2.0 base @ 180; bid side: 3.0 quote-units @ 100 (= 300 quote)
        return [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("position_created", 0.5, position_id="P",
                bins=[{"bin_id": 101, "side": "ask", "amount": 2.0, "price": 180.0},
                      {"bin_id": 99, "side": "bid", "amount": 3.0, "price": 100.0}]),
        ]

    def test_deposit_not_double_counted_in_trace(self):
        r = explain_run(self._open_events())
        # the deposit appears exactly once — no pre-seeded initial balance
        assert len(r.inventory_trace) == 1
        entry = r.inventory_trace[0]
        assert (entry["base"], entry["quote"]) == (2.0, 300.0)
        assert (r.final_inventory["base"], r.final_inventory["quote"]) == (2.0, 300.0)

    def test_fill_updates_trace_and_il(self):
        evs = self._open_events() + [
            _ev("bin_fill", 1.0, side_filled="buy", bin_price=148.0,
                amount_base=1.0, mid_at_fill=150.0),
            _ev("state_observation", 2.0, mid=150.0),
        ]
        r = explain_run(evs)
        assert len(r.inventory_trace) == 2
        last = r.inventory_trace[-1]
        assert (last["base"], last["quote"]) == (3.0, 152.0)
        # IL vs the immutable 0.5-share benchmark (constant initial balances,
        # valued at the final mid): benchmark = 2*150 + 300 = 600,
        # final = 3*150 + 152 = 602.
        assert abs(r.il_pct - (602.0 / 600.0 - 1.0) * 100.0) < 1e-9

    def test_withdraw_resets_inventory(self):
        evs = self._open_events() + [
            _ev("position_withdrawn", 1.0, position_id="P", bps=100,
                tx_signatures=["T"]),
        ]
        r = explain_run(evs)
        assert (r.final_inventory["base"], r.final_inventory["quote"]) == (0.0, 0.0)
        assert (r.inventory_trace[-1]["base"], r.inventory_trace[-1]["quote"]) == (0.0, 0.0)


class TestFees:
    def _fee_events(self, claimed_x: float, claimed_y: float):
        return [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("position_observation", 1.0, position_id="P",
                data={"claimable_fee_x": 0.0, "claimable_fee_y": 1.0}),
            _ev("position_observation", 2.0, position_id="P",
                data={"claimable_fee_x": 0.2, "claimable_fee_y": 1.5}),
            _ev("position_closed", 3.0, position_id="P",
                fees_claimed={"x": claimed_x, "y": claimed_y}),
            _ev("state_observation", 4.0, mid=150.0),
        ]

    def test_accrual_basis_and_divergence_flag(self):
        r = explain_run(self._fee_events(0.1, 0.2))
        # accrued = 0.2*150 + 0.5 = 30.5 ; claimed = 0.1*150 + 0.2 = 15.2
        assert abs(r.lp_fee_accrued - 30.5) < ZERO
        assert abs(r.lp_fee_claimed - 15.2) < ZERO
        assert r.fee_divergence_flagged
        assert any("lp_fee divergence" in f for f in r.flags)

    def test_no_flag_when_claimed_covers_accrued(self):
        r = explain_run(self._fee_events(0.2, 0.5))
        assert abs(r.lp_fee_claimed - 30.5) < ZERO
        assert abs(r.lp_fee_divergence) < 1e-6
        assert not r.fee_divergence_flagged
        assert r.flags == []
        assert abs(r.total_pnl - 30.5) < ZERO

    def test_first_observation_accrues_from_position_creation(self):
        evs = [
            _ev("run_started", 0.0, base_decimals=6, quote_decimals=6),
            _ev("state_observation", 0.0, mid=100.0),
            _ev("position_created", 0.5, position_id="P", bins=[]),
            _ev("position_observation", 1.0, position_id="P",
                claimable_fee_x_raw=100_000, claimable_fee_y_raw=1_000_000),
        ]
        r = explain_run(evs)
        assert r.lp_fee_accrued == pytest.approx(11.0)
        assert r.total_pnl == pytest.approx(11.0)

    def test_claims_without_observations_do_not_raise_false_divergence(self):
        evs = [
            _ev("state_observation", 0.0, mid=100.0),
            _ev("position_closed", 1.0, position_id="P",
                fees_claimed={"x": 0.1, "y": 1.0}),
        ]
        r = explain_run(evs)
        assert r.lp_fee_claimed == pytest.approx(11.0)
        assert r.total_pnl == pytest.approx(11.0)
        assert not r.fee_divergence_flagged

    def test_raw_claimable_fees_use_logged_decimals(self):
        evs = [
            _ev("run_started", 0.0, base_decimals=6, quote_decimals=6),
            _ev("state_observation", 0.0, mid=100.0),
            _ev("position_observation", 1.0, position_id="P",
                claimable_fee_x_raw=1_000_000, claimable_fee_y_raw=2_000_000),
            _ev("position_observation", 2.0, position_id="P",
                claimable_fee_x_raw=1_500_000, claimable_fee_y_raw=2_500_000),
        ]
        r = explain_run(evs)
        assert r.lp_fee_accrued == pytest.approx(50.5)
        assert r.total_pnl == pytest.approx(50.5)


class TestCosts:
    def test_gas_and_rebalance_channel(self):
        evs = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("action_result", 1.0, verb="refresh_bundle", ok=True,
                fee_lamports=50000, tx_signatures=["T"]),
            _ev("cash_flow", 1.0, label="refresh_gas", amount_sol=-50000 / 1e9,
                amount_quote=-0.0075),
        ]
        r = explain_run(evs)
        assert r.gas_lamports == 50000
        assert abs(r.gas_sol - 5e-5) < 1e-18
        assert abs(r.rebalance_cost - (-0.0075)) < 1e-18
        assert abs(r.total_pnl - (-0.0075)) < 1e-18

    def test_claims_route_to_lp_fee_channel(self):
        evs = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("cash_flow", 1.0, label="fees_claimed", amount_sol=2.5),
        ]
        r = explain_run(evs)
        assert abs(r.total_pnl - 2.5) < ZERO

    def test_swap_slippage_vs_mid(self):
        base = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("action_result", 1.0, verb="swap", ok=True, fee_lamports=0,
                data={"amount_in": 10.0, "amount_out": 1490.0}),
        ]
        r = explain_run(base)
        # 10 base @ mid = 1500 USD in vs 1490 quote out → 10 USD slippage
        assert abs(r.swap_slippage - 10.0) < ZERO
        assert r.rebalance_cost == pytest.approx(-10.0)
        assert r.total_pnl == pytest.approx(-10.0)

        at_mid = list(base)
        at_mid[1] = _ev("action_result", 1.0, verb="swap", ok=True, fee_lamports=0,
                        data={"amount_in": 10.0, "amount_out": 1500.0})
        assert explain_run(at_mid).swap_slippage == 0.0

    def test_failed_swap_not_counted(self):
        evs = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("action_result", 1.0, verb="swap", ok=False,
                data={"amount_in": 10.0, "amount_out": 1.0}),
        ]
        assert explain_run(evs).swap_slippage == 0.0


class TestMarkout:
    def test_toxic_buy_gets_negative_30s_markout(self):
        evs = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("bin_fill", 10.0, side_filled="buy", bin_price=148.0,
                amount_base=1.0, mid_at_fill=150.0),
            _ev("state_observation", 41.0, mid=147.0),
        ]
        r = explain_run(evs)
        assert r.avg_markout_bps_30s is not None
        assert r.avg_markout_bps_30s < 0

    def test_insufficient_horizon_is_none(self):
        evs = [
            _ev("state_observation", 0.0, mid=150.0),
            _ev("bin_fill", 10.0, side_filled="buy", bin_price=148.0,
                amount_base=1.0, mid_at_fill=150.0),
            _ev("state_observation", 20.0, mid=147.0),  # only 10 s elapsed
        ]
        assert explain_run(evs).avg_markout_bps_30s is None
