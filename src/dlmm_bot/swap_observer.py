"""
On-chain swap observation (dlmm-logging-plan §4) — the critical missing
piece: the keeper only polls active_bin every 5 s, so crossings that enter
and revert within a poll interval are invisible for fill/fee/markout
matching.

A stream source (TS-executor logsSubscribe, a Python solana websocket task,
or an indexer backfill) decodes each DLMM swap on our pool into a payload
dict and feeds it to SwapObserver.on_swap(). The observer:
- dedupes by tx_signature (poll-derived price_change events dedupe the same)
- emits one `observed_trade` per swap with raw + grid-derived fields
- derives `bin_fill` events using the SAME fill rule as DLMMBacktester
  (fill at bin price, single-sided fills only on the crossed side), so live
  and backtest accounting cannot diverge
- tracks our resting per-bin liquidity (registered by the keeper on deploy)

Payload fields (decoded by the stream source):
  tx_signature (str, dedupe key), slot (int), block_time (int),
  direction ("up"|"down"), prev_active_bin (int), new_active_bin (int),
  amount_in/amount_out (decimal, optional *_raw for on-chain integers),
  trade_size_usd (float, optional), fee_bps (float, optional), tvl_usd
"""

from __future__ import annotations

import time
from typing import Mapping, Optional

from dlmm_bot.event_log import EventLog, _jsonable
from dlmm_bot.grid import VenueGrid


class SwapObserver:
    """Emits observed_trade + bin_fill events into the shared EventLog."""

    def __init__(self, log: EventLog, grid: VenueGrid, pool: str):
        self.log = log
        self.grid = grid
        self.pool = pool
        self._resting: dict[int, float] = {}
        self._position_id: str | None = None
        self._seen: set[str] = set()
        self._last_mid: float | None = None
        self._last_mid_ts: float | None = None
        self._default_fee_bps: float = 25.0

    def set_default_fee_bps(self, fee_bps: float) -> None:
        self._default_fee_bps = float(fee_bps)

    def set_last_mid(self, mid: float, ts: float) -> None:
        self._last_mid, self._last_mid_ts = mid, ts

    def register_ladder(
        self, sizes: Mapping[int, float], position_id: str | None = None
    ) -> None:
        """(Re)deploy happened: replace resting per-bin liquidity."""
        self._resting = {int(b): float(s) for b, s in sizes.items() if s > 0}
        if position_id is not None:
            self._position_id = position_id

    def clear(self) -> None:
        self._resting = {}
        self._position_id = None

    @property
    def resting(self) -> dict[int, float]:
        return dict(self._resting)

    def on_swap(self, payload: dict, ts: float | None = None) -> int | None:
        """Consume one decoded on-chain swap; returns observed_trade seq or
        None when the swap was a duplicate (same tx_signature)."""
        sig = payload.get("tx_signature")
        if sig:
            if sig in self._seen:
                return None
            self._seen.add(sig)

        prev = int(payload.get("prev_active_bin", 0))
        new = int(payload.get("new_active_bin", 0))
        direction = payload.get("direction") or (
            "up" if new > prev else "down"
        )
        event_ts = float(ts or payload.get("ts")
                         or payload.get("block_time")
                         or time.time())
        fee_bps = float(payload.get("fee_bps", self._default_fee_bps))

        crossed = (
            list(range(prev, new))
            if direction == "up"
            else list(range(new + 1, prev + 1))
        )
        crossed_ours = any(
            self._resting.get(b, 0.0) > 0 for b in crossed
        )
        bins_crossed = [
            {
                "bin_id": b,
                "bin_price": self.grid.price_from_bin(b),
                "amount_x": payload.get("amount_in"),
                "amount_y": payload.get("amount_out"),
            }
            for b in crossed
        ]

        seq = self.log.emit(
            "observed_trade",
            pool=self.pool,
            tx_signature=sig,
            slot=payload.get("slot"),
            block_time=payload.get("block_time"),
            ts=event_ts,
            direction=direction,
            prev_active_bin=prev,
            new_active_bin=new,
            amount_in=payload.get("amount_in"),
            amount_out=payload.get("amount_out"),
            amount_in_raw=payload.get("amount_in_raw"),
            amount_out_raw=payload.get("amount_out_raw"),
            trade_size_usd=payload.get("trade_size_usd", 0.0),
            fee_bps=fee_bps,
            tvl_usd=payload.get("tvl_usd"),
            bins_crossed=_jsonable(bins_crossed),
            crossed_ours=crossed_ours,
            source="swap_stream",
        )

        for b in crossed:
            size = self._resting.get(b, 0.0)
            if size <= 0:
                continue
            # Same fill rule as DLMMBacktester._process_event:
            # up-cross fills asks (we sell base), down-cross fills bids.
            side = "sell" if direction == "up" else "buy"
            bin_price = self.grid.price_from_bin(b)
            fee_accrued = size * bin_price * fee_bps / 1e4
            sig_tag = (sig or "")[:8]
            self.log.emit(
                "bin_fill",
                position_id=self._position_id,
                bin_id=b,
                bin_price=bin_price,
                side_filled=side,
                amount_base=size,
                amount_quote=size * bin_price,
                fee_accrued=fee_accrued,
                tx_signature=sig,
                mid_at_fill=self._last_mid,
                ts=event_ts,
                label=f"bin_{b}_{sig_tag}",
            )
            self._resting[b] = 0.0

        return seq

    def feed_stream(self, stream) -> int:
        """Drain an iterable/async-iterable of decoded swap payloads.
        Synchronous iterables only; for async, loop over the async
        iterator yourself and call on_swap()."""
        n = 0
        for payload in stream:
            if self.on_swap(payload) is not None:
                n += 1
        return n


__all__ = ["SwapObserver"]
