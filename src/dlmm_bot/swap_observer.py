"""
On-chain swap observation closes a polling gap: the keeper only polls
active_bin every 5 s, so crossings that enter
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

import asyncio
import json
import os
import time
from typing import AsyncIterable, Callable, Mapping, Optional, Protocol

from dlmm_bot.event_log import EventLog, _jsonable
from dlmm_bot.grid import VenueGrid


class SwapObserver:
    """Emits observed_trade + bin_fill events into the shared EventLog."""

    def __init__(self, log: EventLog, grid: VenueGrid, pool: str):
        self.log = log
        self.grid = grid
        self.pool = pool
        self._resting: dict[int, float | dict] = {}
        self._position_id: str | None = None
        self._seen: set[str] = set()
        self._last_mid: float | None = None
        self._last_mid_ts: float | None = None
        self._default_fee_bps: float = 25.0
        self._fill_callback: Callable[[float, str, float, float], None] | None = None

    def set_default_fee_bps(self, fee_bps: float) -> None:
        self._default_fee_bps = float(fee_bps)

    def set_last_mid(self, mid: float, ts: float) -> None:
        self._last_mid, self._last_mid_ts = mid, ts

    def set_fill_callback(
        self, callback: Callable[[float, str, float, float], None]
    ) -> None:
        self._fill_callback = callback

    def register_ladder(
        self, sizes: Mapping[int, float | dict], position_id: str | None = None
    ) -> None:
        """Replace resting liquidity, preserving token units for each bin."""
        resting: dict[int, float | dict] = {}
        for bin_id, value in sizes.items():
            if isinstance(value, dict):
                row = dict(value)
                if float(row.get("amount_base", 0.0) or 0.0) > 0 or float(
                    row.get("amount_quote", 0.0) or 0.0
                ) > 0:
                    resting[int(bin_id)] = row
            elif float(value) > 0:
                resting[int(bin_id)] = float(value)
        self._resting = resting
        if position_id is not None:
            self._position_id = position_id

    def clear(self) -> None:
        self._resting = {}
        self._position_id = None

    @property
    def resting(self) -> dict[int, float | dict]:
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
        def has_resting_liquidity(bin_id: int) -> bool:
            resting = self._resting.get(bin_id)
            if isinstance(resting, dict):
                expected_side = "ask" if direction == "up" else "bid"
                if resting.get("side") not in (None, expected_side):
                    return False
                # Upward traversal consumes base/ask liquidity; downward
                # traversal consumes quote/bid liquidity. A mixed active bin
                # is ours only when the token actually consumed is non-zero.
                token_key = "amount_base" if direction == "up" else "amount_quote"
                return float(resting.get(token_key, 0.0) or 0.0) > 0
            return resting is not None and float(resting) > 0

        crossed_ours = any(has_resting_liquidity(b) for b in crossed)
        decoded_bins = {
            int(row["bin_id"]): row
            for row in (payload.get("bins_crossed") or [])
            if isinstance(row, dict) and row.get("bin_id") is not None
        }
        bins_crossed = []
        for bin_id in crossed:
            row = dict(decoded_bins.get(bin_id, {}))
            row["bin_id"] = bin_id
            row.setdefault("bin_price", self.grid.price_from_bin(bin_id))
            bins_crossed.append(row)

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
        self.log.emit(
            "price_change",
            prev_active_bin=prev,
            new_active_bin=new,
            direction=direction,
            mid_before=self.grid.price_from_bin(prev),
            mid_after=self.grid.price_from_bin(new),
            source="swap_stream",
            obs_seq=seq,
            tx_signature=sig,
        )

        for b in crossed:
            resting = self._resting.get(b)
            if resting is None:
                continue
            side = "sell" if direction == "up" else "buy"
            bin_price = self.grid.price_from_bin(b)
            if isinstance(resting, dict):
                expected_side = "ask" if side == "sell" else "bid"
                if resting.get("side") not in (None, expected_side):
                    continue
                if side == "sell":
                    amount_base = float(resting.get("amount_base", 0.0) or 0.0)
                    amount_quote = amount_base * bin_price
                else:
                    amount_quote = float(resting.get("amount_quote", 0.0) or 0.0)
                    amount_base = amount_quote / bin_price if bin_price else 0.0
                amount_base_raw = resting.get("amount_base_raw")
                amount_quote_raw = resting.get("amount_quote_raw")
            else:
                # Legacy/backtester input is base-denominated on both sides.
                amount_base = float(resting)
                amount_quote = amount_base * bin_price
                amount_base_raw = None
                amount_quote_raw = None
            if amount_base <= 0:
                continue
            fee_accrued = amount_quote * fee_bps / 1e4
            sig_tag = (sig or "")[:8]
            self.log.emit(
                "bin_fill",
                position_id=self._position_id,
                bin_id=b,
                bin_price=bin_price,
                side_filled=side,
                amount_base=amount_base,
                amount_quote=amount_quote,
                amount_base_raw=amount_base_raw,
                amount_quote_raw=amount_quote_raw,
                fee_accrued=fee_accrued,
                tx_signature=sig,
                slot=payload.get("slot"),
                block_time=payload.get("block_time"),
                mid_at_fill=self._last_mid,
                ts=event_ts,
                label=f"bin_{b}_{sig_tag}",
            )
            if self._fill_callback is not None:
                self._fill_callback(event_ts, side, bin_price, amount_base)
            self._resting[b] = 0.0

        return seq

    async def feed_async_stream(self, stream: AsyncIterable[dict]) -> int:
        """Drain a decoded live stream into the same total-ordered writer."""
        n = 0
        async for payload in stream:
            if self.on_swap(payload) is not None:
                n += 1
        return n

    def feed_stream(self, stream) -> int:
        """Drain an iterable/async-iterable of decoded swap payloads.
        Synchronous iterables only; for async, loop over the async
        iterator yourself and call on_swap()."""
        n = 0
        for payload in stream:
            if self.on_swap(payload) is not None:
                n += 1
        return n


class SwapEventSource(Protocol):
    """Decoded swap source implemented by an executor, RPC decoder, or file tail."""

    async def backfill(self, pool: str, after_signature: str | None) -> list[dict]: ...

    def subscribe(self, pool: str) -> AsyncIterable[dict]: ...


class JsonlSwapEventSource:
    """Tail decoded swap JSONL produced by the Solana/TS executor.

    The source tracks a byte offset, backfills existing rows before tailing, and
    survives truncation/rotation. Each row is the payload accepted by on_swap().
    """

    def __init__(self, path: str, poll_interval: float = 0.25):
        self.path = path
        self.poll_interval = poll_interval
        self._offset = 0

    def _read_new(self, pool: str) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        size = os.path.getsize(self.path)
        if size < self._offset:
            self._offset = 0
        rows = []
        with open(self.path, "r", encoding="utf-8") as fh:
            fh.seek(self._offset)
            for line in fh:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("pool") in (None, "", pool):
                    rows.append(payload)
            self._offset = fh.tell()
        return rows

    async def backfill(self, pool: str, after_signature: str | None) -> list[dict]:
        rows = self._read_new(pool)
        if after_signature is None:
            return rows
        for index, row in enumerate(rows):
            if row.get("tx_signature") == after_signature:
                return rows[index + 1:]
        return rows

    async def subscribe(self, pool: str):
        while True:
            rows = self._read_new(pool)
            if not rows:
                await asyncio.sleep(self.poll_interval)
                continue
            for row in rows:
                yield row


class SwapStreamRunner:
    """Own a decoded live stream, backfill gaps, and feed one EventLog writer."""

    def __init__(
        self, observer: SwapObserver, source: SwapEventSource, retry_delay: float = 1.0
    ):
        self.observer = observer
        self.source = source
        self.retry_delay = retry_delay
        self._task: asyncio.Task | None = None
        self._last_signature: str | None = None

    async def _backfill(self) -> None:
        for payload in await self.source.backfill(
            self.observer.pool, self._last_signature
        ):
            if self.observer.on_swap(payload) is not None:
                self._last_signature = payload.get("tx_signature") or self._last_signature

    async def run(self) -> None:
        await self._backfill()
        while True:
            try:
                async for payload in self.source.subscribe(self.observer.pool):
                    if self.observer.on_swap(payload) is not None:
                        self._last_signature = (
                            payload.get("tx_signature") or self._last_signature
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.observer.log.emit(
                    "swap_stream_gap",
                    error=f"{type(exc).__name__}: {exc}",
                    after_signature=self._last_signature,
                )
                await self._backfill()
                await asyncio.sleep(self.retry_delay)

    def start(self) -> asyncio.Task:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run(), name="dlmm-swap-stream")
        return self._task

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()


__all__ = [
    "SwapObserver", "SwapEventSource", "JsonlSwapEventSource", "SwapStreamRunner",
]
