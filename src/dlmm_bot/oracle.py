"""
Oracle minute bars for the regime gate and σ on pairs whose own pool ticks
too rarely to estimate them (DEX-only crosses such as MET/RAY).

Source: GeckoTerminal pool OHLCV (free, no key, minute bars ≥180 days back).
A cross A/B with no liquid pool of its own is synthesized from two liquid
USD legs — A/USD ÷ B/USD, aligned on bar time — so the thin pool never
prices itself.

Bars are (open_ts, open, high, low, close). A bar is only visible once
closed (open_ts + 60 <= ts): the backtest must not see a bar the live keeper
could not have had yet.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from bisect import bisect_right
from dataclasses import dataclass, field

GECKO = "https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool}/ohlcv/minute"
BAR_SECONDS = 60
# ponytail: fixed pacing under GeckoTerminal's free ~30 calls/min; a keyed
# plan or a shared limiter if several processes fetch at once.
PACE_SECONDS = 2.5

Bar = tuple[float, float, float, float, float]


def fetch_pool_bars(
    pool: str, start: float, end: float, *, network: str = "solana",
    currency: str = "usd", token: str = "base", fetch=None, sleep=time.sleep,
) -> list[Bar]:
    """Closed minute bars with open_ts in [start, end), oldest first.

    `currency="usd", token="base"` prices the pool's base token in USD
    (a leg); `currency="token"` prices base in the pool's quote token.
    """
    fetch = fetch or _get_json
    bars: dict[float, Bar] = {}
    before = int(end)
    first = True
    while before > start:
        if not first:
            sleep(PACE_SECONDS)
        first = False
        url = (GECKO.format(network=network, pool=pool)
               + f"?aggregate=1&limit=1000&currency={currency}&token={token}"
               + f"&before_timestamp={before}")
        rows = fetch(url)["data"]["attributes"]["ohlcv_list"]
        if not rows:
            break
        for t, o, h, lo, c, _v in rows:
            if start <= t < end and t + BAR_SECONDS <= end:
                bars[float(t)] = (float(t), float(o), float(h), float(lo), float(c))
        oldest = min(int(r[0]) for r in rows)
        if oldest >= before:
            break
        before = oldest
    return fill_gaps([bars[t] for t in sorted(bars)], end)


def fill_gaps(bars: list[Bar], end: float) -> list[Bar]:
    """One bar per minute from the first bar up to the last closed by `end`.

    GeckoTerminal omits minutes without trades. A minute with no trade kept
    its price, so it becomes a flat bar at the previous close; without this
    the regime reads an irregular series and a quiet leg looks like a dead
    feed. Staleness then means what it should: the fetch itself stopped.
    """
    out: list[Bar] = []
    for bar in bars:
        while out and out[-1][0] + BAR_SECONDS < bar[0]:
            c = out[-1][4]
            out.append((out[-1][0] + BAR_SECONDS, c, c, c, c))
        out.append(bar)
    while out and out[-1][0] + 2 * BAR_SECONDS <= end:
        c = out[-1][4]
        out.append((out[-1][0] + BAR_SECONDS, c, c, c, c))
    return out


def synthetic_cross(a: list[Bar], b: list[Bar]) -> list[Bar]:
    """A/B from A/USD and B/USD bars sharing an open time.

    Open/close are exact ratios. High/low are the bounds hA/lB and lA/hB:
    the true intra-bar extremes of a ratio are not recoverable from the
    legs' OHLC, so range-based σ (Parkinson/Garman-Klass) reads high on a
    synthetic cross; close-to-close and the regime gate use closes only.
    """
    b_by_t = {bar[0]: bar for bar in b}
    out = []
    for t, oa, ha, la, ca in a:
        if t not in b_by_t:
            continue
        _, ob, hb, lb, cb = b_by_t[t]
        if min(ob, hb, lb, cb) <= 0:
            continue
        out.append((t, oa / ob, ha / lb, la / hb, ca / cb))
    return out


@dataclass
class OracleBars:
    """Closed-bar window for `plan_cycle(regime_history=...)`.

    Backtest: construct with preloaded bars. Live: `refresh(now)` pulls new
    bars from the configured source at most once per bar.
    """

    bars: list[Bar] = field(default_factory=list)
    window: int = 120  # bars; the regime gate needs ≥ 33 closes
    source: dict | None = None  # {"pool": ...} or {"cross": [pool_a, pool_b]}
    _next_refresh: float = 0.0

    def history(self, ts: float) -> list[tuple[float, float]]:
        """(bar close time, close) for the last `window` bars closed by ts."""
        opens = [bar[0] for bar in self.bars]
        n = bisect_right(opens, ts - BAR_SECONDS)
        return [(t + BAR_SECONDS, c) for t, _o, _h, _l, c in self.bars[max(0, n - self.window):n]]

    def stale(self, ts: float, max_age: float = 5 * BAR_SECONDS) -> bool:
        """No bar closed within `max_age` of ts (feed down or pair dormant)."""
        return not self.bars or ts - (self.bars[-1][0] + BAR_SECONDS) > max_age

    def regime_history(self, ts: float) -> list[tuple[float, float]]:
        """What `plan_cycle` gets: the closed window, or [] when stale.

        [] rather than a fallback to pool samples: an empty history has an
        infinite half-life, so the gate closes instead of silently switching
        the pair back to its own too-sparse ticks.
        """
        return [] if self.stale(ts) else self.history(ts)

    def refresh(self, now: float, fetch=None, sleep=time.sleep) -> None:
        if self.source is None or now < self._next_refresh:
            return
        start = self.bars[-1][0] + BAR_SECONDS if self.bars else now - (self.window + 1) * BAR_SECONDS
        if "cross" in self.source:
            pool_a, pool_b = self.source["cross"]
            new = synthetic_cross(fetch_pool_bars(pool_a, start, now, fetch=fetch, sleep=sleep),
                                  fetch_pool_bars(pool_b, start, now, fetch=fetch, sleep=sleep))
        else:
            new = fetch_pool_bars(self.source["pool"], start, now, currency="token",
                                  fetch=fetch, sleep=sleep)
        self.bars = (self.bars + new)[-4 * self.window:]
        self._next_refresh = now + BAR_SECONDS


def _get_json(url: str, attempts: int = 5, sleep=time.sleep) -> dict:
    # GeckoTerminal answers 403 to urllib's default "Python-urllib" agent.
    req = urllib.request.Request(url, headers={"accept": "application/json", "User-Agent": "dlmm-bot/0.1"})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt + 1 == attempts:
                raise
            retry_after = exc.headers.get("Retry-After", "")
            sleep(int(retry_after) if retry_after.isdigit() else 15 * (attempt + 1))
    raise AssertionError("unreachable")
