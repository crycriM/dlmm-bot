"""
Deterministic wall-clock for tests and log replay (dlmm-logging-plan §7).

Freezes ``time.time()`` so a keeper cycle is a pure function of its inputs.
A replayed keeper re-derives identical decisions when it is fed the recorded
``state_observation`` sequence at each event's recorded ``ts_wall`` — the
clock is set to that value before every cycle, so the price-history
deque, vol/regime windows, markout buckets and PnL marks all match the
original run exactly.

All modules in the package reference ``time.time()`` through the shared
stdlib ``time`` module, so patching the attribute once affects every caller
(keeper, event_log, ``_gas``) consistently.
"""

from __future__ import annotations

import time as _time


class FrozenClock:
    """Context manager that freezes ``time.time()`` to a controllable value."""

    def __init__(self, start: float = 1_000_000.0, step: float = 5.0):
        self._value = float(start)
        self.step = float(step)
        self._prev: "float | None" = None
        self._patched = False

    @property
    def now(self) -> float:
        return self._value

    def set(self, value: float) -> None:
        """Jump the frozen clock to an absolute value."""
        self._value = float(value)

    def advance(self, by: float | None = None) -> float:
        """Advance by ``by`` (default ``self.step``) and return the new time."""
        self._value += self.step if by is None else float(by)
        return self._value

    def __call__(self) -> float:
        return self._value

    def __enter__(self) -> "FrozenClock":
        self._prev = _time.time
        _time.time = self.__call__
        self._patched = True
        return self

    def __exit__(self, *exc: object) -> None:
        if self._patched:
            _time.time = self._prev
            self._patched = False


__all__ = ["FrozenClock"]
