"""Shared fixtures + a deterministic FakeExecBridge session recorder.

The ``record_session`` fixture records a keeper run over in-memory state with
a frozen wall-clock, producing a JSONL log that the replay/verify tests and
PnL-explain tests can read back. Time is frozen so the recorded ``ts`` values
are exact, which is what makes the Phase-4 deterministic replay reproducible.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys

import pytest

from dlmm_bot.clock import FrozenClock
from dlmm_bot.config import DLMMConfig
from dlmm_bot.event_log import EventLog
from dlmm_bot.exec_bridge import FakeExecBridge
from dlmm_bot.grid import VenueGrid
from dlmm_bot.keeper import (
    Keeper,
    KeeperConfig,
    dump_keeper_config,
    hash_keeper_config,
)
from dlmm_bot.risk_dlmm import PairType
from dlmm_bot.swap_observer import SwapObserver

_TOOLS_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "tools"))


@pytest.fixture
def grid():
    return VenueGrid(ref_price=150.0, bin_step_bps=20, base_decimals=6, quote_decimals=9)


@pytest.fixture
def cfg(grid):
    return KeeperConfig(
        dlmm=DLMMConfig(
            gamma=1.0, kappa=0.5, bin_step_bps=20, ref_price=150.0,
            levels=5, inner_offset=2, capital=1000.0, level_weight=0.2,
        ),
        grid=grid,
        pool_address="test_pool",
        dry_run=False,
        refresh_interval=5.0,
        position_id=None,
        pair_type=PairType.BLUECHIP,
    )


@pytest.fixture
def record_session(cfg):
    """Record a deterministic keeper session to ``path``.

    Returns ``(log_path, bridge)``. ``active_bins`` is the per-cycle active bin;
    ``balances`` (optional) is a per-cycle list of balance dicts.
    """
    def _record(active_bins, path, balances=None, run_id="run_test",
                start_ts=1_000_000.0, step=5.0):
        bridge = FakeExecBridge()
        bridge.set_receipt(
            position_id="POS_1", fee_lamports=50000,
            slot=1, block_time=100, compute_unit_price=2,
        )
        bridge.set_position("POS_1", {
            "claimable_fee_x": 0.0, "claimable_fee_y": 0.5,
            "positions": [{"activeBin": active_bins[0]}],
        })
        log = EventLog(path, run_id=run_id, config_hash=hash_keeper_config(cfg))
        keeper = Keeper(
            cfg, bridge, event_log=log,
            swap_observer=SwapObserver(log, cfg.grid, cfg.pool_address),
        )
        keeper.emit(
            "run_started",
            run_id=run_id, config_hash=hash_keeper_config(cfg),
            config=dump_keeper_config(cfg), pool_address=cfg.pool_address,
            dry_run=cfg.dry_run,
            base_decimals=cfg.grid.base_decimals,
            quote_decimals=cfg.grid.quote_decimals,
        )

        async def _run():
            with FrozenClock(start=start_ts, step=step) as clock:
                for i, b in enumerate(active_bins):
                    bal = (
                        balances[i] if balances is not None
                        else {"base": float(i) * 0.5, "quote": 10.0}
                    )
                    bridge.set_state(
                        cfg.pool_address, active_bin=b, balances=bal, tvl_usd=50000.0,
                    )
                    clock.advance()
                    await keeper._cycle()
            keeper.emit("run_stopped", reason="stop")

        asyncio.run(_run())
        log.close()
        return path, bridge

    return _record


def _load_tool(name):
    """Import a tool script from tools/ by path (not an installed package)."""
    mod_name = "_tool_" + name.replace(".", "").replace("-", "_")
    spec = importlib.util.spec_from_file_location(mod_name, os.path.join(_TOOLS_DIR, name))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def replay_tool():
    return _load_tool("replay.py")


@pytest.fixture
def verify_tool():
    return _load_tool("verify_log.py")
