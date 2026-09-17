"""Shared fixtures + a deterministic FakeExecBridge session recorder.

The ``record_session`` fixture records a keeper run over in-memory state with
a frozen wall-clock, producing a JSONL log that the replay/verify tests and
PnL-explain tests can read back. Time is frozen so the recorded ``ts`` values
are exact and deterministic replay remains reproducible.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys

import pytest

from dlmm_bot.clock import FrozenClock
from dlmm_bot.config import DLMMConfig
from dlmm_bot.event_log import EventLog
from dlmm_bot.exec_bridge import ExecBridge, FakeExecBridge
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

EXECUTOR_DIR = Path(__file__).resolve().parents[2] / "solana-clmm-executor"


def _synthetic_pubkey(seed: int) -> str:
    """Return a valid, deterministic Solana public key owned by no test user."""
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    raw = bytes([seed]) * 32
    value = int.from_bytes(raw, "big")
    encoded = ""
    while value:
        value, remainder = divmod(value, 58)
        encoded = alphabet[remainder] + encoded
    return encoded


TEST_WALLET_PUBKEY = _synthetic_pubkey(1)


def _executor_fixture_addresses() -> tuple[str, str, str]:
    """Read the public IDs required by the sibling's canonical offline fixtures."""
    requests_path = EXECUTOR_DIR / "fixtures" / "requests.json"
    if not requests_path.is_file():
        # Collection must still work when the optional sibling is absent.
        return tuple(_synthetic_pubkey(seed) for seed in (2, 3, 4))
    requests = json.loads(requests_path.read_text(encoding="utf-8"))
    swap = requests["swap"]
    return requests["get_state"]["pool"], swap["in_mint"], swap["out_mint"]


TEST_POOL_ADDRESS, TEST_BASE_MINT, TEST_QUOTE_MINT = _executor_fixture_addresses()


def pytest_addoption(parser):
    parser.addoption(
        "--executor-subprocess", action="store_true", default=False,
        help="Also run keeper tests against the built, offline TS executor",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "executor_subprocess: requires the built sibling TS executor")


class FixtureExecBridge(ExecBridge):
    """Observe real requests; configure Node's canned read handler via a file.

    Every response comes from Node through ExecBridge._send. No Python fake
    result is substituted. Only the test runner reads this scenario file.
    """

    def __init__(self, node, scenario_path, *, injected=True):
        self.calls = []
        self.scenario_path = scenario_path
        self.scenario = {"ok": True, "state": {}}
        self._save()
        cmd = [node, "fixtures/keeper-runner.mjs", str(scenario_path)] if injected else [node, "fixtures/stub-runner.mjs"]
        super().__init__(cmd, cwd=str(EXECUTOR_DIR))

    def _save(self):
        self.scenario_path.write_text(json.dumps(self.scenario))

    def set_state(self, pool, active_bin, balances=None, tvl_usd=None):
        balances = balances or {"base": 0.0, "quote": 0.0}
        self.scenario["state"] = {
            "active_bin": active_bin, "balances": balances, "tvl_usd": tvl_usd,
            "balances_raw": {
                "base": str(round(balances["base"] * 10**9)),
                "quote": str(round(balances["quote"] * 10**6)),
            },
        }
        self._save()

    def set_next_result(self, ok):
        self.scenario["ok"] = ok
        self._save()

    def _send(self, request):
        self.calls.append(request)
        return super()._send(request)

    def stop(self):
        proc = self._proc
        super().stop()
        if proc is not None:
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None:
                    stream.close()


@pytest.fixture
def executor_env(request, monkeypatch, tmp_path):
    if not request.config.getoption("--executor-subprocess"):
        pytest.skip("enable with --executor-subprocess after npm run build in solana-clmm-executor")
    node = shutil.which("node")
    if node is None or not (EXECUTOR_DIR / "dist" / "bridge.js").is_file():
        pytest.fail("Node and solana-clmm-executor/dist/bridge.js are required; run npm ci && npm run build")
    env = {
        "SOLANA_RPC_URL": "http://localhost:1", "SOLANA_RPC_WRITE_URL": "http://localhost:1",
        "SOLANA_COMMITMENT": "confirmed", "WALLET_SIGNER": "kms", "DRY_RUN": "true",
        "WALLET_PUBKEY": TEST_WALLET_PUBKEY,
        "POOL_ALLOWLIST": TEST_POOL_ADDRESS,
        "MINT_ALLOWLIST": f"{TEST_BASE_MINT},{TEST_QUOTE_MINT}",
        "MAX_SOL_PER_TX": "0.5", "MAX_SOL_PER_RUN": "2", "MAX_SLIPPAGE_BPS": "50",
        "MAX_ACTIVE_BIN_SLIPPAGE_BINS": "3",
        "MAX_PRIORITY_FEE_LAMPORTS": "100000", "JITO_ENABLED": "false", "JITO_TIP_LAMPORTS": "0",
        "EXECUTOR_LOG_DIR": str(tmp_path / "executor-logs"),
        "SWAP_STREAM_PATH": str(tmp_path / "swaps.jsonl"),
    }
    for key in ("KMS_KEY_ARN", "WALLET_SECRET_ARN", "JITO_BLOCK_ENGINE_URL"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return node, tmp_path


@pytest.fixture
def executor_addresses():
    """Offline fixture public IDs shared with subprocess lifecycle tests."""
    return {
        "wallet": TEST_WALLET_PUBKEY,
        "pool": TEST_POOL_ADDRESS,
        "base_mint": TEST_BASE_MINT,
        "quote_mint": TEST_QUOTE_MINT,
    }


@pytest.fixture
def real_subprocess_bridge(executor_env):
    node, tmp_path = executor_env
    bridge = FixtureExecBridge(node, tmp_path / "scenario.json")
    yield bridge
    bridge.stop()


@pytest.fixture
def executor_cli_bridge(executor_env):
    node, tmp_path = executor_env
    bridge = FixtureExecBridge(node, tmp_path / "scenario.json", injected=False)
    yield bridge
    bridge.stop()


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
        keeper._ensure_run_started()

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
            keeper._finalize_log("stop")

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
