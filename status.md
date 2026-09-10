# dlmm-bot status

**Status date:** 2026-09-10
**Scope:** Solana DLMM (Meteora) market-making brain: keeper loop, ladder,
risk, hedging, event-replay backtester, and the `ExecBridge` seam toward the
executor. Companion executor project: `../solana-clmm-executor`
(its spec: `project_docs/opms-spec.md`).

Evidence ledger, not a claim that everything works end-to-end. Compiled from
the checked-in source, tests, plans (`clmm-animation/docs/dlmm-*`), and the
current working tree. The DLMM keeper has **never run live**: the TS executor
it depends on did not exist until `solana-clmm-executor` began (2026-09-03,
spec + wire types only — see its `status.md`).

## Bottom line

| Area | Status | What is actually proven | What is still needed |
|---|---|---|---|
| Keeper loop (poll → regime → ladder → actuate) | **Implemented; offline subprocess proof added** | 157 passed, 15 skipped in the default lane; 172 passed with `--executor-subprocess` in its own venv (2026-09-10). All twelve existing keeper cases run through both the fake and real Node transport. | A live run against the real executor; no production deployment has ever happened. |
| `ExecBridge` protocol (6 verbs, receipts) | **M1 wire gate passed locally** | Twelve shared TS fixtures parse through `ExecResult.from_payload`; all six verbs and a keeper lifecycle run through `node dist/bridge.js`, including raw position observations and receipt-derived gas fees. | Live read/transaction evidence and full four-bridge fixture replay beyond the M1 slice. |
| Swap mints in `swap` verbs | **Fixed 2026-09-09; rejection tested 2026-09-10** | `_de_risk`/`_emergency_exit` send `cfg.base_mint`/`cfg.quote_mint`; M1 request validation rejects symbolic or unlisted mints with `bad_request`. | Live swap execution remains executor M5. |
| Event log + swap observer | **Implemented** | Hash-chained `EventLog` (`run_started` … `action_result`), `SwapObserver` with JSONL tail + backfill, replay/rerun tooling (`tools/`, `ReplayExecBridge`). | Zero verified fills to date — requires the executor's swap stream (opms-spec §6), which is not built. |
| DLMM risk (TVL/rug, pair types) | **Implemented + wrapped** | In-loop `evaluate_tvl` kill-switch plus the separate `TVLMonitor` sentinel path (phase-3 C1, in `hb-enhanced-opms`), both unit-tested. | Live monitor + keeper pair never run together. |
| Hedging (MA + deadband, vol-scaled window, hard cap) | **Implemented; not coupled live** | `hedge.py` unit-tested; `SharedRiskBook` wiring to `HedgeController` (phase-3 C2) unit-tested per-controller. | No integration test sharing one book across DLMM + hedge controllers; no live run. |
| Backtester | **Implemented (bin-crossing fill/fee sim)** | Standalone event-replay with crossed-bin fees, V3 facet. | Calibration on live-pair data per pair type; walk-forward gates not recorded. |
| Production execution path | **M1 transport proved; no live execution** | Dry-run-only TS stub bridge; no RPC, simulation, or signing in this path. | `solana-clmm-executor` steps 2–5 and its test-plan signing gate. |

## Dated achievements

| Date | Achievement | Evidence / interpretation |
|---|---|---|
| (pre-2026-07) | Keeper, grid/ladder, hedge, risk, backtest, exec-bridge interface built. | Phase-3 plan §1.1 lists all of these as done; the keeper runs unchanged through the Gateway experiment and back. |
| 2026-07-16 | Phase-3 Gateway experiment (D5.1–D5.3). | Read path (`get_state`) fixed against live Gateway 2.15; write verbs found to speak a shape Gateway doesn't have (`lowerPrice`/`upperPrice`, no per-bin arrays). Mainnet dust open/close round trip proved rent economics, but bypassed the keeper and bridge. Conclusion: Gateway cannot host this bot's write path. |
| 2026-07-16 | `TVLMonitor` + sentinel path, `HedgeController` HB wrapper, `DLMMController` land (phase-3 C1/C2/B). | Unit-tested with HB stubs; not exercised live. `dlmm-bot` legacy suite recorded 83/85 (two pre-existing failures). |
| 2026-09-03 | `solana-clmm-executor` spec written; `src/protocol.ts` (wire types) checked in. | The TS executor project is created specifically because the Gateway path is a dead end for writes (opms-spec §1.1/§1.2). |
| 2026-09-09 | Spec review pass; opms-spec fixes 1–4 applied; keeper swap-mint bug fixed. | dlmm-bot suite re-run green: **145/145**. Spec now records that this project supersedes the GatewayExecBridge write path. |
| 2026-09-10 | Executor M1 gate passed locally. | `.venv/bin/python -m pytest -q --executor-subprocess`: **172 passed**. Default: **157 passed, 15 skipped**. `npm run check:m1` in the sibling executor builds and runs both language suites. Canned data only; not live or simulated chain evidence. |

## Gate ledger

| Gate | State | Closure condition |
|---|---|---|
| Unit suite | **Passing (157 passed, 15 opt-in skips, 2026-09-10).** | Keep green on every contract change. |
| Wire conformance vs real subprocess | **M1 passed locally (172 tests, 2026-09-10).** | Enable `--executor-subprocess` after the sibling executor build; missing Node/build fails the opt-in lane. |
| Dry-run keeper on mainnet reads | **Not started.** | Executor step 2 (`get_state`/`get_position` via lp-monitor reuse); verify `state_observation`/`position_observation` incl. `claimable_fee_*_raw`. |
| Verified fills (swap stream) | **Not started.** | Executor step 3 (`swapStream.ts`); `observed_trade`/`bin_fill` events appear and `verify_log.py` completeness passes. |
| Dust lifecycle (deposit/withdraw) | **Not started.** | Executor step 4, behind the signing-policy gate (test plan §5). |
| `swap` + `refresh_bundle` live | **Not started.** | Executor step 5; refresh atomicity (Jito) is the last and only atomicity risk. |
| Backtest calibration per pair | **Not recorded.** | Calibrate AS `gamma`/`kappa` per pair on its own data before any live config (monorepo rule). |
| Shadow → micro-capital rollout | **Not started.** | Follow `perp-bot/README.md` step 4 sequence adapted to DLMM; no devnet per the project summary — mainnet shadow/dry-run then micro capital under caps. |

## From here

1. Build `solana-clmm-executor` steps 2–3 (read-only + swap stream) — they
   unblock the full logging pipeline with zero signing risk.
2. Keep the M1 subprocess suite green as live handlers replace the stubs.
3. Keep `GatewayExecBridge` writes out of any DLMM deployment plan (obsoleted,
   opms-spec §1.2); `DLMMController` must be constructed with the TS
   `ExecBridge` once it exists.
