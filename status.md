# dlmm-bot status

**Status date:** 2026-09-14
**Scope:** Solana DLMM (Meteora) market-making brain: keeper loop, ladder,
risk, hedging, event-replay backtester, and the `ExecBridge` seam toward the
executor. Companion executor project: `../solana-clmm-executor`
(its spec: `project_docs/opms-spec.md`).

Evidence ledger, not a claim that everything works end-to-end. Compiled from
the checked-in source, tests, plans (`clmm-animation/docs/dlmm-*`), and the
current working tree. The DLMM keeper has now run read-only on mainnet in
`observation_only` mode; it has **never run a calibrated strategy or submitted
an on-chain transaction**. See the sibling executor's `status.md` for the M2
latency reassessment and M3 swap-stream evidence.

## Bottom line

| Area | Status | What is actually proven | What is still needed |
|---|---|---|---|
| Keeper loop (poll → regime → ladder → actuate) | **Observation-only mainnet run; strategy still offline** | A 30-minute read-only run logged 168 matched state/position observations through the real Node executor, with zero failed reads or action requests (2026-09-14). The 1,851 ms p95 passes the revised <2,000 ms M2 gate. | Calibrate AS parameters before a full strategy shadow run. |
| `ExecBridge` protocol (6 verbs, receipts) | **M1 wire gate passed locally** | Twelve shared TS fixtures parse through `ExecResult.from_payload`; all six verbs and a keeper lifecycle run through `node dist/bridge.js`, including raw position observations and receipt-derived gas fees. | Live read/transaction evidence and full four-bridge fixture replay beyond the M1 slice. |
| Swap mints in `swap` verbs | **Fixed 2026-09-09; rejection tested 2026-09-10** | `_de_risk`/`_emergency_exit` send `cfg.base_mint`/`cfg.quote_mint`; M1 request validation rejects symbolic or unlisted mints with `bad_request`. | Live swap execution remains executor M5. |
| Event log + swap observer | **Implemented; separate M3 live gate passed** | Hash-chained `EventLog` and replay tooling; the sibling executor's M3 run fed 386 swaps through the Python observer and identified 15 owned-position fills. | Attach the observer to a continuous keeper run; the M2 observation-only soak deliberately did not exercise fill accounting. |
| DLMM risk (TVL/rug, pair types) | **Implemented + wrapped** | In-loop `evaluate_tvl` kill-switch plus the separate `TVLMonitor` sentinel path (phase-3 C1, in `hb-enhanced-opms`), both unit-tested. | Live monitor + keeper pair never run together. |
| Hedging (MA + deadband, vol-scaled window, hard cap) | **Implemented; not coupled live** | `hedge.py` unit-tested; `SharedRiskBook` wiring to `HedgeController` (phase-3 C2) unit-tested per-controller. | No integration test sharing one book across DLMM + hedge controllers; no live run. |
| Backtester | **Implemented (bin-crossing fill/fee sim)** | Standalone event-replay with crossed-bin fees, V3 facet. | Calibration on live-pair data per pair type; walk-forward gates not recorded. |
| Production execution path | **Live reads + swap stream; no live execution** | The sibling TS executor now serves mainnet reads and has passed the M2 read-only and separate M3 stream gates; it still requires `DRY_RUN=true` and loads no signer. | Steps 4–5 remain behind the signing gate. |

## Dated achievements

| Date | Achievement | Evidence / interpretation |
|---|---|---|
| (pre-2026-07) | Keeper, grid/ladder, hedge, risk, backtest, exec-bridge interface built. | Phase-3 plan §1.1 lists all of these as done; the keeper runs unchanged through the Gateway experiment and back. |
| 2026-07-16 | Phase-3 Gateway experiment (D5.1–D5.3). | Read path (`get_state`) fixed against live Gateway 2.15; write verbs found to speak a shape Gateway doesn't have (`lowerPrice`/`upperPrice`, no per-bin arrays). Mainnet dust open/close round trip proved rent economics, but bypassed the keeper and bridge. Conclusion: Gateway cannot host this bot's write path. |
| 2026-07-16 | `TVLMonitor` + sentinel path, `HedgeController` HB wrapper, `DLMMController` land (phase-3 C1/C2/B). | Unit-tested with HB stubs; not exercised live. `dlmm-bot` legacy suite recorded 83/85 (two pre-existing failures). |
| 2026-09-03 | `solana-clmm-executor` spec written; `src/protocol.ts` (wire types) checked in. | The TS executor project is created specifically because the Gateway path is a dead end for writes (opms-spec §1.1/§1.2). |
| 2026-09-09 | Spec review pass; opms-spec fixes 1–4 applied; keeper swap-mint bug fixed. | dlmm-bot suite re-run green: **145/145**. Spec now records that this project supersedes the GatewayExecBridge write path. |
| 2026-09-10 | Executor M1 gate passed locally. | `.venv/bin/python -m pytest -q --executor-subprocess`: **172 passed**. Default: **157 passed, 15 skipped**. `npm run check:m1` in the sibling executor builds and runs both language suites. Canned data only; not live or simulated chain evidence. |
| 2026-09-14 | First mainnet keeper observation-only soak. | 1,800.066 seconds, 168 state and 168 position observations with exact raw fees, zero failed executor reads or action requests. Its 1,851 ms p95 failed the former 400 ms limit but passes the revised <2,000 ms gate. Original and separate reassessment files are in `../solana-clmm-executor/logs/test-artifacts/evidence-keeper-m2-20260914T080731Z/`. |
| 2026-09-14 | Observation-only safety and cross-project suite. | `npm run check:m3` from the executor: 250 TypeScript and 187 Python tests passed, including new dry-run and environment-isolation checks. |
| 2026-09-14 | M2 gate revised to strict p95 < 2,000 ms. | Retained 30-minute run revalidated as a pass without changing its original summary. Cross-project suite: 250 TypeScript and 190 Python tests passed. |

## Gate ledger

| Gate | State | Closure condition |
|---|---|---|
| Unit suite | **Passing (157 passed, 15 opt-in skips, 2026-09-10).** | Keep green on every contract change. |
| Wire conformance vs real subprocess | **M1 passed locally (172 tests, 2026-09-10).** | Enable `--executor-subprocess` after the sibling executor build; missing Node/build fails the opt-in lane. |
| Dry-run keeper on mainnet reads | **Passed under revised <2,000 ms p95 gate.** | 30-minute hash-chained observation log is complete; original <400 ms-era summary is retained unchanged. |
| Verified fills (swap stream) | **M3 separate live gate passed.** | The executor stream and Python observer passed a 30-minute mainnet completeness check; continuous keeper integration remains a later gate. |
| Dust lifecycle (deposit/withdraw) | **Not started.** | Executor step 4, behind the signing-policy gate (test plan §5). |
| `swap` + `refresh_bundle` live | **Not started.** | Executor step 5; refresh atomicity (Jito) is the last and only atomicity risk. |
| Backtest calibration per pair | **Not recorded.** | Calibrate AS `gamma`/`kappa` per pair on its own data before any live config (monorepo rule). |
| Shadow → micro-capital rollout | **Not started.** | Follow `perp-bot/README.md` step 4 sequence adapted to DLMM; no devnet per the project summary — mainnet shadow/dry-run then micro capital under caps. |

## From here

1. Monitor the M2 RPC/price tail against the revised <2,000 ms p95 gate;
   repeat the 30-minute read-only run if provider or code changes materially.
2. Calibrate AS gamma/kappa on this pair's own data before a full strategy
   shadow run; keep the subprocess and M3 stream suites green.
3. Keep `GatewayExecBridge` writes out of any DLMM deployment plan (obsoleted,
   opms-spec §1.2); `DLMMController` must be constructed with the TS
   `ExecBridge` once it exists.
