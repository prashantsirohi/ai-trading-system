# Decision Read-Model Migration

- **Purpose:** Define ownership and source selection for decision-layer consumers.
- **Audience:** Developers and reviewers migrating decision-layer reads.
- **Last verified:** 2026-07-13
- **Source of truth:** `pipeline/migrations/029_decision_history.sql`, `030_decision_model_deployment.sql`, and `ui/execution_api/services/readmodels/decision_reads.py`.
- **Invariant:** CSV/JSON outputs remain immutable per-run audit evidence. Migrated current and historical views prefer DuckDB and report any artifact fallback.

## Consumer matrix

| Consumer | Previous source | DuckDB source | Shared owner | Date/version policy | Fallback | Phase |
|---|---|---|---|---|---|---:|
| Stage-1 current API/page/widgets | latest `investigator_stage1_state` date | `investigator_stage1_current` | `Stage1LifecycleReadRepository` + `stage1_operator` | physical current row and lifecycle version lineage | none | 1 |
| Stage-1 current/action/summary Sheets | Stage-1 operator bundle derived from state | `investigator_stage1_current` | same operator service as API | same as UI | none | 1 |
| Stage-1 lifecycle history | state CSV/read model | `investigator_stage1_state` | `Stage1LifecycleReadRepository` | requested date range and lifecycle version | none | 1 |
| Stage-1 transitions/changes/exits | transition table with duplicate presentation logic | `investigator_stage1_transition` | `Stage1LifecycleReadRepository` | event date; immutable transition ID | none | 1 |
| Stage-1 analytical detail | lifecycle state reconstruction | `stage1_history` | `Stage1AnalyticsReadRepository` | approved version effective on requested date | none | 2 |
| Broad Stage dashboard/detail | `stock_scan.csv` | `stage_history` | `StageHistoryReadRepository` | approved version and explicit as-of date | explicit current-view artifact fallback | 3 |
| Broad Stage Sheets | `stock_scan.csv` | `stage_history` | same repository/publish dataset | same as UI | explicit current-view artifact fallback | 3 |
| Rank dashboard/detail/history | `ranked_signals.csv` and run scans | `rank_history` | `RankHistoryReadRepository` | trade date + universe + approved version/config | explicit current-view artifact fallback | 4 |
| Rank Sheets/movers | rank artifacts and publisher-local deltas | `rank_history` | same repository; deltas computed in SQL | same as UI | explicit current-view artifact fallback | 4 |
| Pattern dashboard/timeline | `pattern_scan.csv` | `pattern_history` | `PatternHistoryReadRepository` | date + family + approved version/config | explicit current-view artifact fallback | 5 |
| Pattern Sheets | `pattern_scan.csv` | `pattern_history` | same repository/publish dataset | same as UI | explicit current-view artifact fallback | 5 |
| Main dashboard / Investigator overview | latest operational artifact snapshot | all decision tables | `DecisionOperatorReadService` | independently selected domain versions with explicit dates | source metadata retained per domain | 6 |
| Stock combined history | repeated run-directory scans | all history tables and lifecycle ledgers | `DecisionOperatorReadService` | bounded point-in-time alignment | none | 8 |

## Selection and ownership

`decision_model_deployment` is the approval source for rank, Stage, Stage-1 analytics, pattern, and Stage-1 lifecycle versions. Selection uses the latest effective approved deployment for the requested date; it never compares version strings. A tie is an error. Callers may explicitly provide both model version and configuration hash.

All operational DB paths resolve through `get_domain_paths` and therefore `DATA_ROOT`. Query values are bound parameters; dynamic table and column identifiers are internal constants.

## Artifact reader classification

| Reader | Classification | Reason |
|---|---|---|
| `latest_operational_snapshot` rank/stage/pattern frames | `KEEP_AS_FALLBACK` | Compatibility for current availability-sensitive views until all legacy panels are removed. |
| `publish_payloads` rank/stage/pattern readers | `KEEP_AS_FALLBACK` | Publisher emits source, reason, run, date, and row-count diagnostics. |
| artifact download and run-detail routes | `KEEP_FOR_AUDIT` | Operator inspection and reproducibility. |
| prior-rank artifact loader inside rank computation | `KEEP_FOR_AUDIT` | Rank-stage analytical input; outside consumer migration scope. |
| CSV fixtures in tests | `KEEP_FOR_TEST_FIXTURE` | Contract and fallback coverage. |
| UI historical run-directory scans | `REMOVE` | Replaced by bounded history repositories and endpoints. |

## API and diagnostics

Historical endpoints live under `/api/stocks/{symbol}` and accept bounded date, exchange, version/config, universe/family, limit, and offset filters as applicable. Stage-1 analytics also remains available under the execution-console Stage-1 route. `/api/health/decision-read-sources` exposes the current source, lineage, row count, fallback state, and selection error for every domain.

Normal DuckDB responses identify `data_source=DUCKDB`. Artifact fallback is never used for historical endpoints and is never silent for current publishing reads.
