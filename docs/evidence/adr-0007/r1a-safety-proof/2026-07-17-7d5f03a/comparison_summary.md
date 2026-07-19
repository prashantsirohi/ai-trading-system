# Shadow A/B/C proof — 2026-07-17 @ 7d5f03a

- **Verdict:** PASS
- **Headline `rank/pattern_scan.csv` byte-identical (A vs B):** True
- **Decision content identical (control-subtracted):** True
- **All strict artifacts byte-identical (informational):** False
- **Flag-caused legacy diffs (A~B minus A~C control):** none
- **Run B lane artifacts:** 7 · Run A has lane dir: False
- **Comparison policy:** shadow-parity-policy-v1

## Artifact comparison (non-identical only)

| Artifact | Class | Verdict | Differing cols |
|---|---|---|---|
| candidate_tracker/attempt_1/candidate_tracking_snapshots.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| execute/attempt_1/executed_fills.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| execute/attempt_1/executed_orders.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/company_growth_features.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/company_insight_tags.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/compounder_candidates.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/great_results.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/quarterly_result_scores.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/sector_earnings_latest.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/sector_earnings_leadership.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/sector_valuation_daily.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/sector_valuation_latest.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/stock_valuation_bands_latest.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/turnaround_candidates.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/universe_valuation_daily.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/universe_valuation_latest.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/valuation_cycle_features.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| fundamentals/attempt_1/valuation_cycle_latest.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| insight/attempt_1/event_features.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/active_watchlist.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/archived_investigator.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/daily_gainer_log.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/investigator_scores.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/position_risk_monitor.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/repeat_tracker.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/routed_investigator_scores.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/stage1_current_state.csv | STRICT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/stage1_watchlist.csv | STRICT | CONTENT_EQUIVALENT | [] |
| investigator/attempt_1/trap_log.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| opportunities/attempt_1/adapter_warnings.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| opportunities/attempt_1/position_recovery_proposals.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| performance/attempt_1/phase3c4_artifact_metrics.csv | TELEMETRY | TELEMETRY | [] |
| performance/attempt_1/phase3c4_database_metrics.csv | TELEMETRY | TELEMETRY | [] |
| performance/attempt_1/phase3c4_performance_metrics.csv | TELEMETRY | TELEMETRY | [] |
| rank/attempt_1/ranked_signals.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| rank/attempt_1/ranked_universe.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| rank/attempt_1/stock_scan.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| rank/attempt_1/watchlist_prefilter.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| rank/attempt_1/watchlist_rejections.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| scan_router/attempt_1/active_position_coverage.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| scan_router/attempt_1/deep_scan_universe.csv | CONTENT | DATA_DIFF | ['selection_details'] |
| scan_router/attempt_1/position_monitor_reconciliation.csv | CONTENT | CONTENT_EQUIVALENT | [] |
| scan_router/attempt_1/position_monitor_universe.csv | CONTENT | DATA_DIFF | ['selection_details'] |
| scan_router/attempt_1/scan_routing.csv | CONTENT | DATA_DIFF | ['selection_details'] |
