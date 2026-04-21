# Baseline Artifact Contracts

This file lists the current pipeline artifact filenames that must remain stable during the src-based migration.

## Authoritative run artifact root

- `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`

## Stage outputs

| Stage | Artifact | Format | Current producer |
| --- | --- | --- | --- |
| `ingest` | `ingest_summary.json` | JSON | `services.ingest.orchestration.IngestOrchestrationService` |
| `features` | `feature_snapshot.json` | JSON | `services.features.orchestration.FeaturesOrchestrationService` |
| `rank` | `ranked_signals.csv` | CSV | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `breakout_scan.csv` | CSV | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `pattern_scan.csv` | CSV | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `stock_scan.csv` | CSV | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `sector_dashboard.csv` | CSV | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `dashboard_payload.json` | JSON | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `rank_summary.json` | JSON | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `task_status.json` | JSON | `services.rank.orchestration.RankOrchestrationService` |
| `rank` | `ml_overlay.csv` | CSV | `services.rank.orchestration.RankOrchestrationService` when ML overlay is enabled |
| `execute` | `trade_actions.csv` | CSV | `run/stages/execute.py` |
| `execute` | `executed_orders.csv` | CSV | `run/stages/execute.py` |
| `execute` | `executed_fills.csv` | CSV | `run/stages/execute.py` |
| `execute` | `positions.csv` | CSV | `run/stages/execute.py` |
| `execute` | `execute_summary.json` | JSON | `run/stages/execute.py` |
| `publish` | `publish_summary.json` | JSON | `run/stages/publish.py` |

## Known downstream readers

- `execute` reads rank outputs, especially `ranked_signals.csv` and `dashboard_payload.json`.
- `publish` reads rank outputs and emits `publish_summary.json`.
- `ui/services/execution_data.py` and `ui/services/readmodels/` read the latest rank and execute artifacts.
- `ui/research/data_access.py` reads rank artifact files for operator and research views.
- `publishers/quantstats_dashboard.py` reads rank CSV artifacts from pipeline run folders.

## Fixtures representing current contracts

- `tests/fixtures/artifacts/rank/`
- `tests/fixtures/artifacts/execute/`
- `tests/fixtures/artifacts/publish/`
- `tests/fixtures/api_snapshots/`

## Migration guardrails

- Keep every artifact filename above unchanged.
- Keep JSON top-level keys and CSV column names unchanged.
- Keep stage names and stage output directories unchanged.
