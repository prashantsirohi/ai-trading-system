# Documentation Index

- **Purpose:** Complete map of all documentation under `docs/`.
- **Audience:** Anyone navigating the docs.
- **Last verified:** 2026-07-13
- **Source of truth:** This file is the source of truth for the doc inventory; `SYSTEM_GUIDE.md` is the source of truth for system orientation.

> Each document records its own verification date. See [`development/legacy_cleanup_plan.md`](development/legacy_cleanup_plan.md) for the cleanup history.

## Landing
- [SYSTEM_GUIDE](SYSTEM_GUIDE.md) — canonical system orientation and operating contract
- [README](README.md) — role-based documentation landing page

## Architecture
- [operational_data_flow](architecture/operational_data_flow.md)
- [storage_and_lineage](architecture/storage_and_lineage.md)
- [data_trust_and_dq](architecture/data_trust_and_dq.md)
- [ui_architecture](architecture/ui_architecture.md)
- [target_architecture](architecture/target_architecture.md)
- [opportunity_lifecycle_contracts](architecture/opportunity_lifecycle_contracts.md)

## Stages (13)
- [ingest](stages/ingest.md)
- [features](stages/features.md)
- [rank](stages/rank.md)
- [investigator](stages/investigator.md)
- [fundamentals](stages/fundamentals.md)
- [candidates](stages/candidates.md)
- [candidate_tracker](stages/candidate_tracker.md)
- [events](stages/events.md)
- [execute](stages/execute.md)
- [insight](stages/insight.md)
- [narrative](stages/narrative.md)
- [publish](stages/publish.md)
- [perf_tracker](stages/perf_tracker.md)

## Domains
- [ingest_domain](domains/ingest_domain.md)
- [features_domain](domains/features_domain.md)
- [ranking_domain](domains/ranking_domain.md)
- [execution_domain](domains/execution_domain.md)
- [publishing_domain](domains/publishing_domain.md)
- [research_domain](domains/research_domain.md)
- [ui_domain](domains/ui_domain.md)
- [platform_domain](domains/platform_domain.md)
- [fundamentals_domain](domains/fundamentals_domain.md)
- [catalyst_intelligence_domain](domains/catalyst_intelligence_domain.md)
- [optimization_domain](domains/optimization_domain.md)

## Reference
- [commands](reference/commands.md)
- [configuration](reference/configuration.md)
- [environment_variables](reference/environment_variables.md)
- [api_reference](reference/api_reference.md)
- [database_schema](reference/database_schema.md)
- [artifacts](reference/artifacts.md)
- [data_sources](reference/data_sources.md)
- [ranking_factors](reference/ranking_factors.md)
- [breakout_and_patterns](reference/breakout_and_patterns.md)
- [execution_policy](reference/execution_policy.md)
- [publish_contracts](reference/publish_contracts.md)

## Runbooks
- [daily_operations](runbooks/daily_operations.md)
- [weekly_operations](runbooks/weekly_operations.md)
- [troubleshooting](runbooks/troubleshooting.md)
- [data_repair](runbooks/data_repair.md)
- [dq_failure_response](runbooks/dq_failure_response.md)
- [publish_retry](runbooks/publish_retry.md)
- [backup_and_restore](runbooks/backup_and_restore.md)
- [copied_data_canary](runbooks/copied_data_canary.md)
- [deployment_mac_mini](runbooks/deployment_mac_mini.md)
- [optimization](runbooks/optimization.md)

## Development
- [contributing](development/contributing.md)
- [coding_standards](development/coding_standards.md)
- [testing_strategy](development/testing_strategy.md)
- [package_migration](development/package_migration.md)
- [legacy_cleanup_plan](development/legacy_cleanup_plan.md)
- [adding_new_stage](development/adding_new_stage.md)
- [adding_new_factor](development/adding_new_factor.md)
- [adding_new_publisher](development/adding_new_publisher.md)
- [adding_new_api_endpoint](development/adding_new_api_endpoint.md)
- [docs_update_checklist](development/docs_update_checklist.md)

## Decisions (ADRs)
- [ADR-0001 staged pipeline](decisions/ADR-0001-staged-pipeline.md)
- [ADR-0002 DuckDB control plane](decisions/ADR-0002-duckdb-control-plane.md)
- [ADR-0003 trust-first ingest](decisions/ADR-0003-trust-first-ingest.md)
- [ADR-0004 artifact-driven publish](decisions/ADR-0004-artifact-driven-publish.md)
- [ADR-0005 React operator workspace](decisions/ADR-0005-react-operator-workspace.md)

## Audit
- [documentation_inventory](_audit/documentation_inventory.md)
- [stale_reference_report](_audit/stale_reference_report.md)
- [retired code-truth-map tombstone](_audit/current_code_truth_map.md)

### July 2026 deep codebase review

- [executive summary](audits/codebase_deep_review/00_EXECUTIVE_SUMMARY.md)
- [repository and runtime map](audits/codebase_deep_review/01_REPOSITORY_AND_RUNTIME_MAP.md)
- [confirmed findings](audits/codebase_deep_review/02_CONFIRMED_FINDINGS.md)
- [performance and scaling](audits/codebase_deep_review/03_PERFORMANCE_AND_SCALING.md)
- [maintainability and target architecture](audits/codebase_deep_review/04_MAINTAINABILITY_AND_TARGET_ARCHITECTURE.md)
- [security and execution safety](audits/codebase_deep_review/05_SECURITY_AND_EXECUTION_SAFETY.md)
- [test and CI gaps](audits/codebase_deep_review/06_TEST_AND_CI_GAPS.md)
- [documentation drift](audits/codebase_deep_review/07_DOCUMENTATION_DRIFT.md)
- [prioritized remediation roadmap](audits/codebase_deep_review/08_PRIORITIZED_REMEDIATION_ROADMAP.md)
- [quick wins](audits/codebase_deep_review/09_QUICK_WINS.md)
- [deferred or rejected ideas](audits/codebase_deep_review/10_DEFERRED_OR_REJECTED_IDEAS.md)
- [Phase 1 closeout](audits/codebase_deep_review/11_PHASE1_CLOSEOUT.md)

## Legacy / archive
- [_legacy/](_legacy/README.md)
