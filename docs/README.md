# Documentation

- **Purpose:** Landing page for the AI trading system documentation.
- **Audience:** Operator, developer, future agents.
- **Last verified:** 2026-05-16
- **Source of truth:** This file and [`INDEX.md`](INDEX.md).

---

The full doc map is in [INDEX.md](INDEX.md). Start there if you want everything in one list.

## Quick paths

### Operator
1. [runbooks/daily_operations.md](runbooks/daily_operations.md) — daily run + verify
2. [runbooks/troubleshooting.md](runbooks/troubleshooting.md) — symptom → fix
3. [runbooks/data_repair.md](runbooks/data_repair.md), [dq_failure_response.md](runbooks/dq_failure_response.md), [publish_retry.md](runbooks/publish_retry.md)
4. [reference/commands.md](reference/commands.md) — all CLI commands

### New developer
1. [architecture/overview.md](architecture/overview.md)
2. [architecture/operational_data_flow.md](architecture/operational_data_flow.md)
3. [architecture/target_architecture.md](architecture/target_architecture.md)
4. [stages/](stages/) — per-stage contracts
5. [domains/](domains/) — domain-level ownership
6. [development/contributing.md](development/contributing.md)

### Debugging a pipeline run
1. [architecture/storage_and_lineage.md](architecture/storage_and_lineage.md) — where artifacts live
2. [architecture/data_trust_and_dq.md](architecture/data_trust_and_dq.md) — trust + DQ gating
3. [reference/artifacts.md](reference/artifacts.md) — per-stage artifact reference
4. [runbooks/troubleshooting.md](runbooks/troubleshooting.md)

### Extending the system
1. [development/adding_new_stage.md](development/adding_new_stage.md)
2. [development/adding_new_factor.md](development/adding_new_factor.md)
3. [development/adding_new_publisher.md](development/adding_new_publisher.md)
4. [development/adding_new_api_endpoint.md](development/adding_new_api_endpoint.md)

### Decisions
[decisions/](decisions/) — ADRs explaining major architectural choices.

## Conventions

- Every doc starts with frontmatter (title, purpose, audience, last verified, source of truth).
- All commands are copy-pasteable from repo root.
- All paths are relative to repo root.
- Old docs live under [`_legacy/`](_legacy/README.md) — do not link from current docs.
- See [`DOCS_STANDARD.md`](DOCS_STANDARD.md) for the doc writing standard.
- Run `python scripts/check_docs.py` before merging a docs PR.

## Audit trail

The current doc set was rebuilt on 2026-05-16. See:
- [`_audit/documentation_inventory.md`](_audit/documentation_inventory.md)
- [`_audit/stale_reference_report.md`](_audit/stale_reference_report.md)
- [`_audit/current_code_truth_map.md`](_audit/current_code_truth_map.md)
- [`_audit/documentation_cleanup_report.md`](_audit/documentation_cleanup_report.md) — final report
