# Docs Update Checklist

- **Purpose:** Run through this whenever code that affects docs changes.
- **Audience:** Developer.
- **Last verified:** 2026-07-13
- **Source of truth:** This file; cross-checked against `docs/DOCS_STANDARD.md`.

---

For every PR that touches code, ask:

- [ ] Did the system-level design, pipeline/defaults, persistence ownership, safety contract, system interface, or common operator workflow change? → update [`docs/SYSTEM_GUIDE.md`](../SYSTEM_GUIDE.md) and the relevant detailed document
- [ ] Did public API change? → update [`docs/reference/api_reference.md`](../reference/api_reference.md)
- [ ] Did an artifact schema change? → update [`docs/reference/artifacts.md`](../reference/artifacts.md) and the relevant `docs/stages/*.md`
- [ ] Did a command change (CLI flag added/removed/renamed)? → update [`docs/reference/commands.md`](../reference/commands.md) and [`docs/reference/configuration.md`](../reference/configuration.md)
- [ ] Did an env var change? → update [`docs/reference/environment_variables.md`](../reference/environment_variables.md)
- [ ] Did a DQ rule change? → update [`docs/architecture/data_trust_and_dq.md`](../architecture/data_trust_and_dq.md), `docs/runbooks/dq_failure_response.md`, and the relevant stage doc
- [ ] Did a publisher channel change? → update [`docs/reference/publish_contracts.md`](../reference/publish_contracts.md) and [`docs/stages/publish.md`](../stages/publish.md)
- [ ] Did a ranking factor change? → update [`docs/reference/ranking_factors.md`](../reference/ranking_factors.md)
- [ ] Did a pattern detector / breakout family change? → update [`docs/reference/breakout_and_patterns.md`](../reference/breakout_and_patterns.md)
- [ ] Did execution policy / risk gates change? → update [`docs/reference/execution_policy.md`](../reference/execution_policy.md)
- [ ] Did storage layout change (new DuckDB, new table, new feature-store dir)? → update [`docs/SYSTEM_GUIDE.md`](../SYSTEM_GUIDE.md), [`docs/architecture/storage_and_lineage.md`](../architecture/storage_and_lineage.md), and [`docs/reference/database_schema.md`](../reference/database_schema.md)
- [ ] Did a new stage land in `PIPELINE_ORDER`? → add `docs/stages/<new>.md`, update [`docs/SYSTEM_GUIDE.md`](../SYSTEM_GUIDE.md), `docs/architecture/operational_data_flow.md`, and [`docs/INDEX.md`](../INDEX.md)
- [ ] Docs validation passes? → `PYTHONPATH=src ./.venv/bin/python scripts/check_docs.py`

## Frontmatter update

If you touched a doc, bump its `Last verified:` date to today.

## Anti-checklist

- Do **not** duplicate large blocks across docs. Link instead.
- Do **not** add aspirational claims ("we plan to..."). State what code does today.
- Do **not** describe live trading as production-ready unless explicit code guardrails prove it.
- Do **not** add another current jumpstart, system overview, stage map, persistence map, or operator quick-start document; link to the System Guide.
