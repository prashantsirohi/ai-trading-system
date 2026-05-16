# Testing Strategy

- **Purpose:** Test layers, scope, and what each is for.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `tests/` directory layout.

---

## Layers

| Layer | Location | Scope | Speed |
|---|---|---|---|
| Unit | `tests/<domain>/test_*.py` | Single class/function | Fast (ms) |
| Integration | `tests/integration/` | Multi-module flows; DuckDB writes | Medium (seconds) |
| Smoke | `tests/smoke/` | Cross-cutting sanity checks | Fast |
| Lint | `tests/lint/` | AST-based layer-boundary checks (e.g. routes don't import services directly) | Fast |
| Pipeline canary | via `ai-trading-pipeline --canary --skip-preflight` | End-to-end on a 25-symbol slice | Slow (minutes) |
| API snapshot | (part of integration) | Verify FastAPI routes match expected schemas | Fast |
| Docs validation | `scripts/check_docs.py` | Links, frontmatter, forbidden terms | Fast |

## Running tests

```bash
# Whole suite (slow)
pytest

# A single domain
pytest tests/ingest/ -x

# Just smoke
pytest tests/smoke/

# Lint layer boundaries
pytest tests/lint/

# Docs validation
python scripts/check_docs.py
```

## When to add what

| Change type | Required tests |
|---|---|
| New CLI flag | Unit on the parser + integration on the stage that consumes it |
| New stage | Integration through the orchestrator + smoke that includes the stage |
| New publisher channel | Unit on the channel + integration with `delivery_manager` + dedupe-key test |
| New API endpoint | Route test (status code, schema) + service-layer test |
| New ranking factor | Unit on `factors.py` math + integration that confirms it appears in `ranked_signals.csv` |
| New DQ rule | Unit on the rule predicate + integration that confirms it blocks/passes the stage |

## Pipeline canary

The canary path is the closest thing to production verification:

```bash
ai-trading-pipeline --canary --skip-preflight --stages ingest,features,rank,publish --local-publish
```

It exercises real DuckDB writes and real artifact production on a 25-symbol slice. Run before any release.

## Import smoke tests

`pytest tests/smoke/` includes import-only checks for the canonical package — these catch regressions like a renamed module breaking `pyproject.toml [project.scripts]` entrypoints.

## See also

- [`contributing.md`](contributing.md)
- [`adding_new_stage.md`](adding_new_stage.md), [`adding_new_factor.md`](adding_new_factor.md), [`adding_new_publisher.md`](adding_new_publisher.md), [`adding_new_api_endpoint.md`](adding_new_api_endpoint.md)
