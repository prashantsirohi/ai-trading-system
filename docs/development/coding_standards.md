# Coding Standards

- **Purpose:** Code and doc writing standards in one place.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** Existing patterns in `src/ai_trading_system/`; `docs/DOCS_STANDARD.md` for the doc half.

---

## Code

- **Python version:** see `pyproject.toml [project] requires-python`.
- **Layout:** new code lives in `src/ai_trading_system/`. Do not add new top-level packages without a discussion.
- **Domain boundary:** if a change is more than ~50 lines and crosses domains, split into per-domain commits.
- **Imports:** absolute imports from `ai_trading_system.*`; avoid `from x import *`.
- **Type hints:** required on public functions / class methods. Use `from __future__ import annotations` at file top.
- **Error handling:** raise specific exceptions; don't swallow silently. Pipeline stages may broadly catch only when the stage is documented as non-blocking (e.g. `perf_tracker`).
- **Logging:** use the `logging` module + `logger = logging.getLogger(__name__)`. No `print()` in library code.
- **Side effects:** keep `__init__.py` files import-light. Heavy imports go in functions.
- **Config:** read env vars via `platform/config/` patterns, not scattered `os.getenv(...)` calls.

## Tests

- Use `pytest`. Tests live under `tests/<area>/test_*.py`.
- Prefer testing through public contracts (stage wrappers, service classes) over private helpers.
- For DuckDB / file-touching tests, use the existing fixtures in `tests/fixtures/`.

## Docs

See [`docs/DOCS_STANDARD.md`](../DOCS_STANDARD.md). Key rules:

- Every doc has a frontmatter block: title, purpose, audience, last verified, source of truth.
- Cite file paths (and line numbers when load-bearing) for any claim about behavior.
- "Current code status: unknown — verify before relying on this" is preferred over guessing.
- Do not duplicate large blocks across docs — link.
- All commands must be copy-pasteable from repo root.

## Anti-patterns

- Don't add abstractions for single-use code.
- Don't "improve" adjacent code in a PR not about it.
- Don't add config flags without a real consumer.
- Don't hardcode `data/*.duckdb` paths — use `platform/db/paths.py`.

## See also

- [`testing_strategy.md`](testing_strategy.md)
- [`docs_update_checklist.md`](docs_update_checklist.md)
