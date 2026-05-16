# Contributing

- **Purpose:** How to contribute changes to this repo safely.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** This file + the repo `.docs-pr-checklist.md` content (relocated here as part of docs cleanup).

---

## Before you start

1. Read [`docs/CLAUDE.md`](../../CLAUDE.md) (or root `claude.md`) — behavioral guidelines that apply to humans too: think before coding, simplicity first, surgical changes, goal-driven execution.
2. Read [`docs/DOCS_STANDARD.md`](../DOCS_STANDARD.md) — code-as-source-of-truth rules.
3. Skim [`docs/INDEX.md`](../INDEX.md) to know where things live.

## Branching

- Branch off `main` (or whatever the repo default is).
- Branch name: short, kebab-case, optionally prefixed by topic (`fix-`, `feat-`, `docs-`).

## Tests

- Run the relevant test subset before pushing:
  - `pytest tests/<area>/ -x` — fail fast on the touched area
  - `pytest tests/smoke/` — smoke tests
  - `pytest tests/integration/` — integration (slower)
- Add a test for the change. If the change is a bugfix, add a test that fails on the old behavior and passes on the new.

## Docs

For any code change that crosses the [`docs/development/docs_update_checklist.md`](docs_update_checklist.md) bar — public API, artifact schema, command, env var, DQ rule — update docs in the same PR.

## PR expectations

- Title: concise and descriptive
- Description: what changed and why, not how
- Link to relevant docs
- Tests pass
- Docs checklist completed
- No clobbered unrelated content (`git diff` should be surgical)

## Commits

- Atomic commits preferred (one logical change per commit)
- Imperative present tense ("Add fundamentals stage", not "Added")

## See also

- [`coding_standards.md`](coding_standards.md)
- [`testing_strategy.md`](testing_strategy.md)
- [`docs_update_checklist.md`](docs_update_checklist.md)
