# AGENTS.md — AI Trading System Repository Rules

## Required read order

Before scanning source code:

1. Read this file.
2. Read `docs/SYSTEM_GUIDE.md` for the current system design, stages, persistence, commands, and document map.
3. Read exactly one relevant detailed document linked by the guide, expanding only when the task is genuinely cross-cutting.

Do not rediscover pipeline order, runtime paths, persistence ownership, artifact layout, or operator commands by scanning the repository. The guide is the canonical human-readable orientation; current code is the runtime authority.

## Branching

The default work branch is `main`.

- Do not create, switch to, or push a feature branch unless the operator explicitly requests a branch or PR workflow.
- When asked to push without another branch being named, push the current `main` to `origin/main`.
- Before code changes, run `git status --short --branch`. If the checkout is not on `main`, stop and ask.
- Preserve unrelated changes in a dirty worktree.

## Runtime data and paths

Runtime data is external in the operator environment. Load `.env` and resolve live data through the repository path helpers and `DATA_ROOT`.

- Never hardcode repo-local `data/...` in application code.
- Never hardcode the operator's `/Volumes/...` setting in application code.
- Do not create a second repo-local data tree unless explicitly requested.
- Tests must use temporary directories, not live runtime stores.
- Before changing path, DB, artifact, model, report, or log behavior, read `docs/architecture/storage_and_lineage.md` and inspect how the existing path helpers propagate the configured roots.

## Data trust and execution safety

- Preserve the operational source-of-record and fallback contract documented in the System Guide and data-source reference.
- Synthetic smoke data is disabled; do not use generated market data to bypass trust checks.
- Preserve quarantine and critical DQ blocking behavior.
- Paper execution is the safe default.
- Do not enable live broker placement unless the operator explicitly asks.
- Preview and diagnostic paths must not mutate broker state.
- Before DB migration or repair, back up affected live stores. Do not mutate live databases unless the task explicitly includes it.

## SQL safety

Parameterize all user-controlled and market-data values in DuckDB queries:

```python
conn.execute(
    "SELECT * FROM _catalog WHERE symbol = ? AND date = ?",
    [symbol, trade_date],
)
```

- Do not interpolate symbols, exchanges, dates, IDs, or other values into `WHERE` clauses.
- For `IN` lists, use supported list binding or safely constructed placeholders.
- Table names cannot be parameterized; dynamic table names are allowed only from trusted internal constants.

## Credentials and secrets

Never commit or print:

- `.env`
- Broker credentials, access tokens, or OTP seeds
- Telegram bot tokens or chat IDs
- Google OAuth/service-account files
- Any other production secret

## Implementation workflow

When asked to change code:

1. Confirm `main` and inspect the dirty worktree.
2. Identify the affected stage/domain from `docs/SYSTEM_GUIDE.md`.
3. Read the relevant detailed contract and only the necessary source files.
4. Make the smallest safe patch and preserve unrelated work.
5. Add or update targeted tests.
6. Run targeted tests; run a real-data canary with `--local-publish` when proportionate and safe.
7. Report files changed, commands/tests run, behavior change, and remaining risks.

For feature or ranking changes, state whether a full feature rebuild is required and verify rank/DQ artifacts when a canary is run. Do not claim verification that was not performed.

## Documentation maintenance

`docs/SYSTEM_GUIDE.md` must change in the same commit when a change affects system-level design, pipeline stages/defaults, persistence ownership, safety invariants, system interfaces, or common operator commands. Update the relevant detailed document as well.

Run:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/check_docs.py
```

Follow `docs/DOCS_STANDARD.md` and `docs/development/docs_update_checklist.md`. Do not create another current high-level system summary.

## Current review themes

Before patching a previously reported issue, check whether current code already fixes it. Recurring high-risk areas include:

- Stop-price and trailing-stop behavior.
- Parameterized DB queries and DuckDB writer serialization.
- Portfolio exposure and capital-at-risk gates.
- Trust-envelope and package migration compatibility.
- Sector demeaning, trend scoring, delivery imputation, and rank stability.

These are review prompts, not assertions that the defects remain present.
