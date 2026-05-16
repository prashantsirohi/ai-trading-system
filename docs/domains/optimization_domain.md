# Optimization Domain

- **Purpose:** Research-only strategy rule-pack tuning via Optuna. Walk-forward validation, overfitting controls, mutation rules. Not part of the operational pipeline.
- **Audience:** Researcher, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/research/optimization/`](../../src/ai_trading_system/research/optimization/)

---

## Responsibility

Search the space of strategy rule packs for improved baseline performance, without contaminating production with overfit results. Output is a candidate rule pack + diagnostics; promotion is a manual decision.

## Package / module ownership

| Module | Role |
|---|---|
| `optimization/store.py` | DuckDB-backed Optuna trial storage. |
| `optimization/` (others) | TPE search, walk-forward, mutation rules, reports. **Verify exact module list when writing this doc deeper.** |

## Public contracts

- Trial store in DuckDB (path: verify — may be `data/research.duckdb` or a dedicated file).
- Optimization reports under `data/research/` (verify exact subdir).

## Storage ownership

- Optuna trial tables in research DuckDB.

## Dependencies

- External: Optuna.
- Internal: `research/backtesting/` for evaluating candidate rule packs.
- Reads `config/strategies/` rule packs as starting points.

## Extension points

- New objective: extend the TPE objective in `optimization/`.
- New mutation: add to mutation rule set.
- New validation split: extend walk-forward driver.

## When not to use

- Small backtest windows where overfitting risk dominates Sharpe gain.
- Without walk-forward validation enabled.
- Before defining a baseline you want to beat.

## Known gaps

- Existing doc `docs/architecture/strategy-optimizer.md` covers more detail (Phases 0–4 implemented, 5–6 planned). Migrate into this file then archive.

## See also

- [`docs/architecture/strategy-optimizer.md`](../_legacy/archived_2026-05-16/architecture_strategy-optimizer.md) (to be migrated then archived)
- [`docs/domains/research_domain.md`](research_domain.md)
