# Execution Domain

- **Purpose:** Convert candidates into orders and fills, persist execution state, and enforce risk policies. Paper trading is the default and only verified path.
- **Audience:** Developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/domains/execution/`](../../src/ai_trading_system/domains/execution/), [`src/ai_trading_system/domains/risk/`](../../src/ai_trading_system/domains/risk/), [`src/ai_trading_system/pipeline/stages/execute.py`](../../src/ai_trading_system/pipeline/stages/execute.py)

---

## Live-trading disclaimer

**Paper trading is the only verified execution path.** The `DhanExecutionAdapter` scaffold exists at [`adapters/dhan.py`](../../src/ai_trading_system/domains/execution/adapters/dhan.py) but `place_order` raises `RuntimeError` unless `dry_run=True` ([`dhan.py:62-65`](../../src/ai_trading_system/domains/execution/adapters/dhan.py)). The stage wrapper hardcodes `PaperExecutionAdapter` ([`execute.py:187`](../../src/ai_trading_system/pipeline/stages/execute.py)). Live trading guardrails (broker reconciliation, kill-switch, position caps enforced against broker state) have **not been audited**. Do not enable live execution without a separate hardening pass.

## Responsibility

Decide what to trade and at what size, persist the decisions and any (paper) fills, and surface portfolio state. Risk gates run **before** adapter dispatch.

## Package / module ownership

| Module | Role |
|---|---|
| `service.py::ExecutionService` | Top-level orchestrator. |
| `autotrader.py` | Entry/exit signal evaluation, order generation. |
| `policies.py` | Risk gate logic (sizing, position limits, heat). |
| `models.py` | Pydantic Order, Fill classes. Order type currently hardcoded MARKET + INTRADAY. |
| `store.py::ExecutionStore` | Reads/writes `execution_order` + `execution_fill` tables. |
| `portfolio.py::PortfolioManager` | In-memory position tracking. |
| `adapters/paper.py::PaperExecutionAdapter` | Mock fills with slippage_bps; default. |
| `adapters/dhan.py::DhanExecutionAdapter` | Live broker scaffold — raises unless `dry_run=True`. |
| `domains/risk/` | Risk profile loader, guardrails. |

## Public contracts

Stage artifacts under `data/pipeline_runs/<run_id>/execute/attempt_<n>/`:

| Artifact | Purpose |
|---|---|
| `trade_actions.csv` | Proposed orders (side, qty, price, reason) |
| `executed_orders.csv` | Confirmed/rejected orders |
| `fills.csv` | Executed fills with timestamps |

**DuckDB writes:** `execution_order`, `execution_fill`, `execution_trade_note`, `execution_position_stop`, plus drawdown snapshots in `data/execution.duckdb` (default in [`store.py:29`](../../src/ai_trading_system/domains/execution/store.py)). The Phase 0 truth map incorrectly claimed these lived in `control_plane.duckdb` — code disagrees and was corrected in Phase 6.

## Storage ownership

- All `execution_*` tables in `data/execution.duckdb` — sole writer.
- Execute stage artifacts.

## Dependencies

- Reads candidates + rank artifacts from prior stages.
- Reads risk profiles from `config/risk_profiles/` (`RISK_PROFILE` env var selects).

## Extension points

- New adapter: subclass the adapter interface, gate it behind a feature flag.
- New risk rule: add to `policies.py` or `domains/risk/`.
- New order type: extend `models.py` (currently hardcoded MARKET + INTRADAY).

## Known gaps

- Live broker integration not production-ready (see disclaimer above).
- Order type fixity (MARKET+INTRADAY).
- Stop-loss / trailing-stop implementation status: **Current code status: unknown — verify before relying on this.**

## See also

- [`docs/stages/execute.md`](../stages/execute.md)
- [`docs/reference/execution_policy.md`](../reference/execution_policy.md)
