# Trade Journal MCP Tools

- **Purpose:** Agent-facing contract for the private, account-scoped journal MCP server.
- **Audience:** Operators, developers, and AI agents.
- **Last verified:** 2026-08-28
- **Source of truth:** `src/ai_trading_system/interfaces/journal_mcp/`.

---

`ai-trading-journal-mcp` is separate from `ai-trading-mcp`. It reads one
configured account from `$DATA_ROOT/trade_journal.duckdb` and exposes no
mutation capability.

## Temporal contract

- `portfolio_as_of` is the economic cutoff for portfolio evidence.
- `known_at` is an ISO date or timestamp limiting recording and analysis
  completion. A date includes that full day.
- A historical reconstruction is eligible only when the complete persisted
  reconstruction has no position effective after `portfolio_as_of`.
- Missing historical evidence returns `NO_DATA_AS_OF`; current evidence is not
  substituted.

Every response uses the standard MCP `data`/`meta` envelope. Metadata contains
an opaque `account_scope`, never the stored `account_ref`.

## Tools

| Tool | Purpose | Primary reads |
|---|---|---|
| `describe_schema` | Explain scope, trust, temporal, and column meanings. | constants |
| `get_journal_overview` | Reconstruction/analysis, position/open-episode counts, and unresolved DQ counts. | analysis, reconstruction, episodes, DQ |
| `get_journal_positions` | Non-zero reconstructed positions from one completed version. | reconstruction, aliases |
| `list_trade_episodes` | Bounded zero-to-zero episode history. | episodes, fill links |
| `get_trade_episode` | One account-owned episode with optional fills and annotations. | episodes, fills, annotations |
| `get_trade_evaluations` | Version-pinned entry/add/exit process evaluations. | evaluations, fills |
| `get_portfolio_journal_series` | Holdings-only portfolio risk observations. | risk snapshots |
| `get_journal_reconciliation` | Latest eligible or identified reconciliation and discrepancies. | reconciliation tables |
| `get_journal_data_quality` | Sanitized issue status without raw evidence payloads. | DQ issues |

Limits are clamped server-side. Positions, episodes, evaluations, and DQ cap at
500 rows; portfolio series caps at 1,000.

## Privacy and claim limits

The server does not expose account enumeration, raw imported rows, raw import
metadata, file hashes, task errors, reviewer identities, annotation authors,
or DQ evidence JSON. Annotations require `include_annotations=true`.

`realised_gross_pnl` excludes charges and taxes. `holdings_only` series exclude
cash and account flows. `securities_only` reconciliation compares broker and
reconstructed security inventory. These are not NAV, net-return, TWR, or XIRR
claims.

## Registration

The repo-root `.mcp.json` and `opencode.json` register the server. Set
`DATA_ROOT`; for a multi-account journal also set
`AI_TRADING_JOURNAL_MCP_ACCOUNT_REF`. With exactly one journal account, the
server pins it automatically without exposing its identifier to the agent.
