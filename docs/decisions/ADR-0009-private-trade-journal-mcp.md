# ADR-0009: Private Account-Scoped Trade Journal MCP

- **Purpose:** Define the safe agent boundary for actual portfolio and trading-journal evidence.
- **Audience:** Operator, developers, and AI agents.
- **Last verified:** 2026-08-28
- **Source of truth:** `src/ai_trading_system/interfaces/journal_mcp/`, `tests/interfaces/journal_mcp/`, and `tests/lint/test_layer_boundaries.py`.
- **Status:** Accepted — implemented 2026-08-28.

---

## Context

The general `ai-trading-mcp` serves market, ranking, pipeline, and opportunity
evidence. It deliberately forbids imports from the trade-journal domain. The
journal contains private broker-derived positions, fills, process evaluations,
annotations, and reconciliations, so those surfaces require a separate privacy
and discovery boundary.

Journal evidence has two independent time axes. `portfolio_as_of` is the
economic date described by a position or portfolio observation. `known_at` is
the recording or analysis-completion cutoff. A later import must not appear in
an answer about what the journal knew earlier, even when the imported trade's
economic date is older.

## Decision

Provide a separate stdio server, `ai-trading-journal-mcp`, under
`interfaces/journal_mcp/`. The server:

- opens only `$DATA_ROOT/trade_journal.duckdb` with `read_only=True`;
- imports neither `domains.trade_journal` nor execution, broker, integrations,
  or pipeline-orchestration modules;
- is pinned to one account for its entire process lifetime;
- accepts no account identifier in a tool call and never lists accounts;
- uses `AI_TRADING_JOURNAL_MCP_ACCOUNT_REF` when configured, or automatically
  selects the account only when exactly one account exists;
- returns an irreversible account-scope digest instead of `account_ref`;
- applies both economic-date and recording-time cutoffs;
- exposes only allowlisted columns and omits raw uploads, import metadata,
  reviewer/annotation authors, and DQ evidence payloads;
- exposes no imports, tasks, analysis triggers, proposals, approvals,
  annotation writes, broker access, or execution state.

The server requires journal schema `002`. Migration `002` makes
`journal_latest_analysis` select completed runs before version ranking.

## Tool boundary

The initial surface contains overview, reconstructed positions, episode list
and detail, process evaluations, holdings-only portfolio series,
reconciliation evidence, sanitized DQ status, and schema description. The
canonical contract is [Journal MCP tools](../reference/journal_mcp_tools.md).

Annotations are excluded by default. When explicitly requested, their content
is returned without `author`. Portfolio responses retain `gross`,
`securities_only`, or `holdings_only`; none may be presented as account NAV,
net return, TWR, or XIRR.

## Consequences

- Agents use the journal without source-tree or database-path discovery.
- Connecting the market MCP does not automatically grant private portfolio access.
- Multi-account journals require explicit operator configuration.
- Historical requests may return no data when no completed version satisfies
  both cutoffs.
- Layer-boundary lint covers both MCP packages and rejects unsafe imports.
