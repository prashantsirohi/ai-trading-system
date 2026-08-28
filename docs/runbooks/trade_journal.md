# Actual Trading Journal Operator Runbook

- **Purpose:** Safely initialize, preview, import and inspect the local journal.
- **Audience:** Local operator.
- **Last verified:** 2026-08-28
- **Source of truth:** `ai_trading_system.domains.trade_journal.cli` and the execution API journal router.

---

Load `.env`, confirm `DATA_ROOT`, and back up operational storage before migrations. Never use supplied account exports as committed test fixtures.

```bash
set -a
source .env
set +a
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.trade_journal migrate --apply
```

Preview first; preview does not write journal state:

```bash
ai-trading-journal import-tradebook --broker dhan --account ACCOUNT --file /path/tradebook.xlsx --preview
ai-trading-journal import-holdings --broker dhan --account ACCOUNT --as-of YYYY-MM-DD --market-state eod --mode reconciliation_only --file /path/holdings.csv --preview
```

Apply only after reviewing the hash, row counts, schema, date range and DQ issues:

```bash
ai-trading-journal import-tradebook --broker dhan --account ACCOUNT --file /path/tradebook.xlsx --commit
ai-trading-journal import-holdings --broker dhan --account ACCOUNT --as-of YYYY-MM-DD --market-state eod --mode reconciliation_only --file /path/holdings.csv --commit
ai-trading-journal reconstruct --account ACCOUNT
ai-trading-journal reconcile --account ACCOUNT
ai-trading-journal analyze --account ACCOUNT
```

Opening lots, manual adjustments and split/bonus actions use separate `propose-*` and `approve-*` commands. Each mutation also requires `--commit`; approval records the reviewer and causes a new reconstruction instead of modifying historical derived rows.

`POSITION_DEFICIT`, identity ambiguity, collision, or checkpoint conflict is not a reason to fabricate inventory. Import earlier tradebooks or use the reviewed proposal/approval workflow before trusting the account. `reconciliation_only` holdings are evidence, not historical opening inventory.

The execution API exposes `/api/trade-journal`. Multipart uploads are capped at 25 MiB. Commit requires resubmitting the file with the exact preview SHA-256 and `X-API-Key`; the server keeps no upload cache. Decimal values are strings. The Operator Workspace uses the same in-memory local credential for Phase 4 and execution services.

Use the workspace task controls for reconstruction, reconciliation, and analysis; status is polled by opaque journal run ID. Reconciliation rows open exact broker/FIFO/weighted-average evidence. Episode rows open trusted adjusted candlesticks with fill markers and append-only annotation revisions. Opening inventory and corporate actions remain two-step proposal/approval operations. Treat `PARTIAL`, `BLOCKED`, `CONFLICT`, and `UNTRUSTED` banners as hard scope limitations, not cosmetic warnings.

Portfolio drawdown is `holdings_only`: it compounds close-to-close price P&L on positions held at the preceding session close. It is not account NAV and cannot represent intraday execution timing, cash, charges, dividends, taxes, or transfers. Stop heat is available only for open episodes with a reviewed intended stop.

For characterization, point `TRADE_JOURNAL_SAMPLE_TRADEBOOK` and `TRADE_JOURNAL_SAMPLE_HOLDINGS` at local files and run the opt-in test. It creates only a temporary DuckDB fixture. Never point a test at `$DATA_ROOT/trade_journal.duckdb`.

## Read-only agent access

After backing up the journal, apply schema `002` with the migration command at
the top of this runbook. The repo-root `.mcp.json` and `opencode.json` register
`ai-trading-journal-mcp`. A single-account journal is pinned automatically. For
multiple accounts, place the selected account in the local, uncommitted `.env`:

```bash
AI_TRADING_JOURNAL_MCP_ACCOUNT_REF=ACCOUNT
```

Restart the agent session after changing MCP configuration or `.env`. Verify
tool discovery without opening the live store:

```bash
ai-trading-journal-mcp --list-tools
```

The server is read-only and exposes no import, reconstruction, analysis,
annotation-write, approval, broker, or execution operation. See
[Journal MCP tools](../reference/journal_mcp_tools.md).
