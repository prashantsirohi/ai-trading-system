# Actual Trading Journal Architecture and Data Contract

- **Purpose:** Define the implemented broker-import, reconstruction, reconciliation, and analytical journal boundary.
- **Audience:** Operators, developers, and reviewers.
- **Last verified:** 2026-08-08
- **Source of truth:** `src/ai_trading_system/domains/trade_journal/`, its packaged migrations, and `ui/execution_api/routes/trade_journal.py`.

---

## Boundary and ownership

The on-demand `trade_journal` bounded domain is not a daily-pipeline stage. It owns `$DATA_ROOT/trade_journal.duckdb`; it neither shares nor updates `execution.duckdb`. Market enrichment opens operational OHLCV read-only. Broker-state mutation and external publishing are outside this domain.

The database is resolved only through `trade_journal_db_path()`. Runtime access verifies schema version `001` and fails closed when the database is absent or behind. `ai-trading-journal migrate --apply` backs up an existing regular file before applying packaged SQL. All writers use a store-adjacent inter-process lock and explicit DuckDB transactions.

## Import and identity contract

Dhan tradebooks discover the `Equity` header by its complete economic-field signature. Dhan holdings CSV uses `utf-8-sig`, removes only wholly blank trailing columns, and retains reported values. Required economic formulas, invalid decimals, missing schemas, and non-positive quantities fail parsing. Raw rows, hashes, metadata, run status, and DQ evidence are retained.

File identity is SHA-256 scoped by broker, account and file type. Fills are scoped by broker, account, exchange, trade date and trade ID; orders use the same scope with order ID. A replay of the same file is a no-op. Matching identifiers with different economics are blocking conflicts and never overwrite an earlier row.

Valid ISIN is the primary identity. Same-file symbol evidence can resolve a malformed row only when that symbol has exactly one valid ISIN in the file. Symbols sharing a valid ISIN retain alias evidence. Holdings resolve through validity-bounded journal aliases first, then an unambiguous symbol/ISIN match in the operational SQLite master opened read-only; that evidence is appended to `identity_resolution`. Different ISINs are not silently merged. Ambiguous identities remain review-required.

## Reconstruction and reconciliation

Cash-equity reconstruction orders fills deterministically and retains FIFO disposal and weighted-average observations. A sell beyond known inventory records blocking `POSITION_DEFICIT`, marks the reconstruction untrusted, and creates no fabricated short lot. Episodes are zero-to-zero cycles and can span additions, trims, aliases and calendar years.

Holdings default to `reconciliation_only`: a snapshot and reconciliation are appended but no portfolio event or lot is created. `opening_anchor` is an explicit provenance-tagged bootstrap mode. Reconciliation reports exact quantity plus FIFO, weighted-average, and broker cost evidence using versioned absolute/relative tolerances. Its identity includes the immutable checkpoint and applicable ledger/cost inputs: an unchanged rerun is a no-op, while newly available earlier fills create a new historical version. Conflicting checkpoints remain historical evidence rather than being overwritten.

## Point-in-time and analytics contract

Entry and add context ends at the previous observed trusted exchange session. Exit decision context also uses the previous session; same-day ranges are descriptive only. Forward returns and MFE/MAE begin with the next session. Missing or quarantined data produces incomplete/untrusted evidence, never a current-value substitute. No feature rebuild is required.

Active NSE split/bonus rows in operational `_corporate_actions` are read-only evidence. Analysis appends relevant rows as `PROPOSED` journal corporate actions with their source key and content hash; they do not affect lots, quantities, or costs until an operator records a separate approval.

Component scoring requires at least 80% coverage. Behaviour findings require five eligible observations and three occurrences and include a 95% Wilson interval. Process quality and ex-post outcomes are separate. API scope labels are `gross`, `securities_only`, or `holdings_only`; NAV, cash allocation, net returns, TWR, XIRR and total-account return are not claimed without future funds and charges inputs.

Valuation quantities come only from fills accepted by lot matching plus approved non-trade events. Deficit sells never enter valuation as short positions. The holdings-only drawdown series compounds close-to-close price P&L on prior-session positive holdings, which removes trade cash-flow jumps but remains subject to daily-bar timing ambiguity. Drawdown attribution sums those price-P&L components between the return-index peak and trough. Market value and exposure series remain securities-only snapshots, not NAV.

Reconstruction, reconciliation and analysis API requests append a journal task request, then use the existing operator subprocess tracker. Only an opaque journal run ID and journal database path are placed on the worker command line; the worker resolves the account inside the journal database. Raw account identifiers and uploaded file content are therefore absent from task commands and logs.

## Schema families

Migration `001_initial.sql` creates append-oriented families for import provenance and DQ; identity, aliases and governance proposals; fills, orders, events, lots, disposals, weighted averages and episodes; snapshots, reconstructions and reconciliations; and versioned contexts, evaluations, annotations, valuations, risk and policy breaches. Authoritative financial columns are `DECIMAL(38,8)`. Latest views select successful versions without replacing history.
