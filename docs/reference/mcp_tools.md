# MCP Tools

- **Purpose:** Catalog of the read-only MCP tool surface: parameters, response shape, point-in-time support, and the store each tool reads.
- **Audience:** Operators, developers, and AI agents consuming the server.
- **Last verified:** 2026-08-14
- **Source of truth:** `src/ai_trading_system/interfaces/mcp/server.py`, `.../tools/*.py`, `.../readers/*.py`, `.../schema_catalog.py`.

---

The server exposes the trading system's read surfaces over stdio so an agent can
answer questions without searching the repository for where data lives. Start
the server with `ai-trading-mcp`; see [commands](commands.md#read-only-mcp-server).

## Response envelope

Every tool returns the same shape:

```json
{
  "data": [],
  "meta": {
    "source": "ohlcv.duckdb:_catalog_feature_source",
    "as_of_status": "EXACT",
    "as_of_requested": "2026-01-06",
    "as_of_effective": "2026-01-06",
    "row_count": 2,
    "notes": []
  }
}
```

`meta.source` names the file as well as the table, because two different tables
are named `sector_earnings_leadership` (one in `fundamentals.duckdb`, one in
`ohlcv.duckdb`) and likewise `valuation_cycle_features`.

### `as_of_status`

| Value | Meaning | `data` |
|---|---|---|
| `LATEST` | No cutoff was requested. | rows |
| `EXACT` | Point-in-time; every row is at or before `as_of`. | rows |
| `NO_DATA_AS_OF` | Point-in-time supported, nothing existed by then. | empty |
| `AS_OF_UNSUPPORTED` | The surface is latest-only and cannot be cut off. | empty |

A historical request never returns data published after the requested date, and
`AS_OF_UNSUPPORTED` returns no rows rather than substituting the present.
`envelope.assert_not_future` raises if a tool's cutoff is missing or wrong, so a
leak fails loudly instead of answering incorrectly.
Malformed `as_of` values are rejected rather than being interpreted as an
unbounded latest-data request.

## Tools

| Tool | Purpose | `as_of` | Reads |
|---|---|---|---|
| `describe_schema` | Column dictionary for a surface: type, meaning, units, owning store, stage-vocabulary mapping. | n/a | constants |
| `resolve_symbol` | Ticker, name, ISIN or security id to master candidates. | `AS_OF_UNSUPPORTED` | `masterdata.db:symbols` |
| `get_symbol_profile` | Identity, quote, stage, rank and fundamentals in one call. | `EXACT` | composed |
| `get_ohlcv` | Daily candles plus delivery percentage. | `EXACT` | `ohlcv.duckdb:_catalog[_feature_source]`, `_delivery` |
| `get_technical_features` | Nine indicator families plus Phase 1 risk/liquidity features. | `EXACT` | `feature_store/*.parquet`, `ohlcv.duckdb:feat_phase1_symbol_features` |
| `get_stage_history` | Weinstein stage observations from a chosen store. | `EXACT` | see [stage stores](#stage-stores) |
| `get_rank_detail` | Newest ranked row with the factor breakdown. | `EXACT` | `control_plane.duckdb:rank_history` |
| `get_rank_history` | Rank position over time. | `EXACT` | `control_plane.duckdb:rank_history` |
| `screen_universe` | Cross-sectional filter over the ranked universe. | `EXACT` | `rank_history` + governed stage observations |
| `get_sector_overview` | Stage distribution per sector. | `EXACT` | governed stage observations |
| `get_sector_constituents` | Symbols in a sector with stage, rank and market cap. | `EXACT` | governed stage observations; current master enrichment only for latest requests |
| `get_fundamentals` | Five fundamental blocks. | `EXACT` | `screener_financials.db`, `fundamentals.duckdb` |

Row limits are clamped server-side. Most tools default to 250 rows and cap at
2000; `screen_universe` defaults to 50 and caps at 500. `meta.truncated` is set
whenever a cap clipped the result.

## Price basis

`_catalog.close` is **unadjusted**. Every technical feature is computed on
`COALESCE(adjusted_*, raw)` exposed through the `_catalog_feature_source` view.
`get_ohlcv` therefore defaults to `adjusted=true` and always reports
`meta.price_basis`; comparing raw candles against `sma_200` from the feature
store would be wrong across any split or bonus.

## Stage stores

Three stores hold stage state with different coverage and grain. `granularity`
selects one, and `meta.coverage` always reports that store's window.

| `granularity` | Store | Grain | Notes |
|---|---|---|---|
| `weekly_governed` (default) | `control_plane.duckdb:weekly_stock_stage_history` + `stage_observation_governance` | weekly | Exchange-aware, canonical vocabulary including transition states; correction authority and publication availability are resolved at the requested cutoff. |
| `weekly_legacy` | `ohlcv.duckdb:weekly_stage_snapshot` | weekly | No `exchange` column; coverage typically ends well before the governed store begins. |
| `daily` | `control_plane.duckdb:stage_history` | daily | Version-pinned; legacy `S1..S4` spelling in the store. |

## Stage vocabulary

Two spellings exist in the stores. Every stage-bearing row carries all four
fields so neither a canonical nor a legacy filter silently misses rows:

| Field | Meaning |
|---|---|
| `stage_label` | Canonical `WeinsteinStage` value; always populated. |
| `stage_label_legacy` | Legacy `S1..S4`/`UNDEFINED` code. **Null** for the four transition states, which the legacy vocabulary cannot express. |
| `stage_family` | `stage_1..stage_4` or `unknown`. A transition reports the stage it is leaving, so family filters still match. |
| `is_transition` | True for the four transition states. |

`screen_universe` accepts either spelling in `stage_label` (exact match) and a
family in `stage_family_filter` (looser, admits transitions).
`describe_schema("stage")` returns the full mapping.

Governed stage reads use the same correction-resolution contract as the
pipeline. Superseded observations disappear only when their authoritative
correction was recorded and available by the query cutoff. Historical sector
and screening requests derive classification solely from those governed
observations; they never fill missing sectors from today's symbol master.

## Fundamentals and publication dates

Cutoffs use the **publication** date, not the fiscal period: a quarter ending
2025-12-31 is not knowable on 2026-01-05. `meta.as_of_basis` names the column
used per block.

| Block | Source | Cutoff column |
|---|---|---|
| `financials` | `screener_financials` | `available_at` (bitemporal, in the primary key) |
| `growth` | `company_growth_features` | `available_at` |
| `valuation` | `screener_market_valuation` | `date` (price date) |
| `scores` | `fundamental_scores` | `snapshot_date` (export date, a publication proxy) |
| `snapshot` | `fundamental_snapshot` | `snapshot_date` (same basis) |
| `company` | `screener_company_snapshot` | `as_of_date` |

`statement_basis` is `standalone` (the pipeline default) or `consolidated`.
The two live under separate keys and are never blended;
`meta.available_statement_bases` reports what is actually stored.

## Identity

A symbol is `(symbol_id, exchange)`. `resolve_symbol` returns **candidates**
rather than a single row and sets `meta.ambiguous` when more than one listing
ties at the best match tier, so a dual listing is never silently collapsed to
one exchange. Every other tool takes an explicit `exchange`, defaulting to
`NSE`.

## Concurrency with a running pipeline

DuckDB refuses a read-only attach while another process holds the file
read-write, so while a pipeline run is in progress the tools backed by
`ohlcv.duckdb` (`get_ohlcv`, `get_technical_features`, and the quote block of
`get_symbol_profile`) raise `StoreBusyError` with an explicit message. This is
deliberate: an empty result would read as "no such data".

Tools backed by `control_plane.duckdb`, `masterdata.db`,
`screener_financials.db` and `fundamentals.duckdb` are unaffected unless that
specific store is being written, so stage, rank, sector, screening and
fundamentals questions still answer during a run. Retry the price and technical
tools once the run finishes.

## Rotating universes

The ranked universe is a top-N cross-section, so a symbol can be ranked on one
session and absent the next. `get_rank_detail` resolves the effective date
**per symbol**, returning that symbol's most recent ranking at or before
`as_of` and reporting its true date in `meta.as_of_effective` — rather than
reporting "never ranked" because it is missing from the newest session.
`screen_universe`, being a cross-section, pins to a single session as expected.

## Safety

- Every DuckDB handle opens `read_only=True`; every SQLite handle uses a
  `file:...?mode=ro` URI. Parquet is scanned through an in-memory DuckDB
  connection that holds no store file. Enforced by an autouse test fixture that
  intercepts both connection constructors.
- `interfaces/mcp/**` never imports execution, trade-journal, broker, or
  pipeline-orchestration code; enforced by `tests/lint/test_layer_boundaries.py`.
- All SQL is parameterized; table names come only from trusted internal
  constants. Feature-store paths are containment-checked.
- The `operator` profile requires an explicit external `DATA_ROOT`.

See [ADR-0008](../decisions/ADR-0008-read-only-mcp-interface.md) for the
reasoning behind these invariants.
