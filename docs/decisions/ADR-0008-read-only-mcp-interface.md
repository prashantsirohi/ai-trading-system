# ADR-0008: Read-Only MCP Interface for AI Agents

- **Purpose:** Record why the trading system exposes its read surfaces through a strictly read-only MCP server, and fix the four invariants that make an agent's answers trustworthy.
- **Audience:** Operator (decision owner), developers, future agents.
- **Last verified:** 2026-08-14
- **Source of truth:** `src/ai_trading_system/interfaces/mcp/` (`server.py`, `context.py`, `envelope.py`, `schema_catalog.py`, `tools/`, `readers/`), `tests/interfaces/mcp/conftest.py`, `tests/lint/test_layer_boundaries.py`.
- **Status:** Accepted — implemented 2026-08-12. Read-only by construction; no pipeline, execution, or broker path is reachable from it.

---

## Context

An AI agent answering a question such as *"why is BEL ranked 12, is it a fresh
Stage 2, and how do its fundamentals look?"* previously had to search the
repository to rediscover where each fact lives. That is slow, consumes context,
and is exactly the kind of ad-hoc live-store access `AGENTS.md` warns against.

Worse, the layout contains several traps that a naive reader gets wrong:

- `_catalog.close` is **unadjusted**, while every technical indicator is
  computed on `COALESCE(adjusted_*, raw)` through `_catalog_feature_source`.
- **Three** stores hold stage state with different coverage, and the one the
  existing `symbol_report` loader reads is typically the stale one.
- Those stores use **two incompatible stage vocabularies** (`S2` versus
  `stage_2_advancing`), bridged only by a mapping that lives inside a heavy
  module.
- Fundamentals are split across a SQLite store, a DuckDB store, and CSVs, and
  the fiscal `report_date` is not the date a figure became knowable.
- Identity is `(symbol_id, exchange)`, but several existing read helpers filter
  on `symbol_id` alone.

## Decision

Add a new interface — not new analytics and not new storage — at
`src/ai_trading_system/interfaces/mcp/`, served over stdio by `ai-trading-mcp`,
exposing twelve tools over OHLCV, technical features, stage, sector, rank, and
fundamentals, plus a `describe_schema` column dictionary.

Four invariants are binding.

### I1 — No connection is ever opened read-write

An audit of the candidate reuse graph found that **every SQLite open in it is
read-write**, while DuckDB is already almost entirely read-only:

| Module | DuckDB | SQLite |
|---|---|---|
| `readmodels/decision_reads.py` | `read_only=True` | — |
| `research/symbol_report/loaders.py` | `read_only=True` | — |
| `domains/ranking/stage_store.py` readers | `read_only=True` | — |
| `readmodels/sector_detail.py` | `read_only=True` | **read-write** |
| `readmodels/stock_detail.py` | `read_only=True` | **read-write** |
| `domains/ranking/input_loader.py` | `read_only=True` | **read-write** |
| `analytics/feature_reader.py` | **read-write** | — |
| `domains/ingest/symbol_master.py` | — | **read-write** |
| `domains/fundamentals/screener_store.py` | — | **read-write** |

So the rule is: **the MCP owns 100% of SQLite and Parquet access, and reuses
only DuckDB readers that already open read-only, plus pure frame-in/frame-out
functions.** `masterdata.db` and `screener_financials.db` are read through
`McpContext.sqlite()` (`file:...?mode=ro`); Parquet is scanned through an
in-memory DuckDB connection that holds no store file. No existing module needed
loosening, and `ScreenerFinancialsStore` — whose constructor creates tables by
default — is out of the graph entirely.

Enforcement is an autouse test fixture that replaces `duckdb.connect` and
`sqlite3.connect` and fails any call that would open a writable handle.
Checksum comparison after the fact would only catch a write that happened; this
catches the *ability* to write, on paths a test never pushes hard enough to
mutate anything.

### I2 — `as_of` never returns data from after `as_of`

There is no "return latest with a note" path. Responses carry a typed status —
`LATEST`, `EXACT`, `NO_DATA_AS_OF`, `AS_OF_UNSUPPORTED` — and
`AS_OF_UNSUPPORTED` returns **no rows**, so the agent learns a surface cannot
answer historically instead of being handed the present.

`envelope.assert_not_future` runs on every response and raises on any row dated
after the cutoff, so a tool that forgets its `WHERE` clause fails loudly.
Invalid cutoff values are rejected, rather than being coerced to an unbounded
latest-data request.

The governed weekly stage surface resolves observation payloads through
`stage_observation_governance`, including correction authority, supersession,
recording time, and payload availability. Historical sector and screening
queries do not enrich missing classifications from the current symbol master,
because doing so would leak present identity state into a past answer.

Fundamentals cut off on **publication** date, not fiscal period.
`screener_financials.available_at` is bitemporal and part of the primary key;
`ScreenerFinancialsStore.get_company_data` is not used because it applies no
cutoff at all.

### I3 — The operator profile requires an explicit `DATA_ROOT`

`require_data_root_available()` is a no-op when `DATA_ROOT` is unset, so it does
not prevent the silent fall back to the repo-local `data/` tree. The `operator`
profile therefore requires `DATA_ROOT` to be set, to exist, and to resolve
outside the repository. `--profile fixture` is the only way to read a temporary
or repo-local root, and it is never the default.

### I4 — One stage vocabulary at the boundary, with an honest mapping

`LEGACY_STAGE_MAP` moves from `coverage.py` to `contracts.py`, beside the enum,
with `legacy_code_for`, `stage_family` and `is_transition`. `coverage.py`
imports it, keeping one source of truth.

The mapping is **asymmetric**: the four transition members have no legacy code,
so a lossless bidirectional round trip does not exist. Every stage row therefore
carries `stage_label` (canonical, always set), `stage_label_legacy` (nullable),
`stage_family` (a transition reports the stage it is leaving, so family filters
still match), and `is_transition`.

## Consequences

- An agent answers structural questions in tool calls with no repository
  reads, and `describe_schema` removes the need to grep for column meanings.
- The interface cannot corrupt a live store, trigger a pipeline, or reach a
  broker path, so it is safe to leave enabled.
- Historical answers are either correct or explicitly unavailable — never
  today's data wearing a past date.
- `mcp` becomes a runtime dependency. It must stay out of `domains/`,
  `pipeline/`, `platform/` and `research/`; the layer-boundary lint now
  enforces that, and tool/reader modules stay transport-agnostic so an HTTP
  adapter needs no tool changes.
- Sector RS and rotation quadrant from the rank artifacts are **not** exposed:
  they are latest-only and cannot be cut off by date. Sector structure is
  derived from the governed weekly stage store instead, which is
  effective-dated.
- `describe_schema` is a hand-maintained catalog. A test asserts every
  documented column appears in the corresponding tool's output, so it fails
  rather than rots when a schema moves.

## Alternatives considered

- **Mount MCP on the existing FastAPI app.** Rejected for v1: it requires a
  running server and inherits `EXECUTION_API_KEY` auth for what is a local,
  single-operator workflow. The tool layer is transport-agnostic, so this
  remains available later without rework.
- **Wrap `get_stock_detail` for the profile tool.** Rejected: it resolves
  aliases over read-write SQLite, fetches the latest quote with no `exchange`
  and no cutoff, and blends latest CSV artifacts with persisted history, so a
  historical or BSE profile could mix listings, price bases, and dates. The
  profile is composed from the point-in-time tools instead, with per-block
  dates and an `alignment` field.
- **Loosen each existing reader with a `read_only` flag.** Rejected in favour
  of I1's ownership rule, which touches no existing call site and is bounded to
  two SQLite files.
