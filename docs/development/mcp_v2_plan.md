# MCP v2 Implementation Plan

- **Purpose:** Define the planned expansion of the read-only MCP from research evidence into pattern intelligence, full-universe screening, operational provenance, and lifecycle context.
- **Audience:** Operator and developers implementing MCP v2.
- **Last verified:** 2026-08-18
- **Source of truth:** This is a planned-behavior document. Current behavior remains defined by [`reference/mcp_tools.md`](../reference/mcp_tools.md), [`decisions/ADR-0008-read-only-mcp-interface.md`](../decisions/ADR-0008-read-only-mcp-interface.md), and current code under `src/ai_trading_system/interfaces/mcp/`.
- **Status:** Implemented through lifecycle context on 2026-08-18. Optional portfolio/execution reads remain deferred pending a separate ADR and operator approval.

---

## Binding constraints

MCP v2 preserves every v1 invariant:

- strictly read-only MCP connections and no pipeline, broker, or execution mutation;
- `(symbol_id, exchange)` identity;
- point-in-time cutoffs based on when evidence was knowable;
- correction-aware governed stage resolution;
- explicit `LATEST`, `EXACT`, `NO_DATA_AS_OF`, or `AS_OF_UNSUPPORTED` status;
- no current-state fallback in historical answers;
- bounded row counts and parameterized queries.

Current shortlist behavior must remain unchanged unless the operator separately
approves a ranking-policy change. Adding analytical breadth must not widen
candidate, publishing, or execution inputs.

## Delivery order

1. Pattern intelligence.
2. Fundamental-discovery shadow-lane API.
3. Full-universe persistence and advanced screening.
4. Sector leadership.
5. Data quality, freshness, run, and artifact lineage.
6. Candidate, Investigator, and opportunity lifecycle context.
7. Optional portfolio and execution reads behind a separately reviewed boundary.
8. Schema discovery, transport coverage, and OpenCode acceptance tests.

## Phase 1 — Pattern intelligence

Add:

- `get_pattern_detail(symbol, exchange, as_of)`;
- `get_pattern_history(symbol, exchange, from_date, to_date, as_of, limit)`;
- a `pattern` block in `get_symbol_profile`;
- `pattern_family`, `pattern_state`, `min_pattern_score`, and
  `max_pivot_distance` filters in `screen_universe`;
- `describe_schema("pattern")`.

Expose pattern family, lifecycle state, score, signal date, pivot/breakout
levels, distance from pivot, setup quality, model version, config hash, and
effective date. Operational `pattern_history` and ADR-0007 shadow
`pattern_lane_*` evidence are separate surfaces. Shadow evidence must be
labelled non-actionable and must never be blended into an operational pattern
answer.

## Phase 2 — Fundamental-discovery shadow lane

Expose only the operational shadow `fundamental_discovery` lane through four
MCP tools: `get_fundamental_thesis`, `get_fundamental_thesis_history`,
`screen_fundamental_theses`, and `get_fundamental_lane_overview`. The API joins
`fundamental_thesis_projection` to its exact immutable
`fundamental_thesis_classification` by `(symbol_id, exchange,
source_data_hash, taxonomy_version, rule_version)` and returns classification,
projection, all-seven-family evaluations, and prior-classification change
evidence as separate blocks.

Point-in-time reads constrain projection date, source availability, projection
creation, and classification creation to the cutoff. Cross-sections pin one
projection date. Standalone and consolidated evidence never blend. Generic
`get_fundamentals` scores do not imply thesis state, and missing, stale,
future-dated, unsupported, or incomplete evidence remains ineligible.

The seven-family vocabulary is `QUALITY_COMPOUNDER`, `HIGH_GROWTH_EMERGING`,
`EARNINGS_ACCELERATION`, `UNDERVALUED_QUALITY`,
`CASHFLOW_BALANCE_SHEET_INFLECTION`, `TURNAROUND_CYCLICAL_RECOVERY`, and
`CAPITAL_RETURN_INCOME`. Research-screener filings, annual reports,
qualitative claims, and HTTP routes remain out of scope. Candidate-level
`candidate_fundamental_observation` rows appear only in the lifecycle surface.

## Phase 3 — Full-universe ranking and advanced screening

### Current limitation

`screen_universe` currently reads `control_plane.duckdb:rank_history`.
`rank_history` contains the regime-qualified `ranked_signals` shortlist, not the
full scored universe. On 2026-08-14 the neutral `profile_C_cash_only` policy set
`effective_top_n=20`, so the MCP correctly returned 20 rows with
`truncated=false` even when its requested limit was 500.

The tool name therefore overstates current breadth: it screens the persisted
shortlist rather than every scored security.

### Planned persistence contract

Add `control_plane.duckdb:rank_universe_history` as the durable analytical
cross-section before regime `top_n` truncation. Its planned grain and identity
are:

```text
(symbol_id, exchange, trade_date, universe_id, rank_model_version)
```

The table carries full-universe rank position, composite and adjusted scores,
factor scores, eligibility, rejection reasons, rank confidence, config hash,
pipeline run, and the regime-policy metadata used to derive the shortlist.

The rank stage should compute the full ordered cross-section once and derive
the existing `ranked_signals` shortlist from it. Persisting the full view must
not change the contents, order, row cap, or downstream ownership of
`ranked_signals`.

Do not increase `rank_top_n` merely to expose more MCP rows. That value is a
strategy control used by candidate and execution workflows.

### Planned MCP contract

Extend `screen_universe` with:

```text
scope = "shortlist" | "full_universe"
```

`shortlist` remains the default and reads `rank_history`. `full_universe` reads
`rank_universe_history`. Responses add:

- `meta.scope`;
- `meta.full_universe_size`;
- `meta.shortlist_size`;
- `meta.selection_policy`;
- `meta.effective_min_score` and `meta.effective_top_n`;
- `meta.market_regime`, `meta.regime_as_of`, and regime freshness fields.

Add filters for `stage2_only`, `max_bars_in_stage`, pattern state/family,
technical conditions, liquidity, delivery, fundamental tier, and fundamental
red flags. Keep the hard response cap at 500; `matched_count` must describe the
pre-cap match set.

### Regime freshness requirement

The latest inspected 2026-08-14 rank attempt carried a neutral regime snapshot
dated 2026-05-13 while reporting `regime_age_days=18`. V2 must not silently
repeat that inconsistency.

Before shortlist persistence:

- calculate regime age from `run_date - regime_as_of` using one canonical
  implementation;
- compare the calculated value with any stored age and emit a DQ failure on a
  mismatch;
- classify regime evidence as `ALIGNED`, `STALE`, or `INCOMPLETE` under an
  explicit versioned threshold;
- expose the date, calculated age, status, and policy version through MCP;
- fail closed or mark the rank attempt degraded according to the approved DQ
  policy rather than presenting a stale regime as current.

Changing the stale-regime blocking policy requires the normal rank/DQ contract
review; MCP itself remains a reporter, not the policy owner.

### Phase 2 acceptance criteria

- A neutral run may persist 20 shortlist rows while persisting the complete
  analytical universe separately.
- `scope="shortlist"` remains byte-for-byte compatible with v1 ordering and
  selection.
- `scope="full_universe"` returns more than the shortlist when more eligible
  ranked securities exist.
- Historical requests resolve one model/config version and never read a future
  cross-section or regime observation.
- NSE and BSE rows remain distinct.
- Same-date reruns are idempotent and cannot mix model versions.
- Candidate, publish, and execute consumers continue to read only the existing
  shortlist contract.
- A stale or internally inconsistent regime snapshot is visible in metadata
  and covered by a DQ test.

## Phase 4 — Sector leadership

Add a latest-only `get_sector_leadership` surface for relative strength,
momentum, rotation quadrant, earnings leadership, and valuation-cycle evidence.
Return `AS_OF_UNSUPPORTED` for historical dates where the underlying artifact
has no publication history. Keep the governed stage-based sector overview as
the point-in-time structural surface.

## Phase 5 — Data trust and lineage

Add read-only tools:

- `get_pipeline_run`;
- `get_data_quality_status`;
- `get_artifact_lineage`;
- `get_data_freshness`.

These tools report completed/promoted attempt status, artifact content hashes,
DQ outcomes, source dates, model/config versions, and explicit stale or missing
evidence. They do not trigger retries, promotions, migrations, or repairs.

## Phase 6 — Candidate and opportunity lifecycle

Add:

- `get_candidate_status`;
- `get_candidate_history`;
- `get_investigator_evidence`;
- `get_opportunity_episode`.

The tools explain discovery, promotion, rejection, lifecycle state, evidence
onsets, and correction impacts without modifying candidate or opportunity
stores.

## Phase 7 — Optional portfolio boundary

Evaluate a separate read capability for `get_positions`,
`get_portfolio_exposure`, and `get_order_history`. It may open
`execution.duckdb` read-only but must not import broker adapters or expose order
placement, cancellation, reconciliation mutation, or live authorization.
Landing this phase requires a dedicated ADR update and explicit operator
approval.

## Phase 8 — Discovery and verification

- Extend `describe_schema` to symbol, profile, screening, pattern, lineage,
  lifecycle, and any approved portfolio surfaces.
- Exercise every tool through the MCP SDK, not only through direct Python
  calls.
- Add a real stdio client handshake and tool-call test.
- Repair pytest-cov's NumPy reload conflict and enforce an agreed coverage
  threshold.
- Add OpenCode discovery and representative tool-call smoke tests.
- Run the read-only live-store self-test for latest and historical cutoffs.

## Documentation updated with implementation

Update `SYSTEM_GUIDE.md`, the relevant stage/storage/DQ contracts,
`reference/database_schema.md`, `reference/artifacts.md`,
`reference/mcp_tools.md`, and ADR-0008 in the same implementation commit when
their owned behavior changes. This plan must not be cited as evidence that a v2
surface already exists.
