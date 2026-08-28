# Persistent screener architecture and schema

- **Purpose:** Architecture, persistence, migration, rollback, and CLI contract for the Phase 0 canary and controlled Phase 1 universe discovery.
- **Audience:** Developers, data architects, and operators.
- **Last verified:** 2026-08-21
- **Source of truth:** `src/ai_trading_system/domains/research_screener/` and `configs/research_screener/`.

The canary is a separate research domain under `$DATA_ROOT/research_screener/`. It does not add a pipeline stage, execution dependency, public API, MCP surface, or schedule. The capex J-curve extension preserves the same boundary and adds no operational consumer.

After a passing canary gate, `full_universe` performs the first controlled
expansion without changing that isolation boundary. It constructs the union of
active official NSE and BSE equity listings, deduplicates valid company equities
by ISIN, and retains every non-company-equity or missing-ISIN listing with an
explicit terminal disposition. NSE `EQ`, `BE`, and `BZ` are main-board equity
series; BSE groups `M`, `MT`, `MS`, and `TS` are SME. A dual-listed security is
one universe member with both listing rows. The run reports resolved company
equity identities over resolved plus missing-identity candidates and requires at
least 98% coverage before later rollout stages.

The subsequent `filing_discovery` mode freezes a completed same-date
`full_universe` pack and admits only the parent's explicit cap-eligible cohort
for filing acquisition. The parent security, market, status, and manifest files
are content-hashed run inputs. Per-security checkpoint directories contain the
normalized member result, artifact metadata, and checksum-verified raw bytes;
batch progress is operational only and a final screen run is committed only
after the entire frozen cohort has been reconstructed. The mode does not
re-evaluate market cap, add names, or silently change the cohort.
The default four worker sessions each use a request interval multiplied by four,
holding the aggregate configured request cadence equal to the serial client.
Each worker owns its HTTP session; checkpoint writes are per-security and the
DuckDB transaction remains single-writer at finalization.
For controlled one-time backfills, five through sixty-four workers use a
worker-count-proportional interval, imposing a hard two-times aggregate ceiling.
Retryable HTTP failures still use the client backoff and remain
visible in artifact validation status.

The Phase 1 discovery pass uses NSE quote `totalMarketCap` for NSE-listed
securities and BSE `Mktcap` only for BSE-only securities. It does not estimate
market cap or switch a failed NSE listing to BSE. Non-main-board and non-company-
equity records stop before market-cap acquisition. Securities inside the cap
band remain `DATA_REPAIR_REQUIRED` with
`PHASE1_FILING_DISCOVERY_REQUIRED`; this mode does not treat the diagnostic
Screener-derived store as filing-grade evidence and does not yet acquire
universe-scale statements or corporate actions. Technical timing stays
separate and unavailable in this discovery pass.

Filing discovery uses NSE result metadata/XBRL first and invokes BSE only for an
exact dual listing when the complete NSE snapshot fails the contract. Provider
selection compares whole snapshots and never combines annual periods from one
exchange with quarterly periods from another. A pass requires point-in-time
publication, exact period-effective document identity, one selected scope,
latest disclosed annual and quarterly periods parsed, at least 70% mandatory
field completeness over six annual and twelve quarterly targets, and a valid
official corporate-action feed whose adjustment-requiring events reconcile to
the read-only operational action store. Failures remain explicit repair rows.
A pass ends at `BOUNDARY_REVIEW`, because this acquisition mode neither scores
nor ranks securities.

Successful NSE market-cap responses are checkpointed by AS_OF_DATE, screen
version, symbol, ISIN, and raw-response checksum under
`$DATA_ROOT/research_screener/checkpoints/full_universe/`. An interrupted run
can resume without repeating already validated same-cutoff acquisitions. Failed
responses are never checkpointed and are retried. Checkpoint metadata and bytes
must both exist and revalidate before reuse; the final immutable run pack still
freezes every reused raw response as a normal manifest artifact. `progress.json`
reports processed members and checkpoint hits without creating a durable
successful screening run.

Canary membership is explicitly versioned in `canary_fixture_versions.json`. Regression replay defaults to the checksum-locked v1.0.0 fixture; live canary defaults to the registered current fixture (v1.2.0). The service rejects checksum drift, unknown versions, legacy fixtures in live mode, and current fixtures in replay mode. An explicit `--canary-file` override must still satisfy the registered version/mode contract. The fixture-version registry is itself frozen as a run artifact, including correction evidence and source-master checksums. Effective-dated identifier transitions additionally require an exact match to frozen local official-action evidence before canonical persistence. The E2E bridge retains the old ISIN as a closed history row and opens the new ISIN at the registered split boundary; missing or mismatched action evidence fails closed as `IDENTIFIER_TRANSITION_UNPROVEN`.

Identity is layered as `company_id` (economic issuer) → `security_id` (equity/ISIN) → one or more `listing_id` values. A fixture identifier that conflicts with an official master creates an unresolved placeholder in universe membership and a DQ/repair record; it is not inserted as a canonical security or listing.

The adapter boundary is defined by `SecurityMasterProvider`, `MarketDataProvider`, `FundamentalDataProvider`, and `EvidenceProvider`. Screening services consume normalized adapter results rather than provider tables. The official client uses a session, request pacing, dated NSE MII files, content-type/status/schema/non-empty validation, and fixed failure policies from `source_registry.yaml`.

Market cap uses the current NSE quote metadata/symbol-data endpoints or the BSE
active-security master and normalizes rupees to INR crore explicitly. Financial
discovery uses NSE legacy and integrated result metadata first. For an exact
dual listing, an incomplete NSE snapshot triggers BSE result acquisition and a
deterministic whole-exchange-snapshot selection; annual or quarterly periods are
never spliced between NSE and BSE. BSE-only securities continue to use BSE directly.
The selected source is frozen and parsed as linked XBRL. It enforces publication
date, exact period-effective ISIN/document identity, one statement scope, latest-disclosed-period parsing,
six annual/twelve quarterly coverage, raw facts, formula version, and source-row
hashes. The existing Screener-derived store is diagnostic only and cannot make
a filing result pass. Parsed rows persist in `financial_statement_version` and
are emitted in the two fundamentals Parquet outputs.

For explicitly registered history gaps only, the selected exchange snapshot may
be completed from `issuer_filing_repairs.json`. The repair source must be an
official company investor-relations RHP, prospectus, or audited-results PDF, or
an official BSE filing attachment. Runtime acquisition requires the exact
registered SHA-256, PDF media type/signature, legal-name/CIN markers on fixed
pages, publication before the screen cutoff, and the same statement scope as the
selected exchange snapshot. Curated INR-crore rows carry page evidence and their
own row hashes. Existing exchange facts are never overwritten: a repair may add
only an absent target period or replace a period whose mandatory facts are all
missing. Configured overlap rows must reconcile at least four non-EPS metrics
within tolerance or the entire document repair fails closed. The four v1
contracts cover PREMIERENE and WAAREEENER pre-listing history, E2E older
standalone history, and MCX's corrected post-split FY2026 annual attachment.

Prior filing ISINs are accepted only inside effective windows proven by the
fixture's frozen identifier-transition evidence or by an active stored official
split action whose raw-payload checksum validates. A prior ISIN outside that
window remains a failed document; the adapter does not widen the window to make
coverage pass.

BSE's integrated-results metadata can expose the same filing revision through
both a legacy XML locator and an inline-XBRL HTML locator. The adapter groups
those locators by revision identity and prefers the working inline-XBRL
representation of that exact revision. Inline scale attributes and nested
contexts are normalized explicitly. Documents with a wrong ISIN, malformed
markup, or failed retrieval remain failed artifacts. Completeness is measured
against consecutive target fiscal periods, so older rows cannot silently fill
an internal history gap.

Corporate actions are acquired from fixed NSE/BSE endpoints and normalized to
split, bonus, rights, consolidation, dividend, demerger, merger, symbol/name
change, trading-series, or other. Adjustment-requiring events are reconciled to
the stored action history for the requested continuity window. Any unmatched
event sets `ADJUSTMENT_INCOMPLETE` and suppresses adjusted technical conclusions
for that security only. Filing-discovery 1.2.0 freezes read-only current-master
sector/industry rows by exact ISIN and routes industrial, bank, financial-
institution, and market-infrastructure contracts. Missing, ambiguous,
post-cutoff, or incomplete classification fails closed; neither company-name
similarity nor symbol-only fallback is accepted. Phase 0 persists versioned KPI
definitions with observation state `NOT_DISCLOSED` and never fabricates
observations.

The isolated DuckDB schema starts with `001_initial.sql`; additive migrations
`002_identifier_history_backfill.sql` and
`003_repair_fixture_identity_provenance.sql`,
`004_identifier_history_deduplicate.sql`, and
`005_remove_fixture_identifier_history.sql` populate effective-dated current
identifiers, deduplicate exact observations, and prevent replay-only identifiers
from becoming canonical. `006_ingestion_artifact.sql` adds the many-to-many
acquisition/artifact bridge and backfills every recoverable first-acquisition
link. `007_qualitative_research_runs.sql` adds immutable parent-linked research
run metadata; the existing extension tables store versioned annual-report
documents and page-attributed evidence. Migration
`008_qualitative_claim_contract.sql` adds proposed claims, separate extraction
and verification reviews, model usage, and deterministic policy decisions. The
[qualitative claim contract](qualitative_claim_contract.md) pins the low-cost
model/batch envelope and keeps high-impact or disputed claims under human
review. No annual-report agent runner is active in that milestone. The separate
J-curve runner uses its own announcement-compatible schemas and does not change
the annual-report claim contract. The schema includes Phase
0 provenance, identity, versioned
screen/run/universe/factor/archetype/decision entities, repair/DQ queues, run
comparisons, and extension-ready qualitative research tables. A file lock
enforces one writer. All material results for a successful run commit in one
transaction; a failure rolls back members/scores/decisions and records only a
terminal failed run. A retry receives a new immutable run ID and records the
failed predecessor.

Migration `009_jcurve_research.sql` adds immutable J-curve import/evaluation
runs, frozen announcement projections, model-call audit rows, announcement
claims and reviews, company-owned capex episodes, evidence links, and
append-only stage observations. `market_intel.duckdb` remains separately owned
and is opened read-only; accepted payloads and checksum-valid attachments are
copied into `$DATA_ROOT/research_screener/jcurve_runs/<run_id>/`. The adapter
requires both publication and ingestion timestamps not to exceed the historical
cutoff and resolves identity through effective-dated screener masters.

Migration `010_jcurve_screener_seed.sql` adds immutable screen/accounting seed
runs without changing the announcement contract. V1 freezes authenticated
Screener screen 317873 and evaluates only its exported members, the versioned
25-company baseline, and policy supplemental FCL. It does not scan or download
the entire listed universe. The accounting adapter opens
`screener_financials.db` read-only, chooses exactly one basis, and requires
three annual periods plus eight aligned sales/operating-profit/depreciation
quarters before consolidated can outrank complete standalone history. Missing
CWIP is retained as `NOT_DISCLOSED`; a separately versioned net-block and
depreciation fallback can still identify commissioning without inserting zero.
Seed dispositions are discovery routing and cannot establish an official
capex claim, commissioning date, or J-curve stage by themselves.

The model router caps a request at eight selected pages and 18,000 extracted
characters. Clean text routes to `deepseek/deepseek-chat`; selected complex or
weak-text pages route to `google/gemini-2.5-flash-lite`, with
`qwen/qwen2.5-vl-72b-instruct` available as the governed vision fallback.
Verification is assembled separately and uses a different model family. All
responses use strict JSON schemas. Model IDs are policy inputs, while actual
provider and cost are response metadata and never hard-coded as durable prices.
Evaluation defaults to 25 resolved companies, permits an explicit maximum of
250, and stops before a new request when its request or reported-cost budget is
exhausted. Bootstrap does not infer that the upstream `market_intel` store is
historically complete. It records `HISTORICAL_SOURCE_COVERAGE_UNPROVEN` unless
authoritative completed receipts prove the requested interval for the frozen
cohort's required primary listing sources.

New shadow collections may opt into
`market-intel-high-value-filter-v1`. The upstream store then owns immutable
source-window receipts plus attachment-routing decisions. The screener adapter
joins those decisions by `raw_event_id` only when the policy is explicitly
named, accepts `KEEP` and `FETCH_ATTACHMENT` only when their matched signals
are capex, capacity, facility, commissioning, project-finance, demand-path,
order-award, or project-adverse evidence, and remains read-only. Generic
transactions and financing remain available in `market_intel` but do not enter
the J-curve corpus. The adapter also freezes overlapping receipt metadata into
the import manifest. Coverage is
considered proven only when the union of contiguous complete receipts covers
the entire requested interval for the frozen cohort's required primary listing
sources. NSE-listed members require NSE API coverage; BSE is additionally
required only for a member without an NSE listing. An unrestricted import with
no frozen cohort continues to require both NSE and BSE. Otherwise the import
records `UPSTREAM_FILTER_COVERAGE_INCOMPLETE`. A proven chunked historical
backfill clears both coverage degradations.

Current shadow collection can additionally use
`market-intel-security-master-v1`, whose independent NSE/BSE sync receipts and
normalized listing observations classify active corporate-equity securities as
`NSE_ONLY`, `BSE_ONLY`, or `DUAL` by exact valid ISIN. That upstream master is
collection-routing and provenance evidence only. Enriched raw announcements
carry ISIN, canonical symbol, listing membership, and original exchange
security ID. The read-only J-curve adapter first resolves those announcements
against effective-dated `security_master` and `listing_master` rows in the
screener store. Because the present master declares latest-only temporal trust,
an older filing may fall back to one current listing on the same source
exchange, preferring exact ISIN before an exchange identifier. The adapter
freezes the resolved IDs into the immutable import pack.
Before model or attachment promotion, `market_intel` can export a deterministic
review set from exact completed source receipts. The review set stratifies by
source, filter decision, and listing membership, unions all exact-ISIN baseline
cohort announcements, and remains unlabeled until human review. Its labels
measure the upstream metadata gate only; they do not assert a J-curve claim or
stage.

The default bootstrap cohort is the immutable
`jcurve-capex-baseline-v1` configuration. Its 25 members include WELCORP, HSCL,
and DEEDEV and cover multiple capex disclosure and document-layout patterns.
Before import, every configured member must resolve to one company and security
through exact ISIN, NSE symbol, and BSE code at the run cutoff. The frozen run
pack records the cohort policy hash, resolved IDs, observed-company count, and
members with no eligible announcement evidence. Membership does not encode an
expected claim, stage, success, or failure label. `--all-companies` disables
this default only when broad historical import is intentional. A cohort whose
identity-check date is later than the requested cutoff is rejected as look-ahead.

Run IDs are content-addressed from the screen version, normalized member
inputs/decisions, and raw artifact hashes. Rerunning identical frozen inputs
returns the completed run without overwriting it. Raw exchange responses are
frozen beside the normalized output pack, and the manifest checksum is verified
against those bytes. Source-artifact IDs include the request locator as well as
the content digest, so two different official queries that return identical
bytes remain distinct lineage records. Acquisition records are scoped by mode,
as-of date, and screen version; unchanged artifacts are reused and linked to
each acquisition through `ingestion_artifact`. Each completed run also records
a dataset snapshot over its full artifact set.

## Migration and rollback

The schema is new and does not migrate an operational store. Normal initialization creates only:

```text
$DATA_ROOT/research_screener/control_plane.duckdb
$DATA_ROOT/research_screener/control_plane.duckdb.lock
$DATA_ROOT/research_screener/runs/<run_id>/
$DATA_ROOT/research_screener/jcurve_runs/<run_id>/
```

For a clean rollback before adoption, take a checksum-preserving backup and move the entire `$DATA_ROOT/research_screener/` directory out of `DATA_ROOT`. Do not delete or rewrite individual completed runs. No rollback is required in `control_plane.duckdb`, `ohlcv.duckdb`, `execution.duckdb`, master data, fundamentals, or schedules because this milestone never writes them.

## Commands

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.research_screener.cli \
  --as-of-date 2026-08-08 --run-mode regression_replay

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.research_screener.cli \
  --as-of-date YYYY-MM-DD --run-mode live_canary

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.research_screener.cli \
  --as-of-date YYYY-MM-DD --run-mode full_universe

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.research_screener.cli \
  --as-of-date YYYY-MM-DD --run-mode filing_discovery \
  --parent-run-id <completed-full-universe-run-id> --batch-size 25

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.annual_report_service \
  --as-of-date YYYY-MM-DD --parent-run-id <completed-filing-run-id> \
  --batch-size 25 --workers 4

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.jcurve.cli seed-screener \
  --as-of-date YYYY-MM-DD --screen-id 317873

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.jcurve.cli discover-v2 \
  --as-of-date YYYY-MM-DD

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.jcurve.cli bootstrap \
  --as-of-date YYYY-MM-DD --lookback-years 5 \
  --seed-run-id <completed-jcurve-seed-run-id>

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.jcurve.cli ingest \
  --as-of-date YYYY-MM-DD --overlap-days 7 \
  --upstream-filter-policy market-intel-high-value-filter-v1

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.jcurve.cli evaluate \
  --parent-run-id <completed-jcurve-import-run-id> \
  --as-of-date YYYY-MM-DD --materiality-inputs /path/to/materiality.json

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.jcurve.cli calibrate \
  --evaluation-run-id <completed-evaluation-run-id> \
  --labels /path/to/reviewed-25-company-labels.json
```

Annual-report discovery is intentionally separate from screening. It freezes
the filing parent's exact `BOUNDARY_REVIEW` cohort, follows NSE then official
BSE fallback, validates point-in-time metadata and archive byte counts, and
retains both valid and failed raw bytes. Topic extraction produces only page-
attributed LOW-confidence discovery anchors or `NOT_DISCLOSED`; it cannot
rewrite statements, scores, dispositions, or execution state.

Use a new semantic `--screen-version` whenever rule or parser semantics change. Historical replay output is explicitly labelled as a fixture and must never be represented as current market data.
