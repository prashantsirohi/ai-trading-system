# Daily operator review stage

- **Purpose:** Versioned contract for the consolidated shadow attention list and Sheets pilot.
- **Audience:** Operator, opportunity and publishing maintainers.
- **Last verified:** 2026-10-20
- **Source of truth:** `src/ai_trading_system/domains/opportunities/daily_review.py`, `pipeline/stages/daily_review.py`, and `domains/publish/channels/daily_review.py`.

Start with the [System Guide](../SYSTEM_GUIDE.md).

## Purpose

`daily-operator-review-v1` combines independent discovery and monitoring facts
into one listing/session row. It does not change Final Review's frozen P-or-F
selection, rank, admission, lifecycle, candidate tracking or execution authority.
It adds no database schema or parallel watchlist store.

## Entrypoints

`--daily-review-mode shadow` inserts `daily_review` immediately before `publish`
when using the default CLI stage list. The default is off. The daily shadow
wrapper enables it. CLI use requires `--final-review-mode shadow`, which retains
its registry-shadow prerequisite. Explicit stage lists remain authoritative.

## Input data

Only same-run, completed/promoted artifacts qualify. The reader verifies source
session, availability cutoff, SHA-256 and CSV row counts, and resolves files
inside the configured domain root. Failed/unpromoted sources are absent, never
rescued by another run's current output. Final-review list/universe/summary must
belong to one attempt with matching decision hashes.

| Input | Role |
|---|---|
| `ranked_universe`, `ranked_signals` | Explicit listing identity/rank context; operational shortlist inclusion |
| `investigator_scores`, `active_watchlist` | Weekly-gainer score >=65 investigation and continuing nonterminal research |
| `fundamental_thesis_universe` | Accounting-qualified discoveries; daily admission blockers remain visible |
| `pattern_lane_assessments` | Fresh known positive pattern membership |
| `opportunity_convergence_view`, `technical_evidence_labels` | Existing I/F/P cohort and independent technical context |
| `final_review_universe`, `final_review_list`, `final_review_summary` | Existing selection, original priority, readiness, exclusions and calendar gate |
| `candidate_tracker_current` | Active tracker episodes, including recorded failures requiring attention |
| `execute.positions`, `execute_summary` | Nonzero post-execution paper-ledger positions, with preview disclosed |
| `position_monitor_reconciliation` | Pre-execution monitoring detail; does not replace post-execution holdings |

Identity is `(exchange, symbol, decision session)`. Investigator's missing
exchange resolves to its declared NSE intake domain. Symbol-only tracker rows
require exactly one exchange in explicit current-source identities; ambiguous
or unresolved rows are counted in `identity_exceptions`. Other missing/invalid
identities and future row dates are rejected visibly. Multiple tracker episodes
and position rows remain linked detail under one listing. Conflicting duplicate
single-row sources cannot grant entry-review readiness.

Technical labels and historical performance never create standalone inclusion.
Operator pins are not supported by v1; operator annotations do not manufacture
membership. Actual journal accounts/balances are not joined to the paper ledger.

## Output artifacts

Under `$DATA_ROOT/pipeline_runs/<run_id>/daily_review/attempt_<n>/`:

- `daily_review_list.csv`: complete ordered list with independent source states,
  blockers, episode IDs, source dates/hashes/links and full evidence JSON.
- `daily_review_changes.csv`: new/changed/removed or uncertain absence receipts.
- `daily_review_summary.json`: policy/hash, calendar, population/source counts,
  unresolved identities, coverage, prior session and serialized pack hash.

Historical performance artifact references retain their population/anchor/policy
semantics in `performance_diagnostics`; there is no pooled return or super-score.

## Main modules

`domains/opportunities/daily_review.py` owns the pure policy and deterministic
projection. `pipeline/stages/daily_review.py` owns verified reads, policy
registration and artifact materialization. `RegistryStore` owns lineage queries.
`publish` gates delivery; `channels/daily_review.py` formats materialized rows.

## Process flow

1. Register/verify the immutable policy content through the existing policy registry.
2. Read and verify current sources; union source-owned memberships.
3. Assign one primary queue: Position attention, Entry review, Investigate, Watch.
   Every open paper position stays visible regardless of stage/qualification.
   Recorded trap, tracker failure or SMA20-break evidence overrides entry attention.
4. Compare against the latest verified earlier-session pack under the same policy.
   Same-session retries are never a baseline. Ordinary rank/price fluctuations
   are excluded from material-change identity; readiness, membership, blockers,
   tracker state, position quantities, technical break and episode changes are included.
5. Sort by queue, material change, recorded readiness, broad rank score and listing
   identity. Missing rank stays missing. Preserve the original Final Review priority.
6. Register the three artifacts; publish the unchanged materialized order.

## DQ

Calendar `PASS` and usable broad rank are necessary for a publishable pack.
Missing sources, unresolved identity or duplicate conflicts degrade coverage.
Removals require complete membership-source coverage in both packs. A missing
lane produces `UNCONFIRMED_ABSENCE`, never an asserted removal. A missing position
feed retains prior holdings in Position attention as `UNVERIFIED_PRIOR`, with
unknown current-position status and original evidence dates; it cannot assert
current holdings or closure. The first pack
uses `BASELINE`; a changed policy restarts comparison. BSE calendar/readiness
limitations remain visible through existing review states and summary.

## Failure modes

Stage exceptions use the optional shadow failure boundary and do not suppress
existing publishers. A failed/calendar-invalid summary cannot refresh the pilot.
The Sheets channel is optional: delivery/auth/quota failure is recorded and
the source artifacts remain authoritative. Partial remote writes are possible;
retry from the same pack. Last-successful session/run labels remain explicit.

## Retry behavior

Identical sources and earlier-session baseline reproduce identical artifacts.
Changed sources create a new attempt. Failed producer files never enter the list.
No previous list is copied into fresh success. Channel-specific artifact hashes
drive delivery deduplication. There is no historical evidence backfill.

## Downstream consumers

`google_sheets_daily_review_pilot` creates/refreshes `00_Daily_Review_Pilot` at
the front of the existing workbook. It includes filters, frozen identity columns,
research links and editable operator fields. First creation seeds notes from
the existing Final Review tab when its header contract matches. Thereafter notes
belong to the daily pilot and follow exchange/symbol identity.

All annotations, including inactive names, are preserved in hidden
`_DATA_DAILY_REVIEW_OPERATOR_STATE`. Full rows, changes and summary are mirrored
in `_DATA_DAILY_REVIEW`, `_DATA_DAILY_REVIEW_CHANGES`, and
`_DATA_DAILY_REVIEW_SUMMARY`, without display caps. Oversized evidence fields
are split losslessly into ordered 20,000-character parts in
`_DATA_DAILY_REVIEW_DETAILS`; the main hidden row names that field and part count.
Hidden-data requests are bounded by payload size, not population size, and use
raw values to preserve JSON fragments and literal annotations. Unknown note layouts fail
before overwrite. Specialist tabs remain available. No Telegram consumer is added.
Five real operator reviews and M2 economic acceptance remain outstanding.

## Commands

```bash
./scripts/run_daily_shadow.sh
# Disable only the new pilot, preserving other shadow outputs:
./scripts/run_daily_shadow.sh --daily-review-mode off
# Existing run, regenerate only the consolidation and publish locally:
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --run-date YYYY-MM-DD --stages daily_review,publish \
  --opportunity-registry-mode shadow --final-review-mode shadow \
  --daily-review-mode shadow --local-publish
```

`--local-publish` suppresses external delivery, not registry/artifact writes.
Use a fully isolated copied runtime root for validation. No feature rebuild is
required because this stage reuses existing scores and evidence.

## Captured validation — 2026-10-20

Two copied real runs (October 9 and October 16) produced 991 and 988 unique
listings respectively. Both had verified same-run source counts, no unresolved
identities, calendar `PASS`, byte-identical exact retries and accepted publication
gates. The latest list contained 3 Position attention, 2 Entry review, 412
Investigate and 571 Watch rows. Upstream Final Review remained degraded; no
readiness or economic gate was promoted. Live control-plane and input-artifact
hashes were unchanged during capture/replay. This was a stage-level canary with
`local_publish=True`, not a full operational pipeline run.

The local preview and evidence are under
`/private/tmp/daily-review-canary-nb2hefzv/` (temporary local evidence).
After the operator explicitly approved the configured workbook and payload,
`00_Daily_Review_Pilot` was published to `Stock_Analysis` on October 20 using
October 16 evidence. Read-back verified all 988 listing identities in generated
order; the operator-state archive contains 999 active/inactive listing records.
The [published pilot](https://docs.google.com/spreadsheets/d/1_hyFH_RpMXlKlCQZuDkkt-Eh0CxFtqV_zNtk-_W3KUQ/edit#gid=951316283)
is a shadow workflow trial. Five human reviews and economic acceptance remain
outstanding; publication does not close G1/G2/G3 or change execution authority.

Post-publication read-back also verified all 988 visible annotation records
against their retained state and reconstructed all 351 split evidence fields
from 894 ordered parts, byte-for-text equal to the source CSV values.
