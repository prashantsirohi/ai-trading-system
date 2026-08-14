# Persistent screener canary handoff

- **Purpose:** Acceptance outcome and prerequisites for universe expansion.
- **Audience:** Operator, engineering owner, and rollout approver.
- **Last verified:** 2026-08-12
- **Source of truth:** Canary code, test results, persistent run records, and immutable run packs.

## Outcome

The Phase 0 control plane and both 17-company runs are implemented, persisted,
reproducible, and fully accounted for. The acceptance gate **passes**, authorizing
a controlled NSE+BSE full-universe expansion while retaining the same fail-closed
DQ, provenance, and paper-execution defaults.

Passed:

- fixed versioned source registry and exact 17-row fixtures with checksum-locked mode routing;
- exact current identity plus evidence-bound effective-dated ISIN transitions for E2E and MCX;
- company/security/listing separation with no fuzzy-name joins;
- immutable single-writer control plane, atomic successful-run persistence, explicit dispositions/reasons, DQ and repair queues;
- point-in-time reads, scope separation, missing-not-zero rules, short-history handling, and company-type routing;
- dated official market-cap outcomes for 17/17, with 13 in band and four explicit cap exclusions;
- filing-grade NSE/BSE XML and inline-XBRL acquisition with exact document identity and whole-provider snapshot selection;
- checksum-locked, page-anchored issuer filing repair for PREMIERENE, WAAREEENER, E2E, and MCX, limited to missing or wholly empty periods after overlap reconciliation;
- all 13 in-band companies admitted to `BOUNDARY_REVIEW`, with zero blocking DQ issues and zero repair rows;
- official NSE/BSE corporate-action taxonomy and stored-adjustment reconciliation, including STLTECH's demerger and E2E/MCX splits;
- KPI definitions without invented observations;
- 548 acquisition records, 548 unique linked artifacts, and checksum verification of all 548 frozen source files;
- two immutable output packs, 17 decisions in each, and a persisted cross-run comparison;
- 78 relevant screener/ingest automated tests and documentation validation.

The authoritative handoff pair is
`screen-regression_replay-2026-08-08-6f27465442778028` (fixture v1.0.0) and
`screen-live_canary-2026-08-12-77f89150f38bd698` (fixture v1.2.0), both using
screen definition version 0.2.5. No completed pack was overwritten; earlier
attempts remain immutable forensic history.

## Recommendation

Proceed to a controlled full-universe Phase 1 run. Keep the existing source
ordering, issuer-repair checksum locks, minimum completeness gate, quarantine
behavior, and missing-not-zero policy unchanged. Treat boundary review as an
admission outcome, not an investment recommendation or production ranking.
Filing-backed bank/KPI observations remain later-phase enrichment work and must
continue to stay `NOT_DISCLOSED` until sourced.

This recommendation was executed on 2026-08-12. The authoritative discovery
result and next bounded filing batch are recorded in
[full-universe Phase 1 handoff](full_universe_handoff.md).
