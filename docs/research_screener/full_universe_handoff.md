# Persistent screener full-universe Phase 1 handoff

- **Purpose:** Record the first controlled NSE+BSE universe discovery result and next bounded work.
- **Audience:** Operator, data engineering owner, and filing-discovery reviewer.
- **Last verified:** 2026-08-12
- **Source of truth:** Run `screen-full_universe-2026-08-12-c80cca794995a401`, its immutable pack, and the research-screener control plane.

## Outcome

The controlled identity, instrument/board, and dated market-cap discovery run
completed under screen definition `persistent_screener_phase1` version `1.0.0`.
It accounted for 5,098 deduplicated or explicitly retained official exchange
records representing 4,699 active BSE company-equity listings and 2,399 active
NSE company-equity listings before ISIN deduplication.

- 4,829 company-equity identities resolved by exact ISIN and two active BSE
  listings remain missing valid ISINs, giving 99.9586% identity coverage and
  passing the later-rollout 98% floor.
- 267 official active records are non-company-equity instruments and remain
  explicitly `INELIGIBLE_BOARD_OR_INSTRUMENT`; they were not treated as identity
  failures.
- 1,373 main-board company equities are inside the ₹1,000–₹100,000 crore band
  using trusted dated official full market cap. They are intentionally
  `DATA_REPAIR_REQUIRED` with `PHASE1_FILING_DISCOVERY_REQUIRED`; this discovery
  pass does not claim fundamental admission.
- 2,858 are outside the market-cap band, 77 have explicit
  `ELIGIBILITY_UNKNOWN`, and 788 are SME/non-company-equity instrument
  exclusions. The two unresolved identities are included in the 1,375 repair
  rows.
- The pack contains 2,404 artifacts and acquisition links: 2,373 valid and 31
  retained failed responses. Every frozen file exists and all 2,404 manifest
  SHA-256 values verify.
- The control plane contains 5,098 universe members and decisions, 1,452
  blocking DQ rows, 1,375 repair rows, and one complete dataset snapshot.

## Next controlled batch

The next stage is filing-grade discovery for the 1,373 cap-eligible securities,
not ranking or recommendation. Process that cohort in resumable bounded batches
using NSE XBRL first and exact-listing BSE whole-snapshot fallback. Preserve
statement scope, point-in-time publication cutoff, period-effective identity,
corporate-action continuity, missing-not-zero semantics, and explicit repair
outcomes. Do not treat the existing Screener-derived store as filing-grade
admission evidence and do not alter production schedules or execution.

The implemented operator path is `--run-mode filing_discovery` with this run's
ID supplied as `--parent-run-id`. It checkpoints every security and raw response
under the research-screener data root, resumes without changing cohort
membership, and creates an immutable final pack only after all 1,373 members are
accounted for.

## Filing-discovery completion

The next controlled batch is complete as run
`screen-filing_discovery-2026-08-12-74ee4481148e4e67`. All 1,373 frozen
cap-eligible ISINs are accounted for: 816 reached `BOUNDARY_REVIEW` after the
filing/action contract passed, and 557 remain `DATA_REPAIR_REQUIRED`. The final
manifest's 42,847 raw files all passed byte-count and SHA-256 verification. See
the [filing-discovery handoff](filing_discovery_handoff.md) for DQ evidence and
the next repair and annual-report research gates. No ranking or recommendation
was produced.
