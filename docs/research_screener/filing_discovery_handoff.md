# Persistent screener filing-discovery handoff

- **Purpose:** Record the completed filing-grade discovery result for the frozen Phase 1 cap-eligible cohort and define the next controlled gates.
- **Audience:** Operator, data engineering owner, filing-repair reviewer, and research-system developer.
- **Last verified:** 2026-08-12
- **Source of truth:** Superseding run `screen-filing_discovery-2026-08-12-dc4cb64090b47027`, its immutable output pack, and the research-screener control plane.

## Superseding 1.2.0 outcome

The completed superseding run contains the same 1,373 parent-eligible ISINs
under filing definition 1.2.0 and input snapshot hash
`dc4cb64090b4702727b2e9a72ed0f372f302ed710a15af53ccc76309050f6881`.
It has 763 `BOUNDARY_REVIEW` and 610 `DATA_REPAIR_REQUIRED` members. Its 733
blocking rows comprise 571 fundamental, 94 issuer-classification, and 68
corporate-action issues. The exact-ISIN classification gate routes 1,141
industrial issuers, 103 financial institutions, 30 banks, and 5 market-
infrastructure issuers; 94 remain fail-closed `UNCLASSIFIED`.

The pack contains 6,660 annual and 14,676 quarterly real statement rows and
42,785 artifacts: 39,991 valid and 2,794 retained failures. All source files
were independently verified over 4,452,970,500 bytes with zero hash or byte-
count errors. The normalized top-level digest is
`8d358e917949aa5c2ef52ebc60d20efba0c42695a50471bf214c67317596f890`.
The 763 filing-grade members are now the exact parent cohort of the completed
[annual-report discovery](annual_report_discovery_handoff.md).

## Superseded 1.1.0 baseline

The following section preserves the original 1.1.0 result for audit history; it
is not the current filing snapshot.

Filing discovery completed for all 1,373 securities frozen from parent run
`screen-full_universe-2026-08-12-c80cca794995a401`. The final run is
`COMPLETED` under definition `persistent_screener_filing_discovery` version
`1.1.0`; its input snapshot hash is
`74ee4481148e4e67236bdac01826d6921679c5d119279f6c0e02efe0485544ea`.

- The final cohort exactly equals the parent's 1,373 `ELIGIBLE` ISINs. Security
  and market outputs each contain 1,373 unique securities, and every market row
  retains `ELIGIBLE` from the frozen parent rather than re-evaluating membership.
- 816 securities pass filing provenance, selected-scope completeness, latest-
  period parsing, point-in-time publication, and corporate-action continuity.
  They end at `BOUNDARY_REVIEW`, not `QUALIFIED`; this run performs no factor
  score, rank, recommendation, schedule, publication, or execution action.
- 557 securities remain `DATA_REPAIR_REQUIRED`. There are 585 blocking DQ and
  repair rows because 28 securities have both a fundamental and a corporate-
  action issue. The issue rows comprise 517
  `FUNDAMENTAL_PROVENANCE_OR_COMPLETENESS_FAILED` and 68
  `CORPORATE_ACTION_CONTINUITY_FAILED` records.
- Selected statement scope is standalone for 1,079 securities, consolidated for
  283, and unresolved for 11. Corporate-action continuity validates for 1,305
  securities and remains `ADJUSTMENT_INCOMPLETE` for 68.
- The normalized output contains 6,700 filing-backed annual statement rows and
  14,780 filing-backed quarterly rows. Placeholder rows preserve the missing-
  not-zero state for 32 annual and 12 quarterly security outputs with no parsed
  statement rows.
- The manifest contains 42,847 artifacts and acquisition links: 40,039 valid
  and 2,808 retained failures. The control plane contains one dataset snapshot,
  1,373 universe members, 1,373 decisions, 585 DQ rows, and 585 repair rows.

## Verification evidence

Every one of the 42,847 frozen source files was re-read after finalization and
checked against both manifest `byte_count` and SHA-256 `content_hash`. The
verified raw corpus is 4,461,926,950 bytes. There were zero missing files, hash
mismatches, byte-count mismatches, path escapes, or duplicate frozen paths.
The SHA-256 of the sorted 12-line `<file-sha256><two spaces><filename>` manifest
for the normalized top-level deliverables is
`9f1f9add629d768c0a076ce674aef7620258c272db5b610d68da505bc0f2411e`.

Semantic QA additionally proved:

- exact equality between the final ISIN set and the frozen parent eligible set;
- unique 1,373-row security, market, status, member, and decision grains;
- all 816 passes have annual and quarterly completeness of at least 70% and
  `VALIDATED` corporate-action continuity;
- every technical status remains `UNAVAILABLE`, separate from filing admission;
- all real statement rows have a source-row hash and a manifest-resolved source
  artifact; and
- all 6,700 annual and 14,780 quarterly filing-backed rows were published no
  later than the 2026-08-12 cutoff.

The acquisition retained failed document evidence rather than hiding it. Failed
artifacts comprise 2,807 linked XBRL documents and one BSE metadata response.
Observed failure evidence includes wrong-document ISINs, exchange-ID mismatch,
missing document ISINs, malformed filings, and unavailable BSE locators. There
were no HTTP 429 or 403 artifacts. A failed candidate document does not by
itself fail a security when a complete, identity-valid whole-provider snapshot
still passes the contract.

Targeted verification completed with 62 passing research-screener/demerger
tests. `scripts/check_docs.py` validated all 113 current documents with no
issues, and `git diff --check` passed.

## Superseded 1.1.0 next-work record

Do not rank the 816 `BOUNDARY_REVIEW` securities yet. The next safe sequence is:

1. Partition the 517 fundamental repairs into genuine short/recent-listing
   history, missing latest annual or quarterly periods, unresolved statement
   scope, document-identity mismatch, malformed XBRL, and unavailable official
   locator lanes. Add an issuer/RHP repair only when the official, checksum-
   locked, same-scope contract can be satisfied; never fill a gap from an
   arbitrary financial website.
2. Reconcile the 68 corporate-action failures against the operational adjusted-
   price action store. Keep technical and per-share conclusions suppressed until
   each required split, bonus, rights, consolidation, merger, or demerger event
   matches official evidence.
3. Add broad-universe issuer classification before applying bank, financial-
   institution, market-infrastructure, or industrial metric contracts. Do not
   route a company by name similarity or silently apply industrial completeness
   rules to a bank.
4. Only after repair reruns create a new immutable superseding snapshot, begin
   annual-report and primary-filing research evidence discovery for the filing-
   grade cohort. That step should extract attributable qualitative evidence,
   governance/shareholding facts, business-specific KPI definitions, capex/order
   book statements, and management guidance; it must not rewrite filed numeric
   statements or create an investment recommendation.

The operational trading pipeline, schedules, broker state, and execution stores
were not changed by this run.

## Repair baseline checkpoint

The first three next-work gates are now implemented as a deterministic repair
partition and filing-definition 1.2.0 classification contract. The 517
fundamental members partition into 278 genuine post-listing history gaps, 132
other missing historical periods, 75 filed-metric gaps, 21 latest-document
validation failures, and 11 unresolved scopes. The 68 corporate-action members
contain 71 unmatched events: 42 rights, 14 scheme, 14 split/bonus, and one
consolidation event. Only the split/bonus set is a candidate for bounded
operational reconciliation; no write is authorized by the partition itself.

Definition 1.2.0 freezes exact-ISIN current-master sector/industry evidence and
fails closed when it cannot select a bank, financial-institution, market-
infrastructure, or industrial contract. See the
[filing repair baseline](filing_repair_baseline.md) for counts, treatment, and
the mutation boundary. Those downstream gates are now complete; the current
next steps are recorded in the
[annual-report handoff](annual_report_discovery_handoff.md).
