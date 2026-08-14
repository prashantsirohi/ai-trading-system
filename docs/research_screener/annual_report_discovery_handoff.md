# Annual-report research discovery handoff

- **Purpose:** Record the first complete annual-report evidence-discovery run for the filing-grade cohort and define its safe use boundary.
- **Audience:** Operator, research-data engineer, filing reviewer, and investment-research reviewer.
- **Last verified:** 2026-08-14
- **Source of truth:** Run `research-annual_reports-2026-08-12-62e31fe55217095f`, its immutable output pack, and the research-screener control plane.

## Outcome

The 763 `BOUNDARY_REVIEW` members from filing snapshot
`screen-filing_discovery-2026-08-12-dc4cb64090b47027` were preserved exactly.
The annual-report definition is version 1.1.0 with snapshot hash
`62e31fe55217095f0d6dc79fcf90b4552dfe977aa92b3702189835a5b2707688`.

- 757 companies have a checksum-frozen, parsed official annual report: 726 from
  NSE and 31 from BSE. BSE supplied 20 repairs over the first discovery pass,
  including fallback for truncated or unusable NSE archive documents.
- Six companies remain `SOURCE_UNAVAILABLE`: CIANAGRO, NIITLTD, BEML,
  ABBOTINDIA, UFBL, and JAYKAY. Five have insufficient extractable text in the
  official document. NIITLTD has a truncated NSE archive object and a missing
  BSE attachment. No issuer-IR substitution was made without a reviewed,
  checksum-locked contract.
- The selected fiscal-year distribution is 396 FY2025-26 reports, 360
  FY2024-25 reports, and one FY2023-24 report. Every usable publication timestamp
  is no later than the 2026-08-12 cutoff.
- The output contains 10,417 evidence rows across governance, shareholding,
  business-KPI, capex/capacity, order-book, and management-guidance topics.
  There are 9,479 page-attributed `DISCLOSED_TEXT_MATCH` anchors and 938 explicit
  `NOT_DISCLOSED` observations.

Text matches are discovery anchors, not accepted facts. They carry LOW
confidence and `HUMAN_REVIEW_REQUIRED`; wording such as “outlook” or “expect
to” can occur outside formal guidance. A reviewer must read the cited page and
classify the statement before it becomes a management statement or KPI
observation.

## Provenance and QA

The manifest contains 1,593 artifacts and frozen source files totaling
9,332,783,784 bytes. All files were independently reread and matched both the
manifest byte count and SHA-256 checksum. The manifest contains 1,573 `VALID`
artifacts and 20 retained `FAILED` artifacts, including truncated NSE bytes;
failed evidence is preserved instead of discarded.

Semantic reconciliation found zero issues:

- exactly 763 unique ISINs equal the frozen parent cohort;
- every company has all six topic states, including explicit missing states;
- every disclosed anchor has a source artifact, excerpt, and page within the
  parsed document page count;
- no `NOT_DISCLOSED` row carries invented excerpt or page content;
- all 757 present documents resolve to a valid frozen source artifact and obey
  the publication cutoff; and
- the control plane contains one completed run with 763 `research_document`
  rows and 10,417 `research_evidence` rows.

The pre-migration control-plane backup is
`$DATA_ROOT/research_screener/control_plane.pre-annual-reports-20260812.duckdb`.
Its SHA-256 equals the pre-migration database checksum:
`a5842d462361b0e5e6687a44173582d671e8c9b6b6e850339bbbe90e827b0d86`.

## Safe next work

1. Review and curate the six unavailable documents. Use an official issuer IR
   report only after locking URL, publication timestamp, checksum, identity,
   fiscal year, and overlap with exchange metadata.
2. Run the bounded 25-company calibration only after the extractor/verifier
   runner implements the [qualitative claim contract](qualitative_claim_contract.md).
   The contract now fixes provenance, company-type routing, independent review,
   low-cost budgets, and human escalation; no existing anchor has yet been
   promoted to a claim.
3. Add structured governance/shareholding acquisition from exchange XBRL or
   integrated filings. Annual-report text may corroborate it but must not
   replace point-in-time structured ownership data.
4. Define company-type-specific KPI observations from reviewed evidence. Do not
   apply industrial order-book or EBITDA concepts to banks or financial
   institutions, and do not treat market infrastructure as a lender.
5. Keep valuation, scoring, ranking, recommendation, scheduling, and execution
   downstream until evidence review and the remaining data-repair gates are
   separately approved.

This run did not rewrite filed financial statements, screening dispositions,
operational OHLCV, corporate actions, schedules, portfolios, or broker state.
