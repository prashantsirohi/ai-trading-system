# Filing repair baseline

- **Purpose:** Freeze the cause partition and safe remediation lanes for the completed filing-discovery 1.1.0 snapshot.
- **Audience:** Data engineering owner, filing-repair reviewer, and research-system developer.
- **Last verified:** 2026-08-12
- **Source of truth:** The 1,373 verified member checkpoints for run `screen-filing_discovery-2026-08-12-74ee4481148e4e67` and `repair_analysis.py`.

## Result

The 517 fundamental repair members partition exactly once into:

| Lane | Members | Safe treatment |
|---|---:|---|
| Genuine post-listing history gap | 278 | Consider only checksum-locked official RHP, prospectus, or issuer filing evidence under the existing identity, cutoff, scope, and overlap contract. |
| Missing historical filing periods | 132 | Recover the missing primary filing periods; do not substitute secondary-site values. |
| Filed metric completeness gap | 75 | Re-evaluate only after exact-ISIN issuer classification selects the correct metric contract. |
| Latest disclosed document not validated | 21 | Repair document identity, parsing, or locator evidence for the latest disclosed annual or quarterly period. |
| Statement scope unresolved | 11 | Establish one filing-backed standalone or consolidated scope; do not splice scopes. |

The 68 corporate-action repair members contain 71 unmatched events:

| Lane | Events | Members | Current authority |
|---|---:|---:|---|
| Rights terms and price basis required | 42 | 42 | No write; technical and per-share conclusions suppressed. |
| Scheme and successor price basis required | 14 | 14 | No write; require scheme, entitlement, successor mapping, and price-basis evidence. |
| Split/bonus operational backfill candidate | 14 | 13 | Candidate review only; no automatic write is authorized. |
| Consolidation terms and price basis required | 1 | 1 | No write; require complete official terms and price-basis validation. |

## Classification gate

Filing-discovery definition 1.2.0 freezes a read-only current-master
classification snapshot keyed by exact ISIN. It routes explicit sector and
industry evidence to `BANK`, `FINANCIAL_INSTITUTION`,
`MARKET_INFRASTRUCTURE`, or `INDUSTRIAL`. A missing, ambiguous,
post-cutoff, or incomplete classification becomes `UNCLASSIFIED` and adds a
blocking `ISSUER_CLASSIFICATION_UNRESOLVED` result. Company-name similarity is
not classification evidence.

The completed 1.2.0 snapshot has exact-ISIN, complete mastered evidence for
1,279 members. The routing maps 1,141
industrial issuers, 103 financial institutions, 30 banks, and 5 market-
infrastructure issuers. The remaining 94 members fail closed: 68 unmatched
ISINs, 8 rows previously reachable only by unsafe symbol fallback, and 18
exact-ISIN rows with incomplete sector or industry evidence.

## Repair and mutation boundary

The baseline is descriptive and does not mutate `ohlcv.duckdb`, the screener
control plane, any completed output pack, schedules, or broker state. A split or
bonus candidate still needs an official ratio, matching raw-payload hash,
pre/post-action price validation, a verified backup, and a targeted adjustment
canary before an operational write can be approved. Rights, consolidation,
merger, and demerger labels alone never define an economic adjustment factor.

The verified immutable superseding filing snapshot and annual-report discovery
are now complete. See the [annual-report handoff](annual_report_discovery_handoff.md).
The evidence remains non-scoring and may not rewrite filed numeric statements,
rank securities, or create an investment recommendation.
