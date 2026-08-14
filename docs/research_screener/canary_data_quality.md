# Persistent screener canary data quality

- **Purpose:** Separate historical and live canary DQ findings with acceptance status.
- **Audience:** Canary reviewers and full-universe rollout approvers.
- **Last verified:** 2026-08-12
- **Source of truth:** The two immutable run packs and `$DATA_ROOT/research_screener/control_plane.duckdb`.

Gate result: **approved for controlled full-universe expansion**. Identity and
dated market-cap eligibility are accounted for 17/17. All 13 in-band companies
pass filing and corporate-action discovery; the remaining four are explicitly
outside the configured market-cap band. There are no blocking DQ issues or
repair-queue rows in the authoritative live run.

## Historical regression replay (separate frozen fixture)

- Authoritative rule-version 0.2.5 run: `screen-regression_replay-2026-08-08-6f27465442778028`.
- The replay is checksum-locked to fixture v1.0.0, including the originally supplied Hawkins ISIN. Live fixture corrections cannot rewrite this dataset.
- All 17 companies and 17 terminal decisions persisted.
- The supplied historical aggregate remains 13/17 inside the old market-cap band and 10/13 captured by the old index route.
- SJS, Hawkins, and E2E old silent-loss evidence is reproduced; Hawkins/E2E forced-consolidated failures and usable standalone scope are retained; SJS five-observation history is not converted to zero growth.
- The replay intentionally does not manufacture company-level historical market caps that were not supplied.

## Fresh live canary (official-source inputs through 2026-08-12)

- Authoritative rule-version 0.2.5 run: `screen-live_canary-2026-08-12-77f89150f38bd698`.
- All 17 fixture identities resolve exactly. Hawkins uses current ISIN
  `INE979B01015`; E2E uses an effective-dated bridge from `INE255Z01019` to
  `INE255Z01027` around the official 2026-06-05 split; MCX uses the corresponding
  `INE745G01035` to `INE745G01043` filing window around its 2026-01-02 split.
- All 17 companies have dated official full-market-cap outcomes. Thirteen are
  inside the ₹1,000–₹100,000 crore band. BANKBARODA, ETERNAL, HDFCBANK, and
  RELIANCE are explicitly above the ceiling.
- Filing adapters acquired NSE legacy/integrated XBRLs or BSE XBRLs, enforced
  point-in-time publication dates, exact document identity, one statement
  scope, latest-period parsing, raw values, formula versions, and row hashes.
  An incomplete NSE snapshot may fall back to one exact-listing BSE snapshot;
  exchange periods are never spliced across providers.
- Four checksum-locked issuer documents fill only missing or wholly empty
  periods after same-scope overlap reconciliation: Premier Energies and Waaree
  Energies pre-IPO prospectus history, E2E FY2021/FY2022 audited standalone
  history, and MCX's corrected FY2026 post-split annual filing. Existing usable
  exchange periods retain precedence. Missing disclosed values remain null.
- Final annual/quarterly completeness is: PREMIERENE 0.8333/0.7500,
  WAAREEENER 0.8333/0.7500, E2E 1.0000/1.0000, and MCX 0.9815/0.8333. All four
  now have `PRESENT` fundamentals and `BOUNDARY_REVIEW` dispositions.
- Hawkins' official BSE inline-XBRL documents remain exact-matched to scrip code
  `508486` and ISIN `INE979B01015`. Its 0.8333 annual and 0.7500 quarterly
  completeness passes the admission floor. Wrong-identity and malformed source
  documents remain frozen as failed evidence and are never coerced.
- All 17 official corporate-action histories validate against stored adjustment
  evidence. STLTECH's 2025 demerger and the E2E/MCX split boundaries remain
  evidence-bound. Hawkins has no local OHLCV, so its technical state remains
  `UNAVAILABLE` for that separate reason.
- KPI definitions persist for all 17 companies. Observations remain
  `NOT_DISCLOSED`; no KPI values were invented.

The final live run contains 548 unique manifest artifacts, 548 acquisition
records, 548 `ingestion_artifact` links, one dataset snapshot, and 17 decisions.
All 548 frozen source files exist and match their manifest SHA-256. The 39
`FAILED` source artifacts are retained negative evidence (for example malformed,
wrong-identity, or otherwise rejected documents), not missing files or checksum
failures. The run has zero blocking DQ issues and zero repair rows. The persisted
replay-versus-live comparison keeps both datasets separate; earlier completed
runs remain immutable forensic history.
