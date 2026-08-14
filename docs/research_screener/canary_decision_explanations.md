# Live canary decision explanations

- **Purpose:** Human-readable company-level disposition summary for the live 17-company canary.
- **Audience:** Research operators and data-repair owners.
- **Last verified:** 2026-08-11
- **Source of truth:** Live run `screen-live_canary-2026-08-11-51039c3d718379d2` and its source manifest.

Authoritative field-level explanations and artifact IDs are in the live run pack
at `$DATA_ROOT/research_screener/runs/screen-live_canary-2026-08-11-51039c3d718379d2/canary_decision_explanations.md`.
This summary keeps replay and live evidence separate.

| Symbol | Official cap (₹ crore) | Eligibility / final disposition | Annual / quarterly completeness | Action / technical state |
|---|---:|---|---:|---|
| ACE | 12,801.44 | Eligible / `BOUNDARY_REVIEW` | 1.0 / 0.9167 | Validated / `CONFIRMED` |
| BANKBARODA | 127,318.94 | Above ceiling / `INELIGIBLE_MARKET_CAP` | 0.125 / 0.1875 bank contract | Validated / `WEAK_STRUCTURE` |
| CAMS | 19,565.85 | Eligible / `BOUNDARY_REVIEW` | 1.0 / 0.9167 | Validated / `CONFIRMED` |
| DEEPAKNTR | 24,264.32 | Eligible / `BOUNDARY_REVIEW` | 0.8333 / 0.8333 | Validated / `CONFIRMED` |
| DIXON | 85,739.61 | Eligible / `BOUNDARY_REVIEW` | 1.0 / 0.9167 | Validated / `CONFIRMED` |
| E2E | 12,829.30 | Eligible / `DATA_REPAIR_REQUIRED` | 0.0 / 0.0833 | Split validated / `CONFIRMED` |
| ETERNAL | 306,881.15 | Above ceiling / `INELIGIBLE_MARKET_CAP` | 0.8333 / 1.0 | Validated / `CONFIRMED` |
| HAWKINCOOK | 4,336.83 | Eligible / `BOUNDARY_REVIEW` | 0.8333 / 0.75; explicit gaps retained | Validated / `UNAVAILABLE` (no local OHLCV) |
| HDFCBANK | 1,123,344.65 | Above ceiling / `INELIGIBLE_MARKET_CAP` | 0.125 / 0.1875 bank contract | Validated / `WEAK_STRUCTURE` |
| KIRLPNU | 9,695.76 | Eligible / `BOUNDARY_REVIEW` | 1.0 / 1.0 | Validated / `AWAIT_BREAKOUT` |
| MCX | 73,820.14 | Eligible / `DATA_REPAIR_REQUIRED` | 0.8333 / 0.8333; latest annual document retains the prior post-split ISIN | Validated / `CONFIRMED` |
| PREMIERENE | 47,664.91 | Eligible / `DATA_REPAIR_REQUIRED` | 0.3333 / 0.6667 | Validated / `AWAIT_BREAKOUT` |
| RELIANCE | 1,791,572.80 | Above ceiling / `INELIGIBLE_MARKET_CAP` | 1.0 / 0.9167 | Validated / `WEAK_STRUCTURE` |
| SJS | 7,656.93 | Eligible / `BOUNDARY_REVIEW` | 0.8333 / 1.0 | Validated / `CONFIRMED` |
| STLTECH | 31,759.71 | Eligible / `BOUNDARY_REVIEW` | 1.0 / 1.0 | Demerger validated / `CONFIRMED` |
| WAAREEENER | 77,780.92 | Eligible / `DATA_REPAIR_REQUIRED` | 0.3333 / 0.6667 | Validated / `WEAK_STRUCTURE` |
| WELCORP | 48,640.36 | Eligible / `BOUNDARY_REVIEW` | 1.0 / 1.0 | Validated / `CONFIRMED` |

Technical observations remain separate from fundamental admission. No company
received a production fundamental score, and no missing filing or KPI value was
converted to zero.
