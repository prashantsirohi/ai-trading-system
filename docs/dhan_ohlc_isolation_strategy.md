# Dhan OHLC Isolation Strategy

This strategy is for isolating Dhan daily OHLC corruption independently from the
main ranking pipeline.

## Goal

Identify whether mismatch comes from:

- Dhan source payload
- local write/parsing path
- date-shift parsing bug
- scale/unit distortion

## Isolated Workflow

1. Run Dhan diagnostics only (no ranking/publish dependency):

```bash
cd /Users/prashant/my-ai-project/trading_system/ai-trading-system
PYTHONPATH=. ./.venv/bin/python -m collectors.dhan_ohlc_diagnostics \
  --from-date 2026-03-31 \
  --to-date 2026-04-06 \
  --exchange NSE \
  --symbol-limit 100
```

2. Review report artifacts in `reports/dhan_diagnostics/...`:

- `diagnostic_report.json`
- `symbol_diagnostics.csv`

3. Classify dominant issue tags:

- `db_vs_dhan_mismatch`
- `dhan_vs_reference_mismatch`
- `possible_one_day_shift`
- `possible_scale_issue`
- `missing_in_reference`
- `missing_in_dhan`

4. Apply targeted fix:

- if `possible_one_day_shift` is high:
  - audit Dhan timestamp unit/date parsing path first
- if `possible_scale_issue` is high:
  - enforce scale-ratio guardrails before writes
- if `dhan_vs_reference_mismatch` is high:
  - keep `NSE -> yfinance` as source-of-truth for ranking
  - keep Dhan for execution/broker workflows
- if `missing_in_reference` is high:
  - quarantine and skip symbols for ranking/trading until verified

5. Re-run diagnostics on same window and confirm issue counts improve before
   changing ranking/execute trust controls.

## Dhan Daily Call Semantics

- Dhan historical timestamps are interpreted as UTC epoch and normalized to IST
  before date-level comparisons.
- Daily Dhan validation mode uses a fixed IST window:
  - `fromDate = today - 1`
  - `toDate = today`
- Missing `today` candle is treated as an exchange/data-availability condition,
  not parser failure (for example, holidays or delayed close availability).
- Weekends/holidays in the middle of a requested range are expected to return no
  candle rows for those dates.

## Recent Date Findings (April 2026)

- Postman and live collector checks showed Dhan epoch timestamps like:
  - `1774981800` -> `2026-03-31T18:30:00Z` -> `2026-04-01 00:00:00 IST`
  - `1775068200` -> `2026-04-01T18:30:00Z` -> `2026-04-02 00:00:00 IST`
  - `1775413800` -> `2026-04-05T18:30:00Z` -> `2026-04-06 00:00:00 IST`
- This confirms the old mismatch pattern came from date interpretation (UTC
  truncation) rather than OHLC value corruption for those samples.
- `toDate` can behave as a practical boundary in some windows, so missing the
  final requested trading day should be treated as possible API boundary
  behavior unless cross-source validation confirms a hard mismatch.
- Operational rule: always compare Dhan candles on IST-normalized trade dates
  and keep validator checks (NSE/yfinance) enabled for daily confidence.

## Acceptance Criteria

- no unresolved critical issue tags in active trading window
- no broad one-day-shift or scale distortion signature
- trust status remains `trusted` or controlled `degraded` with explicit quarantine
