# Ranking Factors

- **Purpose:** Enumerate every factor the rank stage computes, with raw inputs, transformation, weight, and caveats.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/domains/ranking/factors.py`, `src/ai_trading_system/domains/ranking/ranker.py`, `src/ai_trading_system/domains/ranking/composite.py`, `src/ai_trading_system/domains/ranking/contracts.py`, `src/ai_trading_system/platform/config/rank_factor_weights.json`, `src/ai_trading_system/domains/ranking/payloads.py`.

## How a row is scored

1. `StockRanker.rank_all` (`ranker.py:95`) loads the latest market snapshot and applies the seven factor calculators in `factors.py` in this order: relative strength → momentum acceleration → volume intensity → trend persistence → proximity to highs → delivery → sector strength (`ranker.py:130-136`).
2. `compute_factor_scores` (`composite.py:107`) winsorises every raw column at the 5th/95th percentile (`composite.py:58-74`), demeans `rel_strength`, `volume_intensity_normalized`, and `trend_score` by sector median (`composite.py:24`, `:77-85`, `:96-101`), then percentile-ranks each raw value to a 0–100 score (`composite.py:117-121`).
3. `composite_score = Σ (factor_score × weight)`, with `sector_strength_score` computed as `sector_rs_score × 0.6 + stock_vs_sector_score × 0.4` (`composite.py:125-132`).
4. Stage 2 bonuses, penalties, and adjustments produce `composite_score_adjusted` (`ranker.py:175-181`):
   `adjusted = composite + stage2_score_bonus + stage2_freshness_bonus + stage2_transition_bonus − penalty_score`, clipped to `[0, 100]`.

The weight file (`rank_factor_weights.json`) currently sets `volume_intensity`, `momentum_acceleration`, and `delivery_pct` to **0.0**. Per the comment in `contracts.py:26-29`, these are event signals that belong in the breakout/pattern layer; their score columns are still emitted for downstream consumers but they do not feed `composite_score`.

## Composite scoring factors

| Factor | Raw feature column | Transformation | Weight | Interpretation | Caveats |
|---|---|---|---|---|---|
| Relative strength | `rel_strength` | Multi-period blend: `0.2·rank_pct(return_20) + 0.5·rank_pct(return_60) + 0.3·rank_pct(return_120)` (each ranked to 0–100) (`factors.py:24-27`). Then blended with NIFTY-relative RS at `NIFTY_RS_BLEND=0.4` (`ranker.py:50`, `:312-320`). Final value winsorised + sector-demeaned (`composite.py:24`) then percentile-ranked to 0–100. | **0.38** | Cross-sectional momentum vs universe and vs benchmark. | If only one of the three return periods is present, falls back to `return_pct` directly (`factors.py:28-34`). Benchmark blend is a no-op when NIFTY history is missing (`ranker.py:297-298`). |
| Volume intensity | `volume_intensity_normalized` (derived from `vol_intensity = volume / vol_20_avg`) | `vol_intensity` clipped to `[0, 5]` (ratio component); `volume_zscore_20` clipped to `[-2, 5]` then mapped via `1 + z/2`, clipped to `[0, 3.5]`. Final: `0.6·ratio + 0.4·z_component` (`factors.py:87-94`). Winsorised + sector-demeaned, percentile-ranked. | **0.0** | Today's volume vs 20-bar average and z-score. | Weight is 0 in `rank_factor_weights.json`. Score column still emitted; not in `composite_score`. |
| Trend persistence | `trend_score` (composite of ADX and SMA alignment) | `adx_score = clip(adx_14, 0, 50) / 50 × 100` (`factors.py:127-131`). `sma_alignment_score = 40·(close>sma_20) + 60·(close>sma_50)` (`factors.py:143-146`). `trend_score = 0.7·adx_score + 0.3·sma_alignment_score` (constants `TREND_STRENGTH_WEIGHT=0.7`, `TREND_ALIGNMENT_WEIGHT=0.3` at `factors.py:8-9`). Winsorised + sector-demeaned, percentile-ranked. | **0.22** | Directional strength (ADX) plus posture above short MAs. | Missing ADX defaults to 50; missing SMAs default to `close` (`factors.py:118-123`). |
| Momentum acceleration | `momentum_acceleration` | `0.6·(return_5 − return_20) + 0.4·(return_10 − return_20) + 0.25·rs_delta` where `rs_delta` is the first present of `{rel_strength_delta, rel_strength_slope, rs_delta, rs_slope}` (`factors.py:46-57`). Winsorised, percentile-ranked. | **0.0** | Short-term return acceleration vs the 20-bar baseline. | Weight is 0; score still emitted. |
| Proximity to highs | `prox_high` | `clip(close / high_52w, 0, 1) × 100` (`factors.py:175-180`). `high_52w` is a windowed `MAX(high)` over up to 252 trailing bars; `prox_lookback_days` records the actual window used and `is_short_history` flags rows where it is below `SHORT_HISTORY_BARS_THRESHOLD = 252` (`factors.py:155`, `:181-187`). | **0.18** | How close the symbol is to its 52-week high. | Newly-listed names get a partial-window high; consumers should de-rate using `is_short_history`. |
| Delivery percentage | `delivery_pct` | Fills missing values with sector median, then universe median (default 20.0 if universe median is also NaN); persists `delivery_pct_imputed` (`factors.py:208-225`). Winsorised, percentile-ranked. | **0.0** | NSE delivery ratio (longer-conviction holders). | Weight is 0; score still emitted. |
| Sector strength | `sector_rs_value`, `stock_vs_sector_value` | Both default to `0.5` / `0.0` when sector inputs are missing (`factors.py:238-241`). Each is percentile-ranked, then `sector_strength_score = 0.6·sector_rs_score + 0.4·stock_vs_sector_score` (`composite.py:123-127`). | **0.22** | Sector momentum and the stock's edge within its sector. | Sector mapping comes from `master.duckdb`; missing mappings default to 0.5/0.0. |

`composite_score` is the weighted sum of all six primary factor scores plus `sector_strength_score × 0.22` (`composite.py:129-132`). The seven weight keys sum to 1.00.

## Stage 2 bonuses (additive into `composite_score_adjusted`)

| Field | Value | Source |
|---|---|---|
| `stage2_score_bonus` | `(stage2_score / 100) × 5.0` | `ranker.py:147-151` |
| `stage2_freshness_bonus` | `4.0` if Stage 2 and `bars_in_stage ≤ 8`; `2.0` if `8 < bars ≤ 15`; `0.0` otherwise. Sets `stage2_age_warning="mature_stage2"` when `bars ≥ 16`. | `ranker.py:525-545`, `contracts.py:40-43` |
| `stage2_transition_bonus` | `5.0` when `weekly_stage_transition="S1_TO_S2"` and `bars_in_stage ≤ 8`. | `ranker.py:542-544`, `contracts.py:44-45` |

Constants: `STAGE2_FRESH_BARS_MAX=8`, `STAGE2_MID_BARS_MAX=15`, `STAGE2_FRESHNESS_BONUS=4.0`, `STAGE2_MID_FRESHNESS_BONUS=2.0`, `STAGE2_TRANSITION_BONUS=5.0`, `STAGE2_TRANSITION_BONUS_BARS_MAX=8` (`contracts.py:40-45`).

## Penalty score (subtracted from composite)

All checks are additive into `penalty_score`, floor-clipped at 0 (`factors.py:262-336`).

| Trigger | Penalty | Source |
|---|---|---|
| `close < sma_200` | +10.0 | `factors.py:271-274` |
| `liquidity_score < 0.20` | +10.0 | `factors.py:276-278` |
| `atr_14 / close > 0.08` | +5.0 | `factors.py:280-283` |
| Exhaustion (mild): one of {`volume_zscore_20 > 3`, `bb_width_percentile` extreme (>0.95 or >95), `close/sma_20 − 1 > 0.12`} | +3.0; `exhaustion_flag="mild_exhaustion"` | `factors.py:285-313` |
| Exhaustion (strong): `volume_zscore_20 > 4` OR `close/sma_20 − 1 > 0.20` OR ≥2 of the mild conditions | +8.0; `exhaustion_flag="strong_exhaustion"` | `factors.py:288-313` |
| Pivot distance: `(close − pivot)/atr > 1.5` | +3.0 (`pivot_distance_penalty`) | `factors.py:317-332` |
| Pivot distance: `(close − pivot)/atr > 2.5` | +6.0 (overrides 3.0) | `factors.py:332-333` |

Pivot is the first present of `{pivot_price, breakout_level, resistance_level, watchlist_trigger_level}`; ATR is the first present of `{atr_14, atr_value, atr_20}` (`factors.py:317-327`).

## Signal freshness

`add_signal_freshness` (`factors.py:340-364`) computes `signal_age` (days since `signal_start_date` if present, else since the latest timestamp) and `signal_decay_score = clip(1 − age/30, 0, 1)`.

## Rank confidence

`compute_rank_confidence` (`composite.py:161-185`) multiplies feature confidence by an eligibility gate and shrinks the result by `1 − penalty/100` (penalty clipped to `[0, 50]`).

## Output columns

The artifact contract is `RANKED_SIGNAL_COLUMNS` in `contracts.py:56-138`. Notable groups:

- Identifiers: `symbol_id`, `exchange`, `close`, `timestamp`.
- Scores: `composite_score`, `composite_score_adjusted`, `rel_strength_score`, `vol_intensity_score`, `trend_score_score`, `momentum_acceleration_score`, `prox_high_score`, `delivery_pct_score`, `sector_strength_score`.
- Raw inputs: `rel_strength`, `vol_intensity`, `volume_intensity_normalized`, `momentum_acceleration`, `trend_score`, `prox_high`, `delivery_pct`, `sector_rs_value`, `stock_vs_sector_value`, `sector_name`.
- Diagnostics: `eligible_rank`, `rejection_reasons`, `penalty_score`, `rank_confidence`, `signal_age`, `signal_decay_score`, `exhaustion_penalty`, `exhaustion_flag`, `pivot_distance_penalty`, `distance_from_pivot_atr`.
- Stage 2 / weekly: `stage2_score`, `is_stage2_structural`, `is_stage2_candidate`, `is_stage2_uptrend`, `stage2_label`, `stage2_hard_fail_reason`, `stage2_fail_reason`, `stage2_score_bonus`, `stage2_freshness_bonus`, `stage2_transition_bonus`, `stage2_age_warning`, `weekly_stage_label`, `weekly_stage_confidence`, `weekly_stage_transition`, `bars_in_stage`, `stage_entry_date`.
- NIFTY-relative RS: `rs_vs_nifty_5/10/20/60/120`, `rs_vs_nifty_score`.
- Sector positioning: `sector_rank_within_sector`, `sector_total_symbols`.
- History flag: `high_52w`, `prox_lookback_days`, `is_short_history`.

Operator-dashboard enrichment (`payloads.py:142-162`) additionally derives `stage_label`, `stage_transition`, and `stage_freshness_bucket` (`fresh_s2` ≤ 8 bars, `mature_s2` ≤ 15, `extended_s2` > 15; `payloads.py:73-90`), and joins the top pattern family per symbol.

## Rank modes

`RANK_MODES = ("default", "momentum", "breakout", "defensive", "watchlist", "stage2_breakout")` (`contracts.py:47-54`). Only `stage2_breakout` mutates filtering: it hard-gates to rows with `is_stage2_structural` (fallback `is_stage2_uptrend`) before scoring (`ranker.py:154-167`).
