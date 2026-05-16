# Breakouts and Patterns

- **Purpose:** Catalog the breakout setup families, pattern scanners, tiering, trend/regime gates, and output schemas produced by the rank stage.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/domains/ranking/breakout.py`, `src/ai_trading_system/domains/ranking/patterns/` (`detectors.py`, `contracts.py`, `evaluation.py`, `signal.py`, `__init__.py`, `cache.py`, `data.py`, `universe.py`), `src/ai_trading_system/domains/features/pattern_features.py`, `src/ai_trading_system/domains/ranking/service.py`, `src/ai_trading_system/domains/ranking/payloads.py`.

## Breakout taxonomy

`scan_breakouts` (`breakout.py:598`) computes per-symbol features from `_catalog` OHLCV and classifies each row with these boolean triggers (`breakout.py:883-894`):

| Trigger column | Condition | Canonical `setup_family` / `taxonomy_family` |
|---|---|---|
| `is_resistance_breakout_50d` | `close > prior_high_50` (MAX(high) over rows [-50, -1]) | `resistance_breakout_50d` |
| `is_high_52w_breakout` | `close > prior_high_252` | `high_52w_breakout` |
| `is_consolidation_breakout` | `is_range_breakout` AND `contraction_ratio ≤ 0.7` | `consolidation_breakout` |
| `is_volatility_expansion_breakout` | `is_range_breakout` AND `day_range_pct ≥ 1.2·atr_pct` AND `atr_pct ≤ 5.5` | `volatility_expansion_breakout` |

Legacy families (kept for backward compatibility via `include_legacy_families`, default True) map onto canonical names via `_canonical_family_from_legacy` (`breakout.py:114-120`):

| Legacy `setup_family` | Canonical `taxonomy_family` |
|---|---|
| `base_breakout` (30/60-day base, narrow range, near 52W high) | `resistance_breakout_50d` |
| `contraction_breakout` (tight 20-bar range + 60-bar context) | `consolidation_breakout` |
| `supertrend_flip_breakout` (Supertrend(10,3) flip-up + range breakout) | `volatility_expansion_breakout` |

The legacy candidate filters use `min_volume_ratio=1.2`, `min_adx=18.0`, `above_sma_20 & above_sma_50`, `supertrend_bullish` (`breakout.py:966-974`) and per-family bounds:
- Base breakout: `base_width_pct_30 ∈ [4, 18]`, `base_width_pct_60 ∈ [6, 28]`, `breakout_pct ≤ 4.0`, `near_52w_high_pct ≤ 12.0`, `contraction_ratio ≤ 0.9` (`breakout.py:976-985`).
- Contraction breakout: `range_width_pct ∈ [2, 12]`, `base_width_pct_60 ∈ [8, 30]`, `contraction_ratio ≤ 0.7`, `breakout_pct ≤ 3.5`, `near_52w_high_pct ≤ 10.0`, `atr_pct ≤ 5.0` (`breakout.py:998-1008`).
- Supertrend flip: `supertrend_flip_up & is_range_breakout`, `breakout_pct ≤ 3.0`, `range_width_pct ∈ [3, 20]`, `near_52w_high_pct ≤ 14.0` (`breakout.py:1021-1029`).

### v2 score contract (`compute_breakout_v2_scores`, `breakout.py:207`)

`breakout_score` is an integer additive composite (`breakout.py:265-281`):

```
+1  is_resistance_breakout_50d
+2  is_high_52w_breakout
+2  is_consolidation_breakout
+1  is_volume_ratio_confirmed
+2  rel_strength_score ≥ 80
+   volume confirmation bonus:
        +2  strong volume confirmation
        +2  ratio AND z20/z50 combined
        +1  z20-only or z50-only
```

Volume thresholds (`breakout.py:16-18`): `VOLUME_Z20_CONFIRM_THRESHOLD=2.0`, `VOLUME_Z50_CONFIRM_THRESHOLD=2.0`, `VOLUME_Z20_STRONG_THRESHOLD=3.0`. `is_volume_ratio_confirmed` requires `volume_ratio ≥ 1.5` (default; `breakout.py:182-204`).

Market-stage deflation (`breakout.py:287-294`): in `S3` (topping) markets, `breakout_score` and `setup_quality` are scaled by `0.65`; in `S4` (bear) both are zeroed. The service layer already suppresses publishing in S4.

## Tiering and lifecycle states

`candidate_tier` is set per row (`breakout.py:357-394`):

- **With structural Stage 2 available** (`is_stage2_structural` column present):
  - `A` if `is_stage2_structural` AND `stage2_score ≥ 85`
  - `B` if `is_stage2_structural` AND `stage2_score ≥ 70`
  - `C` if `is_stage2_candidate`
  - `D` otherwise (non-structural Stage 2)
- **Stage 2 score-only fallback** (>50% rows have `stage2_score`): A=`≥85`, B=`≥70`, C=`≥50`, D otherwise.
- **Legacy three-condition fallback**: `pass_count = above_sma200 + sma50_slope_positive + near_52w_high_ok`; tier A=0 fails, B=1 fail, C otherwise (`breakout.py:382-394`).

`breakout_state` derived from tier + gates (`breakout.py:440-469`):

| State | Meaning |
|---|---|
| `qualified` | Tier A and all regime/trend gates pass |
| `watchlist` | Tier B/C/D and not filtered (score below qualified bar) |
| `filtered_by_regime` | Market bias / breadth / sector RS gate failed |
| `filtered_by_symbol_trend` | Tier D (Stage 2 case) or Tier C (legacy) when trend gate is on |

`execution_label` (`breakout.py:1158-1168`): `ACTIONABLE_BREAKOUT` (BULLISH bias), `EARLY_BREAKOUT` (NEUTRAL), `RELATIVE_STRENGTH_BREAKOUT` (BEARISH), `WATCHLIST_BREAKOUT` (watchlist), `FILTERED_BREAKOUT` (filtered).

## Trend and regime gates (applied before scoring or ranking)

| Gate | Threshold | Source |
|---|---|---|
| Market bias allowlist | Default `{BULLISH, NEUTRAL}`; overridable via `market_bias_allowlist` param | `breakout.py:123-129`, `:306` |
| Market breadth | `breadth_score ≥ min_breadth_score` (default `45.0`) | `breakout.py:211`, `:307` |
| Sector RS absolute | `sector_rs_value ≥ sector_rs_min` (default `None` → pass) | `breakout.py:213-215`, `:312-315` |
| Sector RS percentile | `sector_rs_percentile ≥ sector_rs_percentile_min` (default `60.0`) | `breakout.py:215-216`, `:316-319` |
| Symbol trend (`above_sma200`) | Derived from `close > sma_200`; missing → pass | `breakout.py:326-331` |
| Symbol trend (`sma50_slope_positive`) | `sma50_slope_20d_pct > 0`; missing → pass | `breakout.py:333-334` |
| Symbol trend (`near_52w_high_ok`) | `near_52w_high_pct ≤ breakout_symbol_near_high_max_pct` (default `15.0`) | `breakout.py:336-339` |
| Stage 2 hard gate (mode `stage2_breakout`) | Requires `is_stage2_structural` (fallback `is_stage2_uptrend`) | `ranker.py:154-167` |

Legacy-engine common filter additionally requires: `volume_ratio ≥ min_volume_ratio` (default `1.2`), `adx_14 ≥ min_adx` (default `18.0`), `above_sma_20 & above_sma_50`, `supertrend_bullish` (`breakout.py:966-974`).

## Conviction score (Phase 3/4 features, `breakout.py:903-963`)

Per-row 0–100 conviction score from five binary flags:

```
35 · breakout_level_confirmed
+ 25 · volume_ratio_confirmed       (volume_ratio_20 ≥ 1.5)
+ 20 · delivery_surge_confirmed     (today > own 20d AND > sector 20d median)
+ 10 · range_expansion_confirmed    (day_range_pct ≥ 1.5·atr_pct)
+ 10 · close_near_high_confirmed    (close in top 25% of day range)
```

Supporting features: `volume_ratio_20`, `volume_trend_20_20`, `volume_dryup_10`, `delivery_pct_today`, `delivery_pct_20d_avg`, `delivery_surge_today`, `delivery_vs_sector_20d`, `delivery_sector_median_20d`, `close_position_in_range`.

## Pattern scan families

Live operational scanner: `domains/ranking/patterns/evaluation.py::build_pattern_signals` calls `detect_pattern_signals_for_symbol` in `detectors.py:2693` per symbol. Detectors run in three groups (`detectors.py:2730-2755`):

### Smoothed-extrema detectors (require `min_history_bars`, default 120)

| Family | Detector function | Module |
|---|---|---|
| `cup_handle` | `detect_cup_handle_signals` (`detectors.py:606`) | `domains/ranking/patterns/detectors.py` |
| `round_bottom` | `detect_round_bottom_signals` (`:748`) | same |
| `double_bottom` | `detect_double_bottom_signals` (`:893`) | same |
| `flag` | `detect_flag_signals(..., high_tight_only=False)` (`:1041`) | same |
| `high_tight_flag` | `detect_flag_signals(..., high_tight_only=True)` (`:1041`, `:1061`) | same |
| `ascending_triangle` | `detect_ascending_triangle_signals` (`:1237`) | same |
| `symmetrical_triangle` | `detect_symmetrical_triangle_signals` | same |
| `ascending_base` | `detect_ascending_base_signals` (`:1931`) | same |

### Frame-only detectors

| Family | Detector function |
|---|---|
| `vcp` | `detect_vcp_signals` (`detectors.py:1379`) |
| `flat_base` | `detect_flat_base_signals` (`:1551`) |
| `stage2_reclaim` | `detect_stage2_reclaim_signals` (`:1655`) |
| `three_weeks_tight` (`3wt`) | `detect_3wt_signals` (`:2182`) |
| `darvas_box` | `detect_darvas_box_signals` (`:1747`) |
| `pocket_pivot` | `detect_pocket_pivot_signals` (`:1840`) |
| `inside_week_breakout` | `detect_inside_week_breakout_signals` (`:2091`) |
| `inside_day` | `detect_inside_day_signals` |

### Young-history detectors (require `ipo_base_min_history_bars=35`)

| Family | Detector function |
|---|---|
| `ipo_base` | `detect_ipo_base_signals` (`detectors.py:2011`) |

### Exclusion filter

`head_shoulders` (`detectors.py:2708-2728`): if a confirmed H&S top is detected, all bullish signals for that symbol are dropped and a single bearish signal is emitted with `pattern_operational_tier="suppression_only"`.

Feature precomputation lives in `domains/features/pattern_features.py::compute_pattern_preconditions` (`pattern_features.py:7`).

Pattern signal lifecycle states (`pattern_state`): `confirmed` and `watchlist`. Volume confirmation uses the same `VOLUME_Z20_CONFIRM_THRESHOLD=2.0`, `VOLUME_Z50_CONFIRM_THRESHOLD=2.0`, `VOLUME_Z20_STRONG_THRESHOLD=3.0` thresholds as breakout (`detectors.py:35-37`).

## Pattern tiering

`_operational_tier_for_family` (`detectors.py:383-389`) maps each `pattern_family` to a `pattern_operational_tier`:

| Tier | Families | Source |
|---|---|---|
| `tier_1` | `cup_handle`, `flat_base`, `vcp`, `stage2_reclaim`, `3wt`, `ascending_triangle`, `darvas_box`, `pocket_pivot`, `ascending_base`, `ipo_base`, `inside_week_breakout` | `detectors.py:19-31` |
| `suppression_only` | `head_shoulders` | `detectors.py:32-34` |
| `tier_2` | Everything else (e.g., `round_bottom`, `double_bottom`, `flag`, `high_tight_flag`, `symmetrical_triangle`, `inside_day`) | `detectors.py:383-389` |

## Pattern score and priority score

`_score_signal_rows` (`detectors.py:392-602`) computes two scores per signal, both clipped to 100:

**`pattern_score`** (raw scoring):
- State bonus: `confirmed=+40`, `watchlist=+20`.
- Volume: strong confirmation `+20`, combined confirmation `+18`.
- Relative strength: `≥80→+15`, `≥60→+8`.
- Sector RS percentile: `≥70→+10`, `≥60→+5`.
- `volume_dry_up=+10`.
- Family clarity bonuses (`+10`): tight `cup_handle` handle (≤8%), symmetric `round_bottom` (0.75–1.35), `double_bottom` trough similarity ≤3%, `flag` retracement ≤25%, `high_tight_flag` pole ≥90% + tightness ≤15%.
- Stage 2 bonus: `stage2_score ≥85→+15`, `≥70→+10`, `≥50→+5`.

**`pattern_priority_score`** (operator ranking, `detectors.py:578-588`):

```
0.35 · pattern_score
+ tier_bonus (tier_1=+22, tier_2=+12, suppression_only=0)
+ stage2_priority_bonus (≥85→+14, ≥70→+9, ≥50→+4)
+ rs_priority_bonus (≥80→+10, ≥60→+5)
+ sector_priority_bonus (≥70→+6, ≥60→+3)
+ breakout_priority_bonus (confirmed+strong=10, confirmed+combined=8, confirmed=4, combined=4)
+ clip(setup_quality · 0.12, 0, 12)
+ clarity_bonus (tighter clarity thresholds: 8 / 4)
```

`pattern_rank` orders by `(pattern_score, setup_quality, symbol_id)`; `pattern_priority_rank` orders by `(pattern_priority_score, pattern_score, setup_quality, symbol_id)` (`detectors.py:589-602`).

## Output columns

The rank stage writes per-task CSVs (see `TASK_FILE_MAP` in `service.py:27-37`). The breakout artifact is named `breakout_scan` (filename `breakout_scan.csv`) and the pattern artifact is named `pattern_scan` (filename `pattern_scan.csv`). Operator-facing dashboard fields are assembled in `payloads.py`.

### Breakout CSV (`breakout_scan.csv`) — column list from `breakout.py:1175-1249`

`symbol_id`, `sector`, `setup_family`, `legacy_setup_family`, `taxonomy_family`, `execution_label`, `market_regime`, `market_bias`, `breakout_detected`, `filtered_by_regime`, `filtered_by_symbol_trend`, `breakout_state`, `filter_reason`, `breakout_score`, `breakout_rank`, `candidate_tier`, `symbol_trend_score`, `symbol_trend_reasons`, `symbol_trend_fail_count`, `close`, `prior_range_high`, `breakout_pct`, `base_width_pct_30`, `base_width_pct_60`, `contraction_ratio`, `volume_ratio`, `volume_zscore_20`, `volume_zscore_50`, `is_volume_ratio_confirmed`, `is_z20_confirmed`, `is_z50_confirmed`, `is_any_volume_confirmed`, `is_any_volume_confirmed_breakout`, `is_strong_volume_confirmation`, `adx_14`, `near_52w_high_pct`, `sma50_slope_20d_pct`, `above_sma200`, `range_width_pct`, `supertrend_dir_10_3`, `prev_supertrend_dir_10_3`, `setup_quality`, `breakout_tag`, `rel_strength_score`, `sector_rs_value`, `sector_rs_percentile`, `is_resistance_breakout_50d`, `is_high_52w_breakout`, `is_consolidation_breakout`, `is_volatility_expansion_breakout`, `is_volume_confirmed_breakout`, `market_bias_allowed`, `breadth_gate_passed`, `sector_gate_passed`, `regime_gate_passed`, `volume_ratio_20`, `volume_trend_20_20`, `volume_dryup_10`, `delivery_pct_today`, `delivery_pct_20d_avg`, `delivery_surge_today`, `delivery_vs_sector_20d`, `delivery_sector_median_20d`, `close_position_in_range`, `breakout_level_confirmed`, `volume_ratio_confirmed`, `volume_ratio_strong`, `delivery_surge_confirmed`, `range_expansion_confirmed`, `close_near_high_confirmed`, `conviction_score`.

Sort order: `(breakout_rank, setup_quality)` ascending/descending; default `top_n=25` (`breakout.py:604`, `:1253`).

### Pattern CSV (`pattern_scan.csv`) — columns from `PatternSignal.to_record` (`patterns/contracts.py:237-286`) plus scoring fields

Core `PatternSignal` fields (`contracts.py:238-277`): `signal_id`, `symbol_id`, `pattern_family`, `pattern_state`, `signal_direction`, `pattern_start`, `pattern_end`, `signal_date`, `pattern_start_index`, `pattern_end_index`, `signal_bar_index`, `breakout_level`, `watchlist_trigger_level`, `invalidation_price`, `pattern_score`, `pattern_rank`, `setup_quality`, `pivot_labels`, `pivot_dates`, `pivot_prices`, `pivot_indices`, `volume_ratio_20`, `volume_zscore_20`, `volume_zscore_50`, `rel_strength_score`, `sector_rs_percentile`, `breakout_volume_ratio`, `width_bars`, `volume_dry_up`, `cup_depth_pct`, `handle_depth_pct`, `symmetry_ratio`, `trough_similarity_pct`, `pole_rise_pct`, `flag_tightness_pct`, `flag_retracement_pct`, `config_provenance`.

Added by `_score_signal_rows` (`detectors.py:392-602`): `pattern_operational_tier`, `pattern_priority_score`, `pattern_priority_rank`, optional `stage2_label`.

Operator dashboard surfaces the top pattern per symbol via `_top_pattern_summary` (`payloads.py:93-139`) with columns `top_pattern_family`, `top_pattern_state`, `top_pattern_setup_quality`, `top_pattern_pivot_price`, `top_pattern_invalidation_price`, `reclaim_signal_flag`.
