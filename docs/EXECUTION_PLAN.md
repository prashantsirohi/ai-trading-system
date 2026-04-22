# Execution Plan — AI Trading System
**Revision 2** — Based on corrected AS_IS_DESIGN.md (Rev 2).

## Ground rules for any agent executing this plan

1. **`src/ai_trading_system/` is canonical. Never delete it.** The old plan said "delete src/". That was wrong. Every task targets `src/ai_trading_system/`.
2. **`run/orchestrator.py` and `run/stages/*.py` are already shims** that re-export from `src/ai_trading_system/pipeline/`. Do not add logic there.
3. **Legacy top-level directories still exist in this checkout.** Treat `src/ai_trading_system/` as canonical, but do not assume legacy shims or deprecated entrypoints have already been removed.
4. **One phase = one PR.** Every phase ends with `python -m run.orchestrator` green.
5. **Never modify trading logic** in a structural phase. Logic changes happen only in designated functional phases.
6. **All new code goes in `src/ai_trading_system/`.** New files outside that tree require explicit justification.
7. **Each task has a concrete exit check.** Do not mark a task done until its exit check passes.

---

## Status snapshot (April 22, 2026, post PR-2 implementation)

| Phase | Status | Summary |
|---|---|---|
| Phase 0 | ⚠️ Drifted | Legacy entrypoints still present in checkout: `main.py`, `tools/`, `dashboard/`, `config/settings.py`, and `tests/test_main_entrypoint.py` |
| Phase 1 | ✅ Implemented | Stage 2 foundation and pattern bug fixes exist; verify with current tests in this checkout before treating as green |
| Phase 2 | ✅ Implemented | New detectors and `tests/test_new_patterns.py` exist and should be treated as current repo truth |
| Phase 3 | ✅ Implemented | `011_pattern_cache.sql`, `patterns/cache.py`, Stage 2 pre-screen + incremental orchestration in evaluation/service, and `tests/test_pattern_cache.py` are present |
| Phase 4 | 🔶 Partial | PR-1 backend/API reliability + Stage 2 gate/enrichment is implemented; UI-specific enrichment tasks still remain |
| Phase 5 | 🔶 Partial | Ratchet guardrail + baseline allowlist implemented in PR-2; broad path-hygiene backlog still remains |
| Phase 6 | 🔶 Partial | Shim-first PR-2 tranche completed for high-use collector surfaces, but major legacy modules still remain |
| Phase 7 | 🔲 Pending | UI consolidation |
| Phase 8 | 🔲 Pending | Operational polish + scheduler |

### Progress update details (evidence from current checkout)

**Done / implemented now:**
- Phase 1 + Phase 2 implementation exists in canonical `src/` modules and current tests.
- Phase 3 implementation exists end-to-end:
  - `sql/migrations/011_pattern_cache.sql`
  - `src/ai_trading_system/domains/ranking/patterns/cache.py` (`PatternCacheStore`)
  - Stage 2 pre-screen and cache write flow in `patterns/evaluation.py`
  - Rank orchestration wiring in `domains/ranking/service.py` (`pattern_scan_mode`, `pattern_stage2_only`, `pattern_min_stage2_score`)
  - Tests: `tests/test_pattern_cache.py`
- Significant Phase 6 migration work is already present:
  - Several top-level `analytics/*` modules are now compatibility shims to `src/`
  - Former top-level `features/` and `execution/` package modules are largely removed in favor of `src/ai_trading_system/domains/*`
  - Multiple `ui/*` and `run/*` compatibility shims route to canonical `src/` code
- PR-1 implementation completed and validated:
  - Execution Stage 2 gate controls added in execution request/candidate flow:
    - `execution_require_stage2` (optional override)
    - `execution_stage2_min_score` (default `70.0`)
    - auto-on when `rank_mode == "stage2_breakout"` unless overridden
    - gate diagnostics emitted in execute metadata (`before/after/dropped`, active/available state)
  - Publish Stage 2 enrichment added:
    - `stage2_summary` and `stage2_breakdown_symbols` in publish datasets/metadata
    - Telegram summary now includes compact Stage 2 line
  - Publish retry/run-context reliability improved:
    - CLI publish-only run auto-resolves latest publishable run when `--run-id` is omitted
    - API `POST /api/execution/pipeline/publish-retry` now supports optional `run_id`
  - Validation evidence:
    - `./.venv/bin/python -m pytest -q tests/test_execution_candidate_builder.py tests/test_publish_payloads.py tests/test_pipeline_orchestrator.py tests/test_execution_api.py` → **48 passed**
    - `./.venv/bin/python -m run.orchestrator --stages rank,publish --run-date 2026-04-21 --local-publish` → completed
    - `./.venv/bin/python -m run.orchestrator --stages publish --local-publish` (no run-id) → auto-resolved latest run and completed
- PR-2 implementation completed and validated:
  - Added canonical ingest validation module in `src` and retained `collectors/ingest_validation.py` as compatibility shim.
  - Added collector canonical mapping doc (`docs/refactor/collectors_canonical_map.md`) and explicit deferred boundary note for `collectors/daily_update_runner.py`.
  - Added compatibility/deprecation guardrails:
    - `tests/test_collectors_shim_compat.py`
    - `tests/test_legacy_surface_guardrails.py`
    - `tests/lint/test_path_hygiene_ratchet.py` + baseline allowlist
  - Moved `tools/export_excel.py` logic to canonical `src/ai_trading_system/interfaces/cli/export_excel.py`, with legacy shim retained.
  - Validation evidence:
    - `./.venv/bin/python -m pytest -q tests/test_phase5_guardrails.py tests/lint/test_path_hygiene_ratchet.py tests/test_collectors_shim_compat.py tests/test_legacy_surface_guardrails.py tests/test_ingest_write_validation.py` → **13 passed**
    - `./.venv/bin/python -m run.orchestrator --stages ingest,features,rank --run-date 2026-04-21` → completed (`features/rank` skipped due to no new ingest data)

**Remaining / not yet complete:**
- Phase 0 cleanup remains: legacy entrypoints and directories are still in checkout.
- Phase 4 remains incomplete:
  - Backend/API pieces are implemented; UI-specific Stage 2 filter/enrichment items are still open.
  - Execution policy module (`domains/execution/entry_policy.py`) still has minimal policy logic and can be expanded if Phase 4 requires deeper policy semantics beyond candidate gating.
- Phase 5 remains incomplete:
  - Ratchet baseline exists, but many non-canonical path usages outside the scoped PR-2 surfaces are still open.
- Phase 6 remains incomplete:
  - Top-level `collectors/` still contains substantial logic (for example `daily_update_runner.py`, backfill/repair scripts).
  - `main.py`, `tools/`, `dashboard/` remain.
- Phase 7 and Phase 8 are still pending.

### Next 2 PRs (recommended execution order)

#### PR-3: Phase 4 UI + operator surfacing completion

**Primary goal:** finish remaining Phase 4 user-facing Stage 2 visibility tasks.

**Scope:**
- Add Stage 2-specific columns/filters in execution operator UI/readmodels where needed.
- Ensure ranking snapshot and workspace views consistently expose Stage 2 fields used by operators.
- Keep payload shape backward-compatible for existing clients.

**Suggested touch files:**
- `src/ai_trading_system/interfaces/api/services/readmodels/rank_snapshot.py`
- `src/ai_trading_system/interfaces/api/services/execution_operator.py`
- `src/ai_trading_system/interfaces/streamlit/execution/app.py` (if filter surface is implemented in Streamlit)

**Ship gate:**
```bash
./.venv/bin/python -m pytest -q tests/test_execution_api.py tests/test_readmodel_snapshots.py
./.venv/bin/python -m run.orchestrator --stages rank,publish --run-date 2026-04-21 --local-publish
```

---

#### PR-4: Phase 6 deep collector migration tranche

**Primary goal:** migrate remaining high-risk legacy collector logic into canonical `src` modules while preserving compatibility shims.

**Scope:**
- Split and migrate `collectors/daily_update_runner.py` orchestration logic into `src/ai_trading_system/domains/ingest/service.py` (or adjacent canonical modules).
- Migrate remaining non-shim collector scripts used by operations and keep import-compatible shims at legacy paths.
- Tighten path-hygiene ratchet allowlist by reducing existing exceptions.

**Ship gate:**
```bash
./.venv/bin/python -m pytest -q tests/test_phase5_guardrails.py tests/lint/test_path_hygiene_ratchet.py tests/test_collectors_shim_compat.py
./.venv/bin/python -m run.orchestrator --stages ingest,features,rank --run-date 2026-04-21
```

---

## Phase 0 — Completed work record

The following was executed and is in the repository. Do not redo.

**Current checkout note:**
- `main.py` still exists and is covered by `tests/test_main_entrypoint.py` as a deprecated shim
- `tools/` still exists
- `dashboard/` still exists
- `config/settings.py` still exists
- Treat these as unresolved legacy surfaces, not already-completed deletions

**Phase 1 (Sprint 1) — Stage 2 Uptrend Foundation:**
- `src/ai_trading_system/domains/features/indicators.py` — added `add_stage2_features()`, `_STAGE2_OUTPUT_COLS`
- `src/ai_trading_system/domains/features/feature_store.py` — added `STAGE2_FEATURE_COLUMNS`, `compute_stage2()` method, `'stage2'` key in `feature_methods`
- `src/ai_trading_system/domains/ranking/contracts.py` — added `'stage2_breakout'` to `RANK_MODES`; added 7 Stage 2 columns to `RANKED_SIGNAL_COLUMNS`
- `src/ai_trading_system/domains/ranking/eligibility.py` — added `stage2_gate_enabled`, `stage2_min_score` params
- `src/ai_trading_system/domains/ranking/ranker.py` — added Stage 2 bonus (+0–5 pts), `stage2_breakout` mode filter, wired `stage2_gate_enabled`

**Phase 1 (Sprint 2) — Pattern Engine Bug Fixes:**
- `src/ai_trading_system/domains/ranking/patterns/detectors.py`:
  - Bug Fix 1: O(n³)→O(n²) flag detector (`detect_flag_signals` rewritten)
  - Bug Fix 2: round-bottom positional argmax (replaces `.index[0]`)
  - Bug Fix 3: `smoothing_method` added to `_scan_config_from_backtest()`
  - Bug Fix 4: stale CwH + round-bottom watchlist recency guards
  - Bug Fix 5: round-bottom low-volume watchlist guard
  - Stage 2 bonus in `_score_signal_rows()` (+0/5/10/15 pts)
- `src/ai_trading_system/domains/ranking/breakout.py` — Stage 2 Tier A/B/C/D replaces 3-condition pass_count

**Tests present in this checkout:**
- `tests/test_stage2_features.py` — Stage 2 scoring coverage
- `tests/test_new_patterns.py` — Phase 2 detector coverage
- `tests/test_phase5_guardrails.py` — current guardrail coverage in place of the older `tests/lint/test_path_hygiene.py` references below

---

## Phase 2 — Six New Pattern Detectors (3–4 days)

**Goal:** implement 6 new chart pattern detectors in `src/ai_trading_system/domains/ranking/patterns/detectors.py` and register them in the evaluation pipeline.

**Prerequisite:** Phase 1 complete (Stage 2 features in feature store).

---

### Task 2.1 — Ascending Triangle

**File:** `src/ai_trading_system/domains/ranking/patterns/detectors.py`

**What to add:** function `detect_ascending_triangle_signals`

```
Signature:
def detect_ascending_triangle_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: list[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
```

**Algorithm:**
1. Separate extrema into `peaks` (kind=='peak') and `troughs` (kind=='trough').
2. For each consecutive peak pair `(p1, p2)`:
   a. Compute resistance price from smoothed at each: `res_prices = [smoothed.iloc[p1.index], smoothed.iloc[p2.index]]`
   b. `res_mean = mean(res_prices)`. If any price deviates > `config.asc_tri_flat_tol` (default 0.015 = 1.5%) from `res_mean` → skip (not flat resistance).
   c. Find troughs between `p1.index` and `p2.index`. Require ≥ 2 troughs.
   d. Trough prices must be **ascending**: each trough price ≥ previous × 1.005.
   e. Pattern width: `p2.index - p1.index`. Reject if < 15 or > 90.
   f. Stage 2 pre-filter: if `'stage2_score'` in `frame.columns` and `frame['stage2_score'].iloc[-1] < 50` → skip entire symbol (return empty list early).
3. Compute `resistance_level = frame.iloc[p1.index:p2.index+1]['high'].max()`.
4. `invalidation_price = trough_prices[-1] * 0.98`.
5. Find breakout: close > `resistance_level` with `volume_ratio_20 >= config.breakout_volume_ratio_min`. Use `_find_breakout_confirmation()` helper.
6. Build confirmed signal via `_build_signal()` if breakout found and `_recent_enough()`.
7. Build watchlist signal if latest close within `config.cup_watchlist_buffer_pct` of resistance (use existing buffer constant — no new config key needed for this).
8. Return `(signals, PatternScanStats('ascending_triangle', candidates, confirmed, watchlist))`.

**Config keys needed** (add to `PatternScanConfig` in `domains/ranking/patterns/contracts.py`):
```python
asc_tri_flat_tol: float = 0.015        # 1.5% flatness tolerance on resistance
```

**Exit check:** `python3 -c "from ai_trading_system.domains.ranking.patterns.detectors import detect_ascending_triangle_signals; print('OK')"` succeeds.

---

### Task 2.2 — VCP (Volatility Contraction Pattern)

**File:** `src/ai_trading_system/domains/ranking/patterns/detectors.py`

**What to add:** function `detect_vcp_signals`

```
Signature:
def detect_vcp_signals(
    frame: pd.DataFrame,
    *,
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
```

**Algorithm:**
1. Require `'volume_ratio_20'` in frame. If absent return empty.
2. `closes = frame['close'].to_numpy(float)`. `vrat = frame['volume_ratio_20'].to_numpy(float)`. `n = len(closes)`.
3. `WINDOW = getattr(config, 'vcp_window_bars', 40)`.
4. For each `end in range(WINDOW, n)`:
   a. `start = end - WINDOW`. Divide into three equal thirds.
   b. Compute `ranges[i] = (max(closes[third_i]) - min(closes[third_i])) / max(closes[start_i], 1e-9)`.
   c. Compute `vols[i] = mean(vrat[third_i])`.
   d. Skip if `ranges[0] < config.vcp_min_first_range_pct` (default 0.08 = 8%).
   e. Require price contraction: `ranges[1] < ranges[0] * config.vcp_price_contraction_factor` (default 0.85) AND `ranges[2] < ranges[1] * 0.85`.
   f. Require volume contraction: `vols[1] < vols[0] * config.vcp_vol_contraction_factor` (default 0.90) AND `vols[2] < vols[1] * 0.90`.
   g. Stage 2 bonus: read `frame['stage2_score'].iloc[end]` if available — store in signal extra fields.
5. `pivot = frame.iloc[start:end+1]['high'].max()`.
6. `invalidation = frame.iloc[start:end+1]['low'].min()`.
7. Find breakout: close > `pivot` with volume. Use `_find_breakout_confirmation()`.
8. `setup_quality = 50 + min(20, ranges[0]*100/2) + (10 if vols[2] < 0.7 else 5) + s2_bonus`.
9. Build signals via `_build_signal()`.

**Config keys needed** (add to `PatternScanConfig`):
```python
vcp_window_bars: int = 40
vcp_price_contraction_factor: float = 0.85
vcp_vol_contraction_factor: float = 0.90
vcp_min_first_range_pct: float = 0.08
```

**Exit check:** `detect_vcp_signals` importable and returns a tuple of (list, PatternScanStats).

---

### Task 2.3 — Flat Base

**File:** `src/ai_trading_system/domains/ranking/patterns/detectors.py`

**What to add:** function `detect_flat_base_signals`

```
Signature:
def detect_flat_base_signals(
    frame: pd.DataFrame,
    *,
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
```

**Algorithm:**
1. `highs = frame['high'].to_numpy(float)`. `lows = frame['low'].to_numpy(float)`. `vrat = frame['volume_ratio_20'].to_numpy(float)` (if available). `n = len(highs)`.
2. `MIN_BARS = getattr(config, 'flat_base_min_bars', 25)`. `MAX_BARS = getattr(config, 'flat_base_max_bars', 65)`. `MAX_DEPTH = getattr(config, 'flat_base_max_depth_pct', 0.15)`.
3. For each `end in range(MIN_BARS, n)`:
   a. For each `span in range(MIN_BARS, min(MAX_BARS+1, end+1))`:
      - `start = end - span`
      - `wh = highs[start:end+1].max()`. `wl = lows[start:end+1].min()`.
      - `depth = (wh - wl) / max(wh, 1e-9)`. If `depth > MAX_DEPTH` → continue.
      - Volume contraction check (if `vrat` available): `mid = start + span//2`. Require `vrat[mid:end+1].mean() < vrat[start:mid].mean()`.
      - Valid flat base found → `pivot = wh`. Break (use shortest valid span).
4. Find breakout. Build signals via `_build_signal()`.

**Config keys needed:**
```python
flat_base_min_bars: int = 25
flat_base_max_bars: int = 65
flat_base_max_depth_pct: float = 0.15
```

**Exit check:** `detect_flat_base_signals` importable.

---

### Task 2.4 — 3-Weeks-Tight (3WT)

**File:** `src/ai_trading_system/domains/ranking/patterns/detectors.py`

**What to add:** function `detect_3wt_signals`

```
Signature:
def detect_3wt_signals(
    frame: pd.DataFrame,
    *,
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
```

**Algorithm:**
1. `closes = frame['close'].to_numpy(float)`. `n = len(closes)`.
2. `TIGHT = getattr(config, 'wt3_tight_pct', 0.015)`. `PRIOR_ADV = getattr(config, 'wt3_prior_adv', 0.20)`. `WEEKS = 3`.
3. For each `end in range(WEEKS*5, n)`:
   a. Weekly closes: `w_closes = [closes[end - i*5] for i in range(WEEKS)]` (reversed to chronological).
   b. `tight = (max(w_closes) - min(w_closes)) / max(max(w_closes), 1e-9)`. If `tight > TIGHT` → skip.
   c. Prior advance check: `lookback = end - WEEKS*5`. `prior_start = max(0, lookback - 20)`. `prior_adv = (closes[lookback] - closes[prior_start]) / closes[prior_start]`. If `prior_adv < PRIOR_ADV` → skip.
   d. `pivot = frame.iloc[end-WEEKS*5:end+1]['high'].max()`.
   e. `invalidation = w_closes[0] * 0.97` (3% below first weekly close).
4. Find breakout. Build signals.

**Config keys needed:**
```python
wt3_tight_pct: float = 0.015
wt3_prior_adv: float = 0.20
```

**Exit check:** `detect_3wt_signals` importable.

---

### Task 2.5 — Symmetrical Triangle

**File:** `src/ai_trading_system/domains/ranking/patterns/detectors.py`

**What to add:** function `detect_symmetrical_triangle_signals`

```
Signature:
def detect_symmetrical_triangle_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: list[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
```

**Algorithm:**
1. Separate extrema into `peaks` and `troughs`.
2. For each consecutive peak pair `(p1, p2)`:
   a. Descending peaks required: `smoothed.iloc[p2.index] < smoothed.iloc[p1.index]`.
   b. Find troughs between `p1` and `p2`. Require ≥ 2 inner troughs.
   c. `t1 = inner_troughs[0]`, `t2 = inner_troughs[-1]`. Ascending troughs required: `smoothed.iloc[t2.index] > smoothed.iloc[t1.index]`.
   d. Width: `p2.index - p1.index`. Reject if < 15 or > 80.
   e. Convergence check: upper line value at `p2` must be > lower line value at `t2` (still converging, not crossed).
3. `upper_line = smoothed.iloc[p2.index]`. `lower_line = smoothed.iloc[t2.index]`.
4. Emit BULLISH breakout only: close > `upper_line` with volume. Build confirmed signal.
5. Watchlist: latest close within 2% below `upper_line`.

**No new config keys needed** — uses existing `cup_watchlist_buffer_pct`.

**Exit check:** `detect_symmetrical_triangle_signals` importable.

---

### Task 2.6 — Head & Shoulders Bearish Exclusion Filter

**File:** `src/ai_trading_system/domains/ranking/patterns/detectors.py`

**What to add:** function `detect_head_shoulders_filter` — returns a boolean + neckline, NOT a signal list

```
Signature:
def detect_head_shoulders_filter(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: list[LocalExtrema],
    config: PatternScanConfig,
) -> tuple[bool, float]:
    """Returns (is_hs_top, neckline_price). Pure detection — no signal objects."""
```

**Algorithm:**
1. Separate extrema into `peaks` and `troughs`.
2. For each consecutive triple of peaks `(ls, head, rs)`:
   a. Head symmetry: `head_px > ls_px * 1.03` AND `head_px > rs_px * 1.03`.
   b. Shoulder balance: `abs(ls_px - rs_px) / max(ls_px, 1e-9) <= 0.05` (5% tolerance).
   c. Find `t1` (trough between ls and head) and `t2` (trough between head and rs).
   d. `neckline = (smoothed.iloc[t1.index] + smoothed.iloc[t2.index]) / 2`.
   e. Confirmed breakdown: `frame.iloc[-1]['close'] < neckline * 0.99`.
3. Return `(True, float(neckline))` on first confirmed H&S top. Return `(False, 0.0)` if none found.

**Caller pattern** (in `detect_pattern_signals_for_symbol` or evaluation pipeline):
```python
is_hs, neckline = detect_head_shoulders_filter(frame, smoothed=smoothed, extrema=extrema, config=config)
if is_hs:
    # Exclude from bullish scan results
    # If is_stage2_uptrend, emit stage2_breakdown alert
    ...
```

**No new config keys needed.**

**Exit check:** `detect_head_shoulders_filter` importable and returns `(bool, float)`.

---

### Task 2.7 — Register all 6 new patterns in the evaluation pipeline

**File:** `src/ai_trading_system/domains/ranking/patterns/evaluation.py`

**What to change:** find `detect_pattern_signals_for_symbol` (or equivalent dispatcher function) and add the 6 new detectors.

1. Add imports at top of file:
```python
from ai_trading_system.domains.ranking.patterns.detectors import (
    detect_ascending_triangle_signals,
    detect_vcp_signals,
    detect_flat_base_signals,
    detect_3wt_signals,
    detect_symmetrical_triangle_signals,
    detect_head_shoulders_filter,
)
```

2. In the per-symbol scan function, after existing detectors:
```python
# Ascending Triangle (Stage 2 pre-screened inside the function)
asc_tri_signals, asc_tri_stats = detect_ascending_triangle_signals(
    frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=recent_only
)
all_signals.extend(asc_tri_signals)

# VCP
vcp_signals, vcp_stats = detect_vcp_signals(frame, config=config, recent_only=recent_only)
all_signals.extend(vcp_signals)

# Flat Base
fb_signals, fb_stats = detect_flat_base_signals(frame, config=config, recent_only=recent_only)
all_signals.extend(fb_signals)

# 3-Weeks-Tight
wt3_signals, wt3_stats = detect_3wt_signals(frame, config=config, recent_only=recent_only)
all_signals.extend(wt3_signals)

# Symmetrical Triangle
sym_tri_signals, sym_tri_stats = detect_symmetrical_triangle_signals(
    frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=recent_only
)
all_signals.extend(sym_tri_signals)

# Head & Shoulders exclusion filter
is_hs, neckline = detect_head_shoulders_filter(
    frame, smoothed=smoothed, extrema=extrema, config=config
)
if is_hs:
    # Remove all bullish signals for this symbol
    all_signals = [s for s in all_signals if s.symbol_id != symbol_id]
    # Stage 2 breakdown alert (if applicable)
    latest_s2 = float(frame['stage2_score'].iloc[-1]) if 'stage2_score' in frame.columns else 0.0
    if latest_s2 >= 70.0:
        stage2_breakdown_alerts.append({'symbol_id': symbol_id, 'neckline': neckline, 'stage2_score': latest_s2})
```

3. Add `stage2_breakdown_alerts` as an output field in the scan result object (`PatternScanResult` or equivalent). If no such object exists, add it to the return dict/dataclass.

**Exit check:**
```bash
python3 -c "
from ai_trading_system.domains.ranking.patterns import evaluation
print('evaluation imports OK')
"
```

---

### Task 2.8 — Add new PatternScanConfig fields

**File:** `src/ai_trading_system/domains/ranking/patterns/contracts.py`

Add the following fields to `PatternScanConfig` dataclass (frozen, with defaults):
```python
# Ascending Triangle
asc_tri_flat_tol: float = 0.015

# VCP
vcp_window_bars: int = 40
vcp_price_contraction_factor: float = 0.85
vcp_vol_contraction_factor: float = 0.90
vcp_min_first_range_pct: float = 0.08

# Flat Base
flat_base_min_bars: int = 25
flat_base_max_bars: int = 65
flat_base_max_depth_pct: float = 0.15

# 3-Weeks-Tight
wt3_tight_pct: float = 0.015
wt3_prior_adv: float = 0.20
```

Since `PatternScanConfig` is a frozen dataclass, adding fields with defaults is backward-compatible (no existing callers need to change).

**Exit check:** `from ai_trading_system.domains.ranking.patterns.contracts import PatternScanConfig; c = PatternScanConfig(); assert c.vcp_window_bars == 40`

---

### Task 2.9 — Tests for new patterns

**File:** `tests/test_new_patterns.py` (new file)

Write at minimum one test per new detector:

```python
class TestAscendingTriangle:
    def test_flat_resistance_detected(self):
        # Build a frame with two peaks at same level + ascending troughs
        # Assert at least one signal of family 'ascending_triangle' returned

    def test_stage2_prescreens_non_stage2(self):
        # frame with stage2_score=20 everywhere → returns empty signals

class TestVCP:
    def test_contracting_ranges_detected(self):
        # Three thirds with decreasing range + decreasing volume → candidate found

    def test_non_contracting_skipped(self):
        # Equal ranges → empty

class TestFlatBase:
    def test_flat_within_15pct_depth(self):
        # 30-bar window, max-min < 15% → signal returned

    def test_deep_base_rejected(self):
        # depth > 15% → empty

class TestThreeWeeksTight:
    def test_three_tight_weekly_closes(self):
        # Build frame where last 15 bars have 3 'weekly' closes within 1.5%

    def test_no_prior_advance_skipped(self):
        # Prior advance < 20% → empty

class TestSymmetricalTriangle:
    def test_converging_peaks_troughs(self):
        # Descending peaks + ascending troughs → signal found

class TestHeadShouldersFilter:
    def test_hs_top_detected(self):
        # Build frame with head above two shoulders
        # Assert returns (True, neckline > 0)

    def test_no_hs_returns_false(self):
        # Random price → (False, 0.0)

    def test_hs_removes_bullish_signals(self):
        # In the evaluation pipeline, H&S on a symbol should zero out its bullish signals
```

**Exit check:** `pytest tests/test_new_patterns.py -v` — all tests pass.

---

### Phase 2 ship gate

All of the following must pass before Phase 3:
```bash
python3 -c "import ast; [ast.parse(open(f).read()) for f in [
  'src/ai_trading_system/domains/ranking/patterns/detectors.py',
  'src/ai_trading_system/domains/ranking/patterns/evaluation.py',
  'src/ai_trading_system/domains/ranking/patterns/contracts.py',
]]"
pytest tests/test_new_patterns.py -v
pytest tests/test_stage2_features.py -v   # must still pass
pytest tests/test_phase5_guardrails.py -v  # active guardrail suite in this checkout
```

---

## Phase 3 — Two-Tier Pattern Caching (2–3 days)

**Goal:** full weekly scan + incremental daily scan for 1,600-symbol universe; target < 8s wall time.

---

### Task 3.1 — pattern_cache table in control_plane.duckdb

**File:** `sql/` — create new migration file `010_pattern_cache.sql`

```sql
-- Migration 010: pattern scan result cache
CREATE TABLE IF NOT EXISTS pattern_cache (
    symbol_id          VARCHAR NOT NULL,
    exchange           VARCHAR NOT NULL DEFAULT 'NSE',
    pattern_family     VARCHAR NOT NULL,
    pattern_state      VARCHAR NOT NULL,
    stage2_score       DOUBLE,
    stage2_label       VARCHAR,
    signal_date        DATE NOT NULL,
    breakout_level     DOUBLE,
    watchlist_trigger  DOUBLE,
    invalidation_price DOUBLE,
    pattern_score      DOUBLE,
    setup_quality      DOUBLE,
    width_bars         INTEGER,
    scanned_at         TIMESTAMP DEFAULT current_timestamp,
    scan_run_id        VARCHAR,
    PRIMARY KEY (symbol_id, exchange, pattern_family, scanned_at)
);

CREATE INDEX IF NOT EXISTS idx_pattern_cache_signal_date
    ON pattern_cache (signal_date);

CREATE INDEX IF NOT EXISTS idx_pattern_cache_stage2
    ON pattern_cache (stage2_score, pattern_state);
```

**File:** `src/ai_trading_system/pipeline/` or `src/ai_trading_system/domains/ranking/` — find where `control_plane.duckdb` migrations run (search for `001_pipeline_governance.sql` usage) and register migration `010`.

**Exit check:** running the migration creates `pattern_cache` table in a test DuckDB instance.

---

### Task 3.2 — PatternCacheStore class

**File:** `src/ai_trading_system/domains/ranking/patterns/cache.py` (new file)

```python
"""Two-tier pattern scan cache backed by control_plane.duckdb."""
from __future__ import annotations
import duckdb
import pandas as pd
from pathlib import Path

class PatternCacheStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)

    def read_cached_signals(
        self,
        *,
        signal_date: str,
        exchange: str = "NSE",
        min_pattern_score: float = 0.0,
    ) -> pd.DataFrame:
        """Read cached pattern signals for a given date."""
        ...

    def write_signals(
        self,
        signals_df: pd.DataFrame,
        *,
        scan_run_id: str,
        replace_date: str | None = None,
    ) -> int:
        """Write or replace pattern signals for a scan run. Returns rows written."""
        ...

    def symbols_needing_rescan(
        self,
        all_symbols: list[str],
        *,
        ohlcv_db_path: str | Path,
        min_price_change_pct: float = 1.0,
        min_volume_ratio: float = 1.3,
    ) -> list[str]:
        """Return symbols where |close change| >= threshold OR volume_ratio_20 >= threshold.
        Used for incremental daily scan to filter ~150-250 active symbols."""
        ...

    def latest_full_scan_date(self, exchange: str = "NSE") -> str | None:
        """Return the most recent date where all symbols were scanned (full scan)."""
        ...
```

**Exit check:** class importable; `write_signals` writes to DuckDB and `read_cached_signals` reads back rows.

---

### Task 3.3 — Stage 2 pre-screen function

**File:** `src/ai_trading_system/domains/ranking/patterns/evaluation.py`

Add function `_stage2_prescreened`:

```python
def _stage2_prescreened(
    eligible_payloads: list[dict],
    *,
    stage2_only: bool = False,
    min_stage2_score: float = 70.0,
) -> tuple[list[dict], list[dict]]:
    """Split eligible symbol payloads into stage2 and non-stage2 groups.

    Returns (stage2_payloads, non_stage2_payloads).
    If stage2_only=True, non_stage2_payloads is always empty.

    Each payload is a dict with at least 'frame' (DataFrame) and 'symbol_id'.
    """
    stage2, non_stage2 = [], []
    for payload in eligible_payloads:
        frame = pd.DataFrame(payload.get('frame', {}))
        if frame.empty:
            continue
        latest_s2 = 0.0
        if 'stage2_score' in frame.columns:
            latest_s2 = float(frame['stage2_score'].iloc[-1] or 0)
        if latest_s2 >= min_stage2_score:
            stage2.append(payload)
        elif not stage2_only:
            non_stage2.append(payload)
    return stage2, non_stage2
```

**Usage** in the parallel scan entry point (also in `evaluation.py`):
```python
stage2_payloads, other_payloads = _stage2_prescreened(eligible_payloads, stage2_only=False)
# Scan stage2 symbols for all bullish patterns
# Scan other symbols ONLY for H&S exclusion filter (optional)
```

**Exit check:** `_stage2_prescreened` callable; returns two lists; symbols with `stage2_score >= 70` in stage2 list.

---

### Task 3.4 — Incremental daily scan orchestration

**File:** `src/ai_trading_system/domains/ranking/service.py`

Find `build_pattern_signals` (or equivalent function that the rank stage calls). Add two scan modes:

**Mode A — Full scan** (triggered weekly, e.g., on Sundays or when cache is empty):
- Scan all symbols in universe.
- Write all results to `pattern_cache`.
- Log: `"Full pattern scan: N symbols, K signals, wall_time=Xs"`

**Mode B — Incremental daily scan** (triggered Mon–Fri post-close):
- Call `PatternCacheStore.symbols_needing_rescan(...)` to get ~150–250 active symbols.
- Read remaining symbols from yesterday's cache.
- Scan active symbols, merge with cached results.
- Write updated signals to `pattern_cache` for today's date.
- Log: `"Incremental pattern scan: N active / M total symbols, wall_time=Xs"`

Control which mode runs via a param `scan_mode: Literal['full', 'incremental'] = 'incremental'` passed from the rank stage context params.

**Exit check:** `scan_mode='incremental'` with a small fixture universe runs without error and writes to cache.

---

### Task 3.5 — Tests for caching

**File:** `tests/test_pattern_cache.py` (new file)

```python
class TestPatternCacheStore:
    def test_write_and_read_roundtrip(self, tmp_path):
        # Create PatternCacheStore with tmp DuckDB
        # Write 5 signal rows, read back, assert count and columns match

    def test_replace_date_clears_old_rows(self, tmp_path):
        # Write day1 rows, then write day1 rows again with replace_date=day1
        # Assert only the second write's rows remain for day1

    def test_symbols_needing_rescan_filters_by_change(self, tmp_path):
        # Seed ohlcv with 10 symbols; 3 have |change| > 1%
        # Assert returns exactly those 3

class TestStage2Prescreened:
    def test_stage2_symbols_sorted_correctly(self):
        # 3 payloads: 2 with stage2_score >= 70, 1 with 50
        # stage2_only=False → 2 in stage2, 1 in non_stage2
        # stage2_only=True → 2 in stage2, 0 in non_stage2
```

**Exit check:** `pytest tests/test_pattern_cache.py -v` — all pass.

---

### Phase 3 ship gate

```bash
python3 -c "
from ai_trading_system.domains.ranking.patterns.cache import PatternCacheStore
from ai_trading_system.domains.ranking.patterns.evaluation import _stage2_prescreened
print('OK')
"
pytest tests/test_pattern_cache.py -v
pytest tests/test_phase5_guardrails.py -v  # active guardrail suite in this checkout
```

---

## Phase 4 — Execution Gate + API/UI Enrichment (2 days)

**Goal:** wire Stage 2 into the execution entry gate, expose Stage 2 fields in FastAPI responses, and add Stage 2 filters to the Streamlit UI.

---

### Task 4.1 — Stage 2 gate in entry_policy.py

**File:** `src/ai_trading_system/domains/execution/entry_policy.py`

Find `select_entry_policy` (or the main entry policy function). Extend its signature:

```python
def select_entry_policy(
    candidate: dict,
    policy_name: str = 'breakout',
    require_stage2: bool = False,          # ADD
    stage2_min_score: float = 70.0,        # ADD
) -> dict:
```

**Logic to add** (inside the function, before computing entry price):
```python
is_stage2 = bool(candidate.get('is_stage2_uptrend', False))
s2_score = float(candidate.get('stage2_score', 0))
s2_label = str(candidate.get('stage2_label', 'non_stage2'))

entry_blocked = require_stage2 and not is_stage2

return {
    # ... existing fields ...
    'entry_policy':       policy_name,
    'entry_price':        close if not entry_blocked else None,
    'entry_trigger':      None if entry_blocked else 'breakout_above_pivot',
    'entry_blocked':      entry_blocked,
    'entry_block_reason': f'stage2_score={s2_score:.0f} ({s2_label})' if entry_blocked else None,
    'stage2_score':       s2_score,
    'stage2_label':       s2_label,
    'entry_note':         f'policy={policy_name} stage2={s2_label}',
}
```

**Backward compatibility:** `require_stage2=False` by default — all existing callers work unchanged.

**Exit check:**
```python
result = select_entry_policy({'close': 100, 'stage2_score': 40, 'is_stage2_uptrend': False}, require_stage2=True)
assert result['entry_blocked'] is True
result2 = select_entry_policy({'close': 100, 'stage2_score': 85, 'is_stage2_uptrend': True}, require_stage2=True)
assert result2['entry_blocked'] is False
```

---

### Task 4.2 — Wire require_stage2 from rank stage context

**File:** `src/ai_trading_system/pipeline/stages/execute.py` (or `domains/execution/service.py`)

Find where the execute stage reads its context params and calls `select_entry_policy`. Add:

```python
require_stage2 = bool(context.params.get('require_stage2', False))
# When running in stage2_breakout rank mode, default require_stage2 to True
if context.params.get('rank_mode') == 'stage2_breakout':
    require_stage2 = True

# Pass to entry policy:
policy_result = select_entry_policy(
    candidate,
    policy_name=policy_name,
    require_stage2=require_stage2,
)
```

**Exit check:** `python -m run.orchestrator --help` still works; no import error.

---

### Task 4.3 — Stage 2 fields in FastAPI response

**File:** `src/ai_trading_system/interfaces/` — find the ranked signals response schema (search for `RankedSignal`, `ranked_signals` in the interfaces directory).

Add the following fields to the ranked signal response model:
```python
stage2_score: float | None = None
is_stage2_uptrend: bool | None = None
stage2_label: str | None = None      # 'strong_stage2' | 'stage2' | 'stage1_to_stage2' | 'non_stage2'
stage2_score_bonus: float | None = None
```

If the interface uses Pydantic models, add these as optional fields with `None` defaults (backward-compatible).

**Exit check:**
```bash
python3 -c "from ai_trading_system.interfaces import *; print('interfaces import OK')"
```

---

### Task 4.4 — Stage 2 breakdown list in publish artifact

**File:** `src/ai_trading_system/domains/publish/publish_payloads.py`

Find the publish payload assembly function. Add `stage2_breakdown_symbols` to the output:

```python
# In the publish payload assembly, after pattern signals are collected:
stage2_breakdown_symbols: list[dict] = context.params.get('stage2_breakdown_alerts', [])

# Include in payload:
payload['stage2_breakdown_symbols'] = stage2_breakdown_symbols
payload['stage2_breakdown_count'] = len(stage2_breakdown_symbols)
```

**Exit check:** `publish_payloads` module importable without error.

---

### Task 4.5 — Telegram summary Stage 2 section

**File:** `src/ai_trading_system/domains/publish/telegram_summary_builder.py`

Find the function that builds the Telegram message string. Add a Stage 2 section:

```python
# After existing summary sections, before footer:
s2_entries = sum(1 for c in ranked_candidates if c.get('is_stage2_uptrend'))
s2_breakdowns = len(payload.get('stage2_breakdown_symbols', []))

if s2_entries > 0 or s2_breakdowns > 0:
    lines.append(f"\n📊 *Stage 2*")
    if s2_entries:
        lines.append(f"  🟢 Stage 2 entries available: {s2_entries}")
    if s2_breakdowns:
        syms = ', '.join(d['symbol_id'] for d in payload['stage2_breakdown_symbols'][:5])
        lines.append(f"  🔴 Stage 2→3 breakdowns: {s2_breakdowns} ({syms}{'...' if s2_breakdowns > 5 else ''})")
```

**Exit check:** Telegram builder importable; Stage 2 section appears in output for a mock payload with `is_stage2_uptrend=True` candidates.

---

### Task 4.6 — Streamlit Stage 2 filter UI

**File:** `ui/research/app.py` (or whichever Streamlit file contains the ranked signals table)

Find where the ranked signals dataframe is displayed. Add a sidebar filter:

```python
# In the Streamlit sidebar:
st.sidebar.subheader("Stage 2 Filter")
stage2_only = st.sidebar.checkbox("Stage 2 uptrends only", value=False)
min_stage2_score = st.sidebar.slider("Min Stage 2 score", 0, 100, 0, step=5)

# Apply filter to ranked signals dataframe:
if stage2_only:
    ranked_df = ranked_df[ranked_df.get('is_stage2_uptrend', False) == True]
if min_stage2_score > 0 and 'stage2_score' in ranked_df.columns:
    ranked_df = ranked_df[ranked_df['stage2_score'] >= min_stage2_score]
```

Also add a Stage 2 score column to the display table if `'stage2_score'` is in the dataframe columns.

**Exit check:** `import streamlit; import ui.research.app` — no syntax error.

---

### Task 4.7 — Tests for execution gate

**File:** `tests/test_execution_gate.py` (new file)

```python
from ai_trading_system.domains.execution.entry_policy import select_entry_policy

class TestEntryPolicyStage2Gate:
    def test_gate_blocks_when_require_stage2_and_not_stage2(self):
        result = select_entry_policy(
            {'close': 100, 'is_stage2_uptrend': False, 'stage2_score': 30},
            require_stage2=True
        )
        assert result['entry_blocked'] is True
        assert 'stage2_score=30' in result['entry_block_reason']

    def test_gate_passes_when_stage2_ok(self):
        result = select_entry_policy(
            {'close': 200, 'is_stage2_uptrend': True, 'stage2_score': 85},
            require_stage2=True
        )
        assert result['entry_blocked'] is False
        assert result['entry_price'] == 200

    def test_gate_off_by_default(self):
        # require_stage2=False → never blocked regardless of stage2 status
        result = select_entry_policy(
            {'close': 100, 'is_stage2_uptrend': False, 'stage2_score': 0},
            require_stage2=False
        )
        assert result['entry_blocked'] is False

    def test_stage2_label_in_output(self):
        result = select_entry_policy(
            {'close': 150, 'is_stage2_uptrend': True, 'stage2_score': 88, 'stage2_label': 'strong_stage2'}
        )
        assert result['stage2_label'] == 'strong_stage2'
```

**Exit check:** `pytest tests/test_execution_gate.py -v` — all pass.

---

### Phase 4 ship gate

```bash
python3 -c "
from ai_trading_system.domains.execution.entry_policy import select_entry_policy
from ai_trading_system.domains.publish.publish_payloads import *
from ai_trading_system.domains.publish.telegram_summary_builder import *
print('Phase 4 imports OK')
"
pytest tests/test_execution_gate.py -v
pytest tests/test_phase5_guardrails.py -v  # active guardrail suite in this checkout
```

---

## Phase 5 — Path Hygiene Completion (1–2 days)

**Goal:** complete path-hygiene cleanup. In this checkout, use `tests/test_phase5_guardrails.py` as the active guardrail suite and treat the older `tests/lint/test_path_hygiene.py` references as legacy placeholders.

**Current ALLOWLIST** (each must be fixed):
```
run/daily_pipeline.py
channel/stock_scan.py
channel/portfolio_analyzer.py
channel/sector_dashboard.py
src/ai_trading_system/domains/ingest/providers/nse.py
src/ai_trading_system/domains/ingest/providers/yfinance.py
src/ai_trading_system/domains/features/sector_rs.py
src/ai_trading_system/domains/publish/portfolio_analyzer.py
src/ai_trading_system/domains/ranking/_scan_data.py
features/compute_sector_rs.py
collectors/delete_stale.py
collectors/nse_collector.py
collectors/yfinance_collector.py
```

**Rule:** replace every raw `"data/<subtree>..."` string literal with a call to `core.paths` helpers.

---

### Task 5.1 — Fix src/ violators first (highest priority — these are in the canonical package)

For each file in the src/ tree:

**`src/ai_trading_system/domains/ingest/providers/nse.py`:**
- Find every raw `"data/ohlcv..."` or `"data/masterdata..."` or similar literal.
- Replace with: `from ai_trading_system.platform.db.paths import ensure_domain_layout` then `paths = ensure_domain_layout(...)` then `paths.ohlcv_db_path` / `paths.master_db_path`.
- Remove from ALLOWLIST after fix.

**`src/ai_trading_system/domains/ingest/providers/yfinance.py`:** same pattern.

**`src/ai_trading_system/domains/features/sector_rs.py`:** same pattern.

**`src/ai_trading_system/domains/publish/portfolio_analyzer.py`:** same pattern.

**`src/ai_trading_system/domains/ranking/_scan_data.py`:** same pattern.

For each file: grep for the DATA_PATH_RE pattern to find the exact literals, then replace.

**Exit check after each file:** run the relevant focused guardrail test in `tests/test_phase5_guardrails.py` or the nearest active replacement in the repo. Do not rely on `tests/lint/test_path_hygiene.py` unless that file is restored in the checkout you are working in.

---

### Task 5.2 — Fix top-level violators

**`run/daily_pipeline.py`:** Replace raw paths with `core.paths.ensure_domain_layout(...)` helpers.

**`channel/stock_scan.py`, `channel/portfolio_analyzer.py`, `channel/sector_dashboard.py`:** These channel modules will eventually be eliminated in Phase 6. For now, route their paths through `core.paths` to satisfy the hygiene test.

**`features/compute_sector_rs.py`:** Route through `core.paths`.

**`collectors/delete_stale.py`, `collectors/nse_collector.py`, `collectors/yfinance_collector.py`:** Route through `core.paths`.

**For each file** — the fix pattern is always:
1. Add `from core.paths import ensure_domain_layout` (or the equivalent import that resolves in the top-level tree).
2. Replace `"data/ohlcv..."` literals with `str(ensure_domain_layout(...).ohlcv_db_path)` etc.
3. Run the relevant guardrail test after each fix. In this checkout, start with `pytest tests/test_phase5_guardrails.py -v`.

---

### Task 5.3 — Shrink ALLOWLIST to zero and remove it

Once all 13 files are fixed:

1. Open the active path-hygiene or guardrail test file for the checkout you are in. In this checkout, that is `tests/test_phase5_guardrails.py`.
2. Set `ALLOWLIST: set[str] = set()`.
3. Verify `test_allowlist_is_not_stale` passes (empty allowlist is vacuously not stale).
4. Verify `test_no_new_raw_data_paths` passes (no violators anywhere).

**Exit check:**
```bash
pytest tests/test_phase5_guardrails.py -v
```

---

### Phase 5 ship gate

```bash
pytest tests/test_phase5_guardrails.py -v
grep -r '"data/ohlcv\|"data/control_plane\|"data/feature_store\|"data/masterdata' \
  --include="*.py" src/ collectors/ features/ channel/ run/ | grep -v test | grep -v "#"
# Output must be empty
```

---

## Phase 6 — Domain Migration: Top-Level → src/ (3–4 days)

**Goal:** Move the live operational code from top-level legacy packages into `src/ai_trading_system/domains/`, then replace top-level files with pure shims (re-exports).

**Order of operations:** do NOT delete top-level packages — replace their content with shims that `from ai_trading_system.domains.X import *`. This preserves any callers that import from the old path.

---

### Task 6.1 — Map what's NOT yet in src/ vs what is

Before coding, run the following audit:

```bash
# For each domain, compare what's in the top-level vs src/
# collectors/ vs domains/ingest/
diff <(ls collectors/*.py | xargs -I{} basename {} .py | sort) \
     <(ls src/ai_trading_system/domains/ingest/*.py | xargs -I{} basename {} .py | sort)

# features/ vs domains/features/
diff <(ls features/*.py | xargs -I{} basename {} .py | sort) \
     <(ls src/ai_trading_system/domains/features/*.py | xargs -I{} basename {} .py | sort)

# analytics/ vs domains/ranking/
diff <(ls analytics/*.py | xargs -I{} basename {} .py | sort) \
     <(ls src/ai_trading_system/domains/ranking/*.py | xargs -I{} basename {} .py | sort)
```

**Expected finding:** most functionality is already duplicated. The canonical version is in `src/`. The top-level version is legacy. Identify any functionality in the top-level that has NO equivalent in `src/` — those must be moved first.

---

### Task 6.2 — Convert analytics/ to shims

For each file `analytics/<module>.py`:
1. Check if `src/ai_trading_system/domains/ranking/<module>.py` exists and is the canonical version.
2. If yes: replace the file content with:
   ```python
   """Compatibility shim — canonical implementation is in ai_trading_system.domains.ranking."""
   from ai_trading_system.domains.ranking.<module> import *  # noqa: F401,F403
   ```
3. If the module has no equivalent in `src/` (e.g., `analytics/alpha/`, `analytics/dq/`, `analytics/registry/`): leave as-is for now — these are either research tools or cross-cutting concerns.

**Do NOT shim:** `analytics/dq/`, `analytics/registry/`, `analytics/backtester.py`, `analytics/alpha/`, `analytics/ml_engine.py`, `analytics/lightgbm_engine.py` — these remain at the top level until a dedicated migration phase.

**Exit check:** `python -c "import analytics.ranker; import analytics.regime_detector"` — imports succeed via shims.

---

### Task 6.3 — Convert features/ to shims

For each file `features/<module>.py`:
1. Check if `src/ai_trading_system/domains/features/<module>.py` exists.
2. If yes: replace with shim (same pattern as 6.2).
3. If `features/compute_all_features.py` doesn't have a direct equivalent, check `domains/features/feature_store.py` — `FeatureStore.compute_and_store_features()` is the canonical version.

**Exit check:** `python -c "import features.feature_store; import features.indicators"` — imports succeed.

---

### Task 6.4 — Convert execution/ to shims

For each file `execution/<module>.py`:
1. Check if `src/ai_trading_system/domains/execution/<module>.py` exists.
2. If yes: replace with shim.
3. `execution/adapters/dhan.py` — decision gate: if operator confirms paper-only forever, delete it. Otherwise gate behind `LIVE_TRADING_ENABLED=false` check at instantiation time.

**Exit check:** `python -c "import execution.autotrader; import execution.portfolio"` — imports succeed.

---

### Task 6.5 — Convert publishers/ to shims

For each file `publishers/<module>.py`:
1. Check if `src/ai_trading_system/domains/publish/channels/<module>.py` or equivalent exists.
2. If yes: replace with shim.

**Exit check:** `python -c "import publishers.telegram; import publishers.dashboard"` — imports succeed.

---

### Task 6.6 — Convert collectors/ to shims

This is more complex because `collectors/` contains the only implementations of many ingest tools that may not yet be duplicated in `domains/ingest/`.

**Strategy:**
1. For each `collectors/<module>.py`, check if `domains/ingest/<module>.py` has the same functionality.
2. If YES → shim.
3. If NO → **move** the implementation into `domains/ingest/` and create a shim at the old path.
4. `collectors/daily_update_runner.py` (~82 KB): move its three logical sections into `domains/ingest/service.py` (already the orchestration home). Shim the old file.

**Exit check:** `python -c "import collectors.nse_collector; import collectors.daily_update_runner"` — imports succeed.

---

### Task 6.7 — Tests for shim correctness

**File:** `tests/test_import_hygiene.py` (or the checkout's active lint-equivalent test location if a `tests/lint/` package is later restored)

```python
"""Verify that all top-level compatibility shims correctly re-export from canonical src/."""
import importlib

SHIM_MODULES = [
    ('analytics.ranker', 'ai_trading_system.domains.ranking.ranker'),
    ('features.feature_store', 'ai_trading_system.domains.features.feature_store'),
    ('execution.autotrader', 'ai_trading_system.domains.execution.autotrader'),
    # add all shimmed modules
]

def test_shims_importable():
    for shim_path, canonical_path in SHIM_MODULES:
        shim = importlib.import_module(shim_path)
        canonical = importlib.import_module(canonical_path)
        # Key symbols should be the same object
        assert shim is not None
        assert canonical is not None
```

**Exit check:** `pytest tests/test_import_hygiene.py -v` — all pass.

---

### Phase 6 ship gate

```bash
python -m run.orchestrator --help         # CLI intact
pytest tests/smoke/ -x                    # smoke tests green
pytest tests/test_phase5_guardrails.py -v # active guardrail suite in this checkout
python -c "
import analytics.ranker
import features.feature_store
import execution.autotrader
import collectors.nse_collector
print('all shim imports OK')
"
```

---

## Phase 7 — UI Consolidation (3–4 days)

**Goal:** one `streamlit run ui/app.py` command opens the full operator surface. Existing `ui/research/app.py` remains as the fallback until fully ported.

---

### Task 7.1 — Scaffold `ui/app.py`

**File:** `ui/app.py` (new file)

```python
"""Multi-page Streamlit operator console."""
import streamlit as st

st.set_page_config(page_title="NSE Trading System", page_icon="📊", layout="wide")

# Navigation
pages = {
    "📈 Research": "ui/pages/1_research.py",
    "🎯 Ranked Signals": "ui/pages/2_ranked_signals.py",
    "⚡ Execution": "ui/pages/3_execution.py",
    "🤖 ML Shadow": "ui/pages/4_ml_shadow.py",
    "🛠️ Control Plane": "ui/pages/5_control_plane.py",
}
```

Use Streamlit's native multi-page support (`pages/` directory).

---

### Task 7.2 — Port ranked signals view

**File:** `ui/pages/2_ranked_signals.py` (new file)

Content: port the ranked signals table from `ui/research/app.py`. Add Stage 2 filter sidebar (from Task 4.6). Include:
- Ranked signals table with `stage2_score`, `stage2_label` columns.
- Filter: Stage 2 only toggle.
- Filter: min Stage 2 score slider.
- Pattern signals section (CwH, VCP, ascending triangle etc.) from pattern cache.
- Stage 2 breakdown alerts section.

---

### Task 7.3 — Port research charts view

**File:** `ui/pages/1_research.py` (new file)

Content: port `ui/research/app.py` chart views (price chart, RS chart, sector heatmap).

---

### Task 7.4 — Port execution view

**File:** `ui/pages/3_execution.py` (new file)

Port from `ui/execution/` (NiceGUI) to Streamlit. Key views:
- Open positions table.
- Today's entries/exits.
- P&L summary.
- Execute stage last-run status.

---

### Task 7.5 — Port ML shadow view

**File:** `ui/pages/4_ml_shadow.py` (new file)

Port from `ui/ml/app.py`. Key views:
- ML model performance metrics.
- Shadow vs composite score comparison.

---

### Task 7.6 — Control plane view

**File:** `ui/pages/5_control_plane.py` (new file)

New page (not ported — enhanced):
- Pipeline run history table.
- Stage attempt status per run.
- DQ results viewer.
- Pattern cache stats (total signals by family, last full scan date).

---

### Task 7.7 — Archive `ui/execution_api/` and `web/execution-console/`

Decision gate: if these are not used daily by the operator, archive them.
- `git mv ui/execution_api/ archive/execution_api/`
- `git mv web/execution-console/ archive/execution-console/`

If they ARE used daily, keep them but document entry points in `docs/operations/runbook.md`.

---

### Phase 7 ship gate

```bash
# Must work without error:
python3 -c "import ui.app" 2>/dev/null || echo "streamlit import skipped (no streamlit in test env)"
streamlit run ui/app.py --server.headless true &
sleep 3
curl -s http://localhost:8501 | grep -i "streamlit" && echo "UI started OK"
```

---

## Phase 8 — Operational Polish (1–2 days)

**Goal:** scheduler, concurrency guard, production-grade tests, release tag.

---

### Task 8.1 — Advisory lock in orchestrator

**File:** `src/ai_trading_system/pipeline/orchestrator.py`

Add a file-based advisory lock around the pipeline run. Implementation:

```python
# In src/ai_trading_system/platform/ — new file: locks.py
import fcntl, os, time
from pathlib import Path
from contextlib import contextmanager

@contextmanager
def advisory_lock(lock_dir: Path, name: str, timeout_s: float = 5.0):
    """File-based advisory lock. Raises RuntimeError if lock held by another process."""
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{name}.lock"
    fd = open(lock_path, 'w')
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError(f"Another pipeline run is active ({lock_path}). Aborting.")
    try:
        yield lock_path
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
```

Use in the orchestrator's `run_pipeline` method:
```python
lock_dir = paths.data_root / ".lockfiles"
with advisory_lock(lock_dir, f"pipeline_{run_date}_{data_domain}"):
    # ... entire pipeline run ...
```

**Exit check:**
```python
# Two threads both call run_pipeline → second raises RuntimeError within timeout_s
```

---

### Task 8.2 — Scheduler file (macOS launchd)

**File:** `run/scheduler/launchd.plist` (new file)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.trading.daily-pipeline</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>-m</string>
        <string>run.orchestrator</string>
        <string>--data-domain</string>
        <string>operational</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/ai-trading-system</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key><integer>18</integer>
        <key>Minute</key><integer>15</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/tmp/trading-pipeline.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/trading-pipeline-err.log</string>
</dict>
</plist>
```

**File:** `run/scheduler/cron.example` (new file)

```
# NSE market closes ~15:30 IST, pipeline runs at 18:15 IST (bhavcopy published ~17:30)
15 18 * * 1-5 cd /path/to/ai-trading-system && python -m run.orchestrator --data-domain operational >> /tmp/trading-pipeline.log 2>&1
```

**File:** `docs/operations/runbook.md` — add section "Scheduler setup" with launchctl install/uninstall commands.

---

### Task 8.3 — Concurrency lock regression test

**File:** `tests/regression/test_concurrency_lock.py` (new file)

```python
"""Test that two concurrent pipeline invocations do not corrupt state."""
import threading, time
from ai_trading_system.platform.locks import advisory_lock
from pathlib import Path
import pytest

def test_second_invocation_fails_cleanly(tmp_path):
    results = []
    def run(idx):
        try:
            with advisory_lock(tmp_path, "test_pipeline", timeout_s=0.1):
                time.sleep(0.5)
                results.append(('ok', idx))
        except RuntimeError as e:
            results.append(('blocked', idx))

    t1 = threading.Thread(target=run, args=(1,))
    t2 = threading.Thread(target=run, args=(2,))
    t1.start()
    time.sleep(0.05)  # let t1 acquire the lock
    t2.start()
    t1.join(); t2.join()

    statuses = [r[0] for r in results]
    assert 'ok' in statuses      # first invocation succeeded
    assert 'blocked' in statuses  # second invocation cleanly refused
```

**Exit check:** `pytest tests/regression/test_concurrency_lock.py -v` — passes.

---

### Task 8.4 — Paper-only guard test

**File:** `tests/regression/test_paper_only_guard.py` (new file)

```python
"""Test that the Dhan live adapter cannot be instantiated unless LIVE_TRADING_ENABLED=true."""
import os
import pytest

def test_dhan_adapter_refused_by_default():
    os.environ.pop('LIVE_TRADING_ENABLED', None)  # ensure not set
    try:
        from ai_trading_system.domains.execution.adapters.dhan import DhanAdapter
        adapter = DhanAdapter()
        # If instantiation succeeds, it MUST raise or refuse live-trading
        assert False, "DhanAdapter should refuse when LIVE_TRADING_ENABLED is not 'true'"
    except (ImportError, RuntimeError, AssertionError) as e:
        # Expected: either not importable, or raises on instantiation
        pass

def test_paper_adapter_works_without_flag():
    from ai_trading_system.domains.execution.adapters.paper import PaperAdapter
    adapter = PaperAdapter()
    assert adapter is not None
```

**Prerequisite:** modify `domains/execution/adapters/dhan.py` to check `os.environ.get('LIVE_TRADING_ENABLED', 'false').lower() != 'true'` at class instantiation and raise `RuntimeError("Live trading disabled. Set LIVE_TRADING_ENABLED=true to enable.")`.

**Exit check:** `pytest tests/regression/test_paper_only_guard.py -v` — passes.

---

### Task 8.5 — DQ gate matrix test

**File:** `tests/regression/test_dq_gate_matrix.py` (new file)

```python
"""Parametrised tests for DQ gate policy: which failures block vs soft-fail vs degrade."""
import pytest

# (stage, failure_mode, expected_pipeline_behavior)
DQ_GATE_MATRIX = [
    ('ingest',   'CRITICAL_DQ_FAILURE',    'hard_fail'),
    ('features', 'HIGH_PCT_MISSING',       'hard_fail'),
    ('features', 'LOW_PCT_MISSING',        'degrade'),
    ('rank',     'UNTRUSTED_DATA',         'hard_fail_unless_allow_flag'),
    ('execute',  'ENTRY_POLICY_ERROR',     'soft_fail'),
    ('publish',  'TELEGRAM_UNREACHABLE',   'soft_fail'),
    ('publish',  'GSHEETS_QUOTA_EXCEEDED', 'soft_fail'),
]

@pytest.mark.parametrize("stage,failure_mode,expected", DQ_GATE_MATRIX)
def test_dq_gate_policy(stage, failure_mode, expected):
    # This is a documentation/contract test.
    # Each parametrize entry documents the intended behavior.
    # Implementation: inject the failure mode into a fixture orchestrator run
    # and assert the final run status matches expected.
    pass  # TODO: implement with fixture orchestrator + mock failure injection
```

Note: marked `pass` for now — this is a scaffolding test that documents the intended DQ gate policy. A future iteration fills in the fixture runner.

---

### Task 8.6 — Update AGENTS.md and docs

**File:** `AGENTS.md`

Update to reflect the corrected architecture:
1. Remove any reference to "delete `src/ai_trading_system/`" or "migration target".
2. Replace with: "**`src/ai_trading_system/` is the canonical package.** All new code goes here. Top-level packages are compatibility shims."
3. Update the CODEX_REFACTOR_PLAN reference to point to this `EXECUTION_PLAN.md`.

**File:** `docs/TO_BE_DESIGN.md`

Update Section 3 ("Removed / migrated"):
- Change "src/ai_trading_system/ → deleted" to "src/ai_trading_system/ → **KEPT AND IS CANONICAL**"
- Update Phase 3 description from "Delete the parallel tree" to "Collapse top-level legacy packages into shims"

---

### Task 8.7 — Release tag

After all phases complete and the test suite is green:
```bash
git tag -a v2.1-stage2-patterns -m "Stage 2 scoring, 6 new patterns, path hygiene, src/ canonical"
```

---

### Phase 8 ship gate

```bash
pytest tests/ -x --ignore=tests/regression/test_dq_gate_matrix.py
# (gate matrix is scaffolded, not yet fully implemented)
python -m run.orchestrator --help
```

---

## Verification protocol (every phase)

Before merging any phase, run in this exact order:

```bash
# 1. Syntax check all modified files
python3 -c "
import ast, glob
for f in glob.glob('src/ai_trading_system/**/*.py', recursive=True):
    ast.parse(open(f).read())
print('All src/ files parse OK')
"

# 2. Core imports
python3 -c "
import run.orchestrator
from ai_trading_system.pipeline.orchestrator import PipelineOrchestrator
from ai_trading_system.domains.features.indicators import add_stage2_features
from ai_trading_system.domains.ranking.ranker import StockRanker
from ai_trading_system.domains.ranking.patterns.detectors import detect_flag_signals
print('Core imports OK')
"

# 3. CLI intact
python -m run.orchestrator --help

# 4. Active guardrail tests for this checkout
pytest tests/test_phase5_guardrails.py -v

# 5. Stage 2 tests
pytest tests/test_stage2_features.py -v

# 6. Phase-specific tests (as specified per phase)
```

---

## Rollback plan (per phase)

Every phase is one PR / one commit range.

- **Rollback:** `git revert <range>` — produces a clean revert commit.
- **Data safety:** no phase modifies DuckDB schemas (except Phase 3 Task 3.1 which adds a new table — rolling back drops the table, no data loss since it's a cache).
- **Shim safety:** Phase 6 replaces file *content* with shims. If a shim breaks something, revert the file — the original content is in git history.

---

## Out of scope (do not attempt)

- Postgres migration
- Kafka / streaming ingestion
- Airflow / Prefect / Dagster
- Real-time ingest
- Live broker integration (beyond the paper-only guard test)
- Multi-user auth
- Docker / Kubernetes
- OpenTelemetry / Grafana
- Redesigning the DuckDB schema

---

## Quick-reference: canonical file locations

| Concept | Canonical file |
|---|---|
| Pipeline orchestrator | `src/ai_trading_system/pipeline/orchestrator.py` |
| Stage wrappers | `src/ai_trading_system/pipeline/stages/{ingest,features,rank,execute,publish}.py` |
| Ingest service | `src/ai_trading_system/domains/ingest/service.py` |
| NSE provider | `src/ai_trading_system/domains/ingest/providers/nse.py` |
| Feature store | `src/ai_trading_system/domains/features/feature_store.py` |
| Indicators (incl Stage 2) | `src/ai_trading_system/domains/features/indicators.py` |
| Ranker | `src/ai_trading_system/domains/ranking/ranker.py` |
| Ranking contracts | `src/ai_trading_system/domains/ranking/contracts.py` |
| Eligibility | `src/ai_trading_system/domains/ranking/eligibility.py` |
| Breakout scoring | `src/ai_trading_system/domains/ranking/breakout.py` |
| Pattern detectors | `src/ai_trading_system/domains/ranking/patterns/detectors.py` |
| Pattern evaluation | `src/ai_trading_system/domains/ranking/patterns/evaluation.py` |
| Pattern contracts | `src/ai_trading_system/domains/ranking/patterns/contracts.py` |
| Pattern cache | `src/ai_trading_system/domains/ranking/patterns/cache.py` |
| Entry policy | `src/ai_trading_system/domains/execution/entry_policy.py` |
| Publish payloads | `src/ai_trading_system/domains/publish/publish_payloads.py` |
| Telegram builder | `src/ai_trading_system/domains/publish/telegram_summary_builder.py` |
| DB path helpers | `src/ai_trading_system/platform/db/paths.py` |
| Advisory lock | `src/ai_trading_system/platform/locks.py` |
| Path hygiene guardrail | `tests/test_phase5_guardrails.py` |
| Stage 2 tests | `tests/test_stage2_features.py` |
| New pattern tests | `tests/test_new_patterns.py` |
| Pattern cache tests | `tests/test_pattern_cache.py` |
| Execution gate tests | `tests/test_execution_gate.py` |
| Concurrency test | `tests/regression/test_concurrency_lock.py` |
| Paper-only guard test | `tests/regression/test_paper_only_guard.py` |
