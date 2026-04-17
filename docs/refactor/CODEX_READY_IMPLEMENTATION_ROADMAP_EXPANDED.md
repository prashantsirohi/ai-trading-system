# CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md
## AI Trading System – Codex-Ready Implementation Roadmap (Expanded Edition)

### Version: 2.0
### Maintained By: Prashant Sirohi
### Purpose
Convert the Business Logic Review into a Codex-executable implementation roadmap with:
- ordered phases
- concrete file targets
- code stubs / pseudocode
- acceptance criteria
- phase-specific Codex prompts
- compatibility and safety constraints

---

## 1. How to Use This Document

This roadmap should be executed **after** structural refactor phases are complete.

It assumes the repository already has:

- staged pipeline: `ingest -> features -> rank -> execute -> publish`
- `core/` runtime utilities
- `services/` orchestration and business modules
- artifact-driven pipeline contracts
- backward-compatible execution API
- trust and DQ enforcement

### Execution rule
Codex must execute **one phase at a time** and must not mix phases in one pass.

---

## 2. Global Constraints

Codex must:

- preserve existing artifact filenames and folder layout
- preserve existing public API routes and core contracts
- keep all changes additive or config-gated
- add tests for every logic enhancement
- preserve trust and DQ gates
- preserve artifact replayability
- keep changes small and reviewable

Codex must not:

- rename `ranked_signals.csv`, `breakout_scan.csv`, `pattern_scan.csv`, `stock_scan.csv`, `sector_dashboard.csv`, `dashboard_payload.json`, `execute_summary.json`, `publish_summary.json`
- replace DuckDB / Parquet
- enable live trading
- replace the technical ranking core with ML
- remove compatibility shims without validation
- merge research and operational domains

---

## 3. Compatibility Contract

### Artifact compatibility
Allowed:
- additive columns
- additive JSON keys
- additive metadata in summaries
- new internal helper modules

Not allowed:
- changing meaning of required fields
- removing fields consumed by downstream stages
- changing run folder semantics

### API compatibility
Allowed:
- using read models behind existing endpoints
- additive response keys
- better internal service layering

Not allowed:
- removing existing routes
- changing request shape incompatibly
- breaking dashboard / React consumers

---

# Phase 1 — Ingest Enhancements

## Goal
Expand ingest from trusted OHLCV capture into a richer market context and governance layer while keeping current source-of-record behavior intact.

## Deliverables
- benchmark/index ingestion support
- adjusted price scaffolding
- symbol master governance
- freshness status contract
- provider reconciliation and confidence markers
- optional benchmark context propagation for downstream features

## Create
- `services/ingest/benchmark_ingest.py`
- `core/symbol_master.py`
- `tests/ingest/test_benchmark_ingest.py`
- `tests/ingest/test_symbol_master.py`
- `tests/ingest/test_freshness_contract.py`
- `tests/ingest/test_provider_reconciliation.py`

## Modify
- `collectors/daily_update_runner.py`
- `collectors/nse_collector.py`
- `collectors/dhan_collector.py` (diagnostics or reconciliation only)
- `run/stages/ingest.py`
- `analytics/data_trust.py`

---

## Task 1.1 — Benchmark ingestion

### Intent
Treat benchmark and sector index data as first-class pipeline inputs.

### Stub
```python
# services/ingest/benchmark_ingest.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import pandas as pd


@dataclass(frozen=True)
class BenchmarkSpec:
    symbol: str
    label: str
    provider: str = "nse"
    instrument_type: str = "index"


DEFAULT_BENCHMARKS = [
    BenchmarkSpec("NIFTY_50", "NIFTY 50"),
    BenchmarkSpec("NIFTY_500", "NIFTY 500"),
    BenchmarkSpec("NIFTY_BANK", "NIFTY BANK"),
    BenchmarkSpec("NIFTY_AUTO", "NIFTY AUTO"),
    BenchmarkSpec("NIFTY_IT", "NIFTY IT"),
]
# services/ingest/benchmark_config.py

BENCHMARKS = [
    # Core Benchmarks
    "NIFTY 50",
    "NIFTY 100",
    "NIFTY 200",
    "NIFTY 500",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150",
    "NIFTY SMLCAP 100",
    "NIFTY SMLCAP 250",
    "NIFTY MIDSML 400",

    # Sector Indices
    "NIFTY BANK",
    "NIFTY FINSRV25 50",
    "NIFTY IT",
    "NIFTY AUTO",
    "NIFTY PHARMA",
    "NIFTY FMCG",
    "NIFTY ENERGY",
    "NIFTY INFRA",
    "NIFTY REALTY",
    "NIFTY METAL",
    "NIFTY PSU BANK",
    "NIFTY PSE",
    "NIFTY COMMODITIES",
    "NIFTY CONSUMPTION",
    "NIFTY MEDIA",
    "NIFTY SERV SECTOR",
    "NIFTY MNC",
]
# Take data from NSE bhavcopy
def fetch_benchmark_rows(spec: BenchmarkSpec, date_range: Iterable[str]) -> pd.DataFrame:
    # TODO: replace with real provider fetch
    rows = []
    for dt in date_range:
        rows.append(
            {
                "symbol": spec.symbol,
                "date": dt,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "volume": 0,
                "provider": spec.provider,
                "instrument_type": spec.instrument_type,
                "is_benchmark": True,
            }
        )
    return pd.DataFrame(rows)


def ingest_benchmarks(date_range: Iterable[str], specs: list[BenchmarkSpec] | None = None) -> pd.DataFrame:
    specs = specs or DEFAULT_BENCHMARKS
    frames = [fetch_benchmark_rows(spec, date_range) for spec in specs]
    if not frames:
        return pd.DataFrame()
    output = pd.concat(frames, ignore_index=True)
    return output
```

### Acceptance
- benchmark rows load through ingest pipeline
- schema remains compatible with catalog consumers
- indices can be identified by metadata without breaking stock consumers

---

## Task 1.2 — Adjusted OHLC scaffolding

### Intent
Add adjusted-price fields without overwriting raw OHLCV.

### Stub
```python
# inside collectors/daily_update_runner.py
import pandas as pd


def apply_adjustment_fields(frame: pd.DataFrame, corporate_actions: pd.DataFrame | None = None) -> pd.DataFrame:
    output = frame.copy()
    output["adjusted_open"] = output["open"]
    output["adjusted_high"] = output["high"]
    output["adjusted_low"] = output["low"]
    output["adjusted_close"] = output["close"]
    output["adjustment_factor"] = 1.0
    output["adjustment_source"] = None

    # TODO:
    # 1. join corporate action events by symbol/date
    # 2. compute cumulative adjustment factors
    # 3. preserve raw OHLC as source-of-record
    return output
```

### Acceptance
- raw OHLC columns remain unchanged
- additive adjusted fields exist
- downstream consumers can ignore adjusted fields safely

---

## Task 1.3 — Symbol master governance

### Intent
Support ISIN mapping, symbol aliases, rename handling, active universe membership, and lifecycle status.

### Stub
```python
# core/symbol_master.py
from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True)
class SymbolRecord:
    symbol: str
    canonical_symbol: str
    isin: str | None
    status: str  # active, suspended, delisted, renamed
    sector: str | None = None
    industry: str | None = None


class SymbolMaster:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame.copy()

    def canonicalize(self, symbol: str) -> str:
        rows = self.frame[self.frame["symbol"] == symbol]
        if rows.empty:
            return symbol
        return str(rows.iloc[0].get("canonical_symbol") or symbol)

    def isin_for(self, symbol: str) -> str | None:
        canonical = self.canonicalize(symbol)
        rows = self.frame[self.frame["canonical_symbol"] == canonical]
        if rows.empty:
            return None
        value = rows.iloc[0].get("isin")
        return None if pd.isna(value) else str(value)

    def is_active(self, symbol: str) -> bool:
        canonical = self.canonicalize(symbol)
        rows = self.frame[self.frame["canonical_symbol"] == canonical]
        if rows.empty:
            return True
        return str(rows.iloc[0].get("status", "active")).lower() == "active"

    def filter_active(self, symbols: list[str]) -> list[str]:
        return [symbol for symbol in symbols if self.is_active(symbol)]
```

### Acceptance
- ingest can canonicalize symbols before write
- alias/rename support is available
- active universe filtering can be applied without touching research domain logic

---

## Task 1.4 — Freshness contract

### Intent
Expose whether the ingest output is current, delayed, or stale.

### Stub
```python
# run/stages/ingest.py
from __future__ import annotations


def classify_freshness_status(target_end_date: str, latest_available_date: str | None) -> str:
    if latest_available_date is None:
        return "stale"
    if latest_available_date == target_end_date:
        return "fresh"
    return "delayed"
```

### Example summary addition
```python
ingest_summary["freshness_status"] = classify_freshness_status(
    target_end_date=target_end_date,
    latest_available_date=latest_catalog_date,
)
```

### Acceptance
- `ingest_summary.json` contains `freshness_status`
- logic is test-covered
- downstream stages can surface it in trust or operator summaries

---

## Task 1.5 — Provider reconciliation

### Intent
Prefer primary provider data but mark discrepancies and confidence.

### Stub
```python
# collectors/daily_update_runner.py or analytics/data_trust.py
def reconcile_provider_row(primary_row: dict, fallback_row: dict | None = None) -> dict:
    chosen = dict(primary_row)
    chosen["provider_confidence"] = 1.0
    chosen["provider_discrepancy_flag"] = False
    chosen["provider_discrepancy_note"] = None

    if fallback_row is None:
        return chosen

    primary_close = primary_row.get("close")
    fallback_close = fallback_row.get("close")
    if primary_close is None or fallback_close is None:
        return chosen

    diff = abs(float(primary_close) - float(fallback_close))
    if diff > 0:
        chosen["provider_discrepancy_flag"] = True
        chosen["provider_discrepancy_note"] = f"primary_vs_fallback_close_diff={diff}"
        chosen["provider_confidence"] = 0.8
    return chosen
```

### Acceptance
- primary source remains authoritative
- discrepancy metadata is preserved
- confidence becomes available for feature propagation

---

## Task 1.6 — Benchmark propagation hook

### Intent
Make benchmark rows discoverable by features and regime logic.

### Stub
```python
# services/ingest/benchmark_ingest.py
def benchmark_lookup(frame: pd.DataFrame) -> pd.DataFrame:
    cols = ["symbol", "date", "close"]
    subset = frame[cols].copy()
    subset.rename(columns={"close": "benchmark_close"}, inplace=True)
    return subset
```

### Acceptance
- features layer can access benchmark time series without special-case hacks

---

## Phase 1 Acceptance Criteria
- benchmark ingestion support exists
- adjusted price scaffolding is additive only
- symbol master governance exists
- ingest summary contains freshness classification
- provider reconciliation exposes confidence / discrepancy metadata
- tests exist for new logic

## Codex Prompt — Phase 1
```text
Read docs/refactor/CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md and execute only Phase 1 — Ingest Enhancements.

Implement benchmark ingestion, adjusted-price scaffolding, symbol master governance, freshness status, provider reconciliation, and benchmark propagation hooks.

Constraints:
- Preserve current ingest artifact behavior.
- Do not overwrite raw OHLC values.
- Keep all changes additive and test-backed.
```

---

# Phase 2 — Feature Enhancements

## Goal
Upgrade the feature layer from indicator computation into a decision-ready state engine with readiness, confidence, liquidity, cross-sectional, and pattern-precondition features.

## Deliverables
- feature readiness flags
- feature confidence
- multi-timeframe returns
- liquidity features
- cross-sectional percentile and sector ranks
- pattern precondition features
- benchmark-relative and sector-relative features

## Create
- `features/pattern_features.py`
- `tests/features/test_feature_readiness.py`
- `tests/features/test_feature_confidence.py`
- `tests/features/test_multi_timeframe_returns.py`
- `tests/features/test_cross_sectional_features.py`
- `tests/features/test_pattern_features.py`

## Modify
- `features/feature_store.py`
- `features/indicators.py`
- `features/compute_sector_rs.py`
- `services/features/orchestration.py`

---

## Task 2.1 — Feature readiness flags

### Stub
```python
# features/feature_store.py
import pandas as pd


def add_feature_readiness(frame: pd.DataFrame, min_lookback: int = 50) -> pd.DataFrame:
    output = frame.copy()
    output["feature_ready"] = output.groupby("symbol").cumcount() >= (min_lookback - 1)
    return output
```

### Acceptance
- rows with insufficient lookback are clearly marked
- no silent partial-feature ambiguity

---

## Task 2.2 — Feature confidence propagation

### Stub
```python
# features/feature_store.py
def add_feature_confidence(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["feature_confidence"] = 1.0

    if "feature_ready" in output.columns:
        output.loc[~output["feature_ready"], "feature_confidence"] = 0.0

    if "provider_confidence" in output.columns:
        output["feature_confidence"] = output[["feature_confidence", "provider_confidence"]].min(axis=1)

    return output
```

### Acceptance
- confidence is bounded and additive
- ingest confidence can flow downstream

---

## Task 2.3 — Multi-timeframe returns

### Stub
```python
# features/indicators.py
def add_multi_timeframe_returns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    grouped = output.groupby("symbol")

    for period in [5, 20, 60, 120, 252]:
        output[f"return_{period}d"] = grouped["close"].pct_change(period)

    return output
```

### Acceptance
- 5d, 20d, 60d, 120d, 252d returns exist
- backward-compatible with downstream consumers

---

## Task 2.4 — Liquidity features

### Stub
```python
# features/feature_store.py
def add_liquidity_features(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["turnover"] = output["close"] * output["volume"]
    output["liquidity_score"] = output.groupby("date")["turnover"].rank(pct=True)
    return output
```

### Acceptance
- turnover is available
- liquidity score is cross-sectional per date

---

## Task 2.5 — Cross-sectional features

### Stub
```python
# features/feature_store.py
def add_cross_sectional_features(frame: pd.DataFrame, metric: str = "return_20d") -> pd.DataFrame:
    output = frame.copy()
    output["rank_in_universe"] = output.groupby("date")[metric].rank(ascending=False, method="dense")
    output["percentile_score"] = output.groupby("date")[metric].rank(pct=True)

    if "sector" in output.columns:
        output["rank_in_sector"] = output.groupby(["date", "sector"])[metric].rank(ascending=False, method="dense")

    return output
```

### Acceptance
- universe and sector relative placement is available
- features can support rank explainability later

---

## Task 2.6 — Pattern precondition features

### Stub
```python
# features/pattern_features.py
from __future__ import annotations
import pandas as pd


def compute_pattern_preconditions(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()

    output["base_tightness"] = None
    output["consolidation_range_pct"] = None
    output["volatility_contraction"] = None
    output["pullback_depth_pct"] = None
    output["resistance_slope"] = None

    # TODO:
    # - base tightness from rolling range compression
    # - consolidation range percentage
    # - volatility contraction from ATR compression
    # - pullback depth relative to rolling highs
    # - resistance slope from local maxima trendline
    return output
```

### Acceptance
- module exists and integrates additively
- pattern scans can consume precondition features later

---

## Task 2.7 — Benchmark-relative features

### Stub
```python
# features/compute_sector_rs.py or feature_store.py
def add_benchmark_relative_features(frame: pd.DataFrame, benchmark_frame: pd.DataFrame, benchmark_symbol: str = "NIFTY_500") -> pd.DataFrame:
    output = frame.copy()
    bench = benchmark_frame[benchmark_frame["symbol"] == benchmark_symbol][["date", "close"]].copy()
    bench.rename(columns={"close": "benchmark_close"}, inplace=True)

    output = output.merge(bench, on="date", how="left")
    output["stock_vs_benchmark"] = (output["close"] / output["benchmark_close"]) - 1
    return output
```

### Acceptance
- benchmark-relative context is available
- features remain additive

---

## Phase 2 Acceptance Criteria
- readiness and confidence fields exist
- multi-timeframe and liquidity features exist
- cross-sectional features exist
- pattern feature module exists
- benchmark-relative features are plumbed
- tests cover new feature logic

## Codex Prompt — Phase 2
```text
Read docs/refactor/CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md and execute only Phase 2 — Feature Enhancements.

Implement feature readiness, feature confidence, multi-timeframe returns, liquidity features, cross-sectional features, pattern-precondition features, and benchmark-relative features.

Constraints:
- Preserve existing feature snapshot behavior.
- Keep all additions backward-compatible.
- Add tests for each new feature family.
```

---

# Phase 3 — Rank Enhancements

## Goal
Improve rank quality, explainability, and control without replacing the current technical factor engine.

## Deliverables
- eligibility filtering
- penalty scoring
- rank confidence
- signal freshness and decay
- explainability metadata
- optional rank stability controls
- rank mode scaffolding

## Create
- `services/rank/eligibility.py`
- `tests/rank/test_eligibility.py`
- `tests/rank/test_penalty_score.py`
- `tests/rank/test_rank_confidence.py`
- `tests/rank/test_signal_freshness.py`
- `tests/rank/test_explainability.py`

## Modify
- `services/rank/factors.py`
- `services/rank/composite.py`
- `services/rank/contracts.py`
- `services/rank/dashboard_payload.py`
- `services/rank/orchestration.py`

---

## Task 3.1 — Eligibility filter

### Stub
```python
# services/rank/eligibility.py
from __future__ import annotations
import pandas as pd


def apply_rank_eligibility(
    frame: pd.DataFrame,
    *,
    min_price: float = 20.0,
    min_liquidity_score: float = 0.20,
) -> pd.DataFrame:
    output = frame.copy()
    output["eligible_rank"] = True
    output["rejection_reasons"] = [[] for _ in range(len(output))]

    low_price = output["close"] < min_price
    output.loc[low_price, "eligible_rank"] = False

    if "feature_ready" in output.columns:
        not_ready = ~output["feature_ready"]
        output.loc[not_ready, "eligible_rank"] = False

    if "liquidity_score" in output.columns:
        illiquid = output["liquidity_score"] < min_liquidity_score
        output.loc[illiquid, "eligible_rank"] = False

    return output
```

### Acceptance
- rank eligibility is explicit
- reject reasons can be packaged later

---

## Task 3.2 — Penalty system

### Stub
```python
# services/rank/factors.py
def compute_penalty_score(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["penalty_score"] = 0.0

    if {"close", "sma200"}.issubset(output.columns):
        output.loc[output["close"] < output["sma200"], "penalty_score"] += 10.0

    if "liquidity_score" in output.columns:
        output.loc[output["liquidity_score"] < 0.20, "penalty_score"] += 10.0

    if {"atr_14", "close"}.issubset(output.columns):
        high_vol = (output["atr_14"] / output["close"]) > 0.08
        output.loc[high_vol, "penalty_score"] += 5.0

    return output
```

### Acceptance
- penalties are additive and explainable
- core factor scores remain intact

---

## Task 3.3 — Rank confidence

### Stub
```python
# services/rank/composite.py
def compute_rank_confidence(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["rank_confidence"] = 1.0

    if "feature_confidence" in output.columns:
        output["rank_confidence"] *= output["feature_confidence"].fillna(0.0)

    if "eligible_rank" in output.columns:
        output.loc[~output["eligible_rank"], "rank_confidence"] = 0.0

    if "penalty_score" in output.columns:
        output["rank_confidence"] *= (1 - output["penalty_score"].clip(lower=0.0, upper=50.0) / 100.0)

    return output
```

### Acceptance
- confidence is derived, not opaque
- rank confidence can later flow to execute/publish

---

## Task 3.4 — Signal freshness and decay

### Stub
```python
# services/rank/factors.py
def add_signal_freshness(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["signal_age"] = 0
    output["signal_decay_score"] = 1.0

    # TODO:
    # - infer setup start date or breakout trigger date
    # - compute days since signal became active
    # - decay stale setups gradually
    return output
```

### Acceptance
- staleness is explicit
- later execution prioritization can consume decay score

---

## Task 3.5 — Explainability fields

### Stub
```python
# services/rank/dashboard_payload.py
def build_score_breakdown(row: dict) -> dict:
    keys = [
        "relative_strength",
        "volume_intensity",
        "trend_persistence",
        "proximity_to_highs",
        "delivery_pct",
        "sector_strength",
        "penalty_score",
    ]
    return {key: row.get(key) for key in keys if key in row}


def build_top_factors(row: dict) -> list[str]:
    # TODO: rank factor contributions by score magnitude or percentile
    return []


def build_rejection_reasons(row: dict) -> list[str]:
    reasons = []
    if row.get("eligible_rank") is False:
        reasons.append("failed_eligibility")
    return reasons
```

### Acceptance
- explainability can be surfaced in dashboard payloads
- no existing consumer breaks if fields are ignored

---

## Task 3.6 — Rank stability controls

### Stub
```python
# services/rank/composite.py
def apply_rank_stability(current_frame: pd.DataFrame, previous_frame: pd.DataFrame | None = None) -> pd.DataFrame:
    output = current_frame.copy()
    output["rank_change_limit"] = None

    if previous_frame is None or previous_frame.empty:
        return output

    # TODO:
    # merge previous run rank position
    # optionally clip churn for names with tiny score changes
    return output
```

### Acceptance
- stability logic is optional or config-gated
- daily churn can be analyzed later without breaking rank output

---

## Task 3.7 — Rank mode scaffolding

### Stub
```python
# services/rank/contracts.py
RANK_MODES = ["default", "momentum", "breakout", "defensive", "watchlist"]
```

### Acceptance
- future regime-aware scoring can build on explicit rank modes

---

## Phase 3 Acceptance Criteria
- eligibility, penalties, confidence, freshness, and explainability exist
- additive rank metadata is available
- no artifact filename changes
- rank remains technically driven and backward-compatible

## Codex Prompt — Phase 3
```text
Read docs/refactor/CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md and execute only Phase 3 — Rank Enhancements.

Implement rank eligibility, penalty scoring, rank confidence, signal freshness, explainability metadata, rank stability scaffolding, and rank mode scaffolding.

Constraints:
- Preserve current rank artifact filenames and core scoring behavior.
- Keep changes additive or config-gated.
- Add tests for all new rank metadata.
```

---

# Phase 4 — Execute Enhancements

## Goal
Turn ranked signals into more explicit, risk-managed execution decisions while preserving trust gating and paper-safety.

## Deliverables
- entry policy module
- exit policy module
- ATR-based sizing
- portfolio constraints
- execution priority logic
- position lifecycle states
- execution weight scaffolding

## Create
- `services/execute/entry_policy.py`
- `services/execute/exit_policy.py`
- `tests/execute/test_entry_policy.py`
- `tests/execute/test_exit_policy.py`
- `tests/execute/test_position_sizing.py`
- `tests/execute/test_portfolio_constraints.py`

## Modify
- `services/execute/candidate_builder.py`
- `execution/autotrader.py`
- `execution/policies.py`
- `execution/service.py`
- `execution/portfolio.py`
- `analytics/risk_manager.py`

---

## Task 4.1 — Entry policy

### Stub
```python
# services/execute/entry_policy.py
from __future__ import annotations


def select_entry_policy(candidate: dict, policy_name: str = "breakout") -> dict:
    close = candidate.get("close")
    return {
        "entry_policy": policy_name,
        "entry_price": close,
        "entry_trigger": None,
        "entry_note": f"policy={policy_name}",
    }
```

### Acceptance
- candidate entry style is explicit
- future policy variants can be added without changing pipeline contract

---

## Task 4.2 — Exit policy

### Stub
```python
# services/execute/exit_policy.py
from __future__ import annotations


def build_exit_plan(candidate: dict, atr_multiple: float = 2.0, max_holding_days: int = 20) -> dict:
    close = candidate.get("close")
    atr = candidate.get("atr_14") or 0.0
    stop_loss = None if close is None else float(close) - (float(atr) * atr_multiple)

    return {
        "stop_loss": stop_loss,
        "trailing_stop": None,
        "time_stop_days": max_holding_days,
        "exit_reason": None,
    }
```

### Acceptance
- stop and time exit concepts are explicit
- execution policy becomes more inspectable

---

## Task 4.3 — ATR-based position sizing

### Stub
```python
# analytics/risk_manager.py or execution/policies.py
def compute_atr_position_size(
    capital: float,
    risk_per_trade: float,
    entry_price: float,
    atr: float,
    atr_multiple: float = 2.0,
) -> int:
    if capital <= 0 or entry_price <= 0 or atr <= 0:
        return 0

    risk_amount = capital * risk_per_trade
    stop_distance = atr * atr_multiple
    qty = int(risk_amount / stop_distance)
    return max(qty, 0)
```

### Acceptance
- position sizing is explicit and testable
- can remain config-gated so default behavior is preserved

---

## Task 4.4 — Portfolio constraints

### Stub
```python
# execution/portfolio.py
def check_portfolio_constraints(
    candidate: dict,
    portfolio_state: dict,
    *,
    max_positions: int = 10,
    max_sector_exposure: float = 0.30,
    max_single_stock_weight: float = 0.10,
) -> dict:
    reasons = []

    if portfolio_state.get("open_positions_count", 0) >= max_positions:
        reasons.append("max_positions_reached")

    # TODO:
    # - compute sector exposure
    # - compute stock weight
    # - block or downgrade candidate if constraints fail
    return {
        "allowed": len(reasons) == 0,
        "reasons": reasons,
    }
```

### Acceptance
- constraints are available without enabling live capital changes by default

---

## Task 4.5 — Execution priority

### Stub
```python
# services/execute/candidate_builder.py
import pandas as pd


def prioritize_execution_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [col for col in ["composite_score", "rank_confidence", "signal_decay_score"] if col in frame.columns]
    if not sort_cols:
        return frame
    return frame.sort_values(sort_cols, ascending=False)
```

### Acceptance
- limited capital can be allocated to best candidates first
- priority is transparent

---

## Task 4.6 — Position lifecycle states

### Stub
```python
# execution/portfolio.py or execution/models.py
POSITION_STATES = [
    "candidate",
    "active",
    "partial",
    "exit",
]
```

### Acceptance
- position state semantics are formalized
- logs and reports can reference them later

---

## Task 4.7 — Execution weight propagation

### Stub
```python
# services/execute/candidate_builder.py
def attach_execution_weight(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["execution_weight"] = output.get("rank_confidence", 1.0)
    return output
```

### Acceptance
- execution layer can consume rank confidence without changing order semantics by default

---

## Phase 4 Acceptance Criteria
- explicit entry/exit plans exist
- ATR sizing is available
- portfolio constraints are available
- execution priority exists
- lifecycle states exist
- trust gating remains intact

## Codex Prompt — Phase 4
```text
Read docs/refactor/CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md and execute only Phase 4 — Execute Enhancements.

Implement entry and exit policy modules, ATR-based sizing, portfolio constraints, execution priority, position lifecycle states, and execution-weight scaffolding.

Constraints:
- Preserve trust gating.
- Preserve preview mode.
- Do not enable live trading.
- Keep changes additive or config-gated.
```

---

# Phase 5 — Publish Enhancements

## Goal
Make outputs more actionable, explainable, trust-aware, and channel-specific while preserving retry-safe, idempotent delivery.

## Deliverables
- signal classification
- trust-aware output overlay
- diff-based publish fields
- explainability output packaging
- channel-specific formatting
- publish confidence field

## Create
- `services/publish/signal_classification.py`
- `tests/publish/test_signal_classification.py`
- `tests/publish/test_trust_overlay.py`
- `tests/publish/test_diff_publish.py`
- `tests/publish/test_channel_formatting.py`

## Modify
- `services/publish/telegram_summary_builder.py`
- `services/publish/publish_payloads.py`
- `publishers/dashboard.py`
- `publishers/google_sheets.py`
- `publishers/telegram.py`

---

## Task 5.1 — Signal classification

### Stub
```python
# services/publish/signal_classification.py
from __future__ import annotations


def classify_signal(row: dict) -> str:
    score = float(row.get("composite_score") or 0)
    if score >= 85:
        return "actionable"
    if score >= 65:
        return "watchlist"
    return "informational"
```

### Acceptance
- signal importance is explicit
- downstream channels can present urgency level

---

## Task 5.2 — Trust-aware overlay

### Stub
```python
# services/publish/publish_payloads.py
def apply_trust_overlay(payload: dict, trust_status: str) -> dict:
    output = dict(payload)
    output["trust_status"] = trust_status
    output["trust_warning"] = None

    if trust_status != "trusted":
        output["trust_warning"] = f"Trust status is {trust_status}. Review before acting."

    return output
```

### Acceptance
- trust state is visible in publish layer
- degraded / blocked state can be clearly communicated

---

## Task 5.3 — Diff-based publish fields

### Stub
```python
# services/publish/publish_payloads.py
def add_rank_diff(current_rows: list[dict], previous_rows: list[dict]) -> list[dict]:
    prev_rank_map = {row.get("symbol"): idx + 1 for idx, row in enumerate(previous_rows)}
    enriched = []

    for idx, row in enumerate(current_rows, start=1):
        symbol = row.get("symbol")
        previous_rank = prev_rank_map.get(symbol)
        enriched.append(
            {
                **row,
                "previous_rank": previous_rank,
                "rank_change": None if previous_rank is None else previous_rank - idx,
                "new_entry": previous_rank is None,
            }
        )
    return enriched
```

### Acceptance
- publish can show what's changed since prior run
- logic is additive only

---

## Task 5.4 — Explainability output packaging

### Stub
```python
# services/publish/publish_payloads.py
def attach_publish_explainability(row: dict) -> dict:
    return {
        **row,
        "why_selected": row.get("why_selected") or row.get("top_factors"),
        "key_factors": row.get("top_factors"),
        "risk_note": row.get("risk_note") or row.get("rejection_reasons"),
    }
```

### Acceptance
- operator-facing outputs include rationale
- publish layer remains concise or full depending on channel

---

## Task 5.5 — Channel-specific formatting

### Stub
```python
# services/publish/publish_payloads.py
def format_rows_for_channel(rows: list[dict], channel: str) -> dict:
    if channel == "telegram":
        return {"rows": rows[:10], "mode": "concise"}
    if channel == "sheets":
        return {"rows": rows, "mode": "full"}
    if channel == "dashboard":
        return {"rows": rows, "mode": "structured_json"}
    return {"rows": rows, "mode": "default"}
```

### Acceptance
- Telegram remains concise
- Sheets remains detailed
- dashboard remains structured and machine-friendly

---

## Task 5.6 — Publish confidence field

### Stub
```python
# services/publish/publish_payloads.py
def attach_publish_confidence(row: dict) -> dict:
    return {
        **row,
        "publish_confidence": row.get("rank_confidence"),
    }
```

### Acceptance
- publish layer can surface confidence without altering delivery semantics

---

## Phase 5 Acceptance Criteria
- signals are classified
- trust overlay exists
- diff fields exist
- explainability fields are publish-ready
- per-channel formatting exists
- publish remains retry-safe and idempotent

## Codex Prompt — Phase 5
```text
Read docs/refactor/CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md and execute only Phase 5 — Publish Enhancements.

Implement signal classification, trust-aware overlays, diff-based publish fields, explainability packaging, channel-specific formatting, and publish-confidence fields.

Constraints:
- Preserve current publish retry and dedupe behavior.
- Keep all changes additive.
- Do not break existing channel handlers.
```

---

# Phase 6 — Cross-Layer Logic

## Goal
Propagate trust, confidence, and auditability across ingest, features, rank, execute, and publish in a consistent way.

## Deliverables
- trust/confidence contract
- feature-to-rank confidence propagation
- rank-to-execute execution weight propagation
- publish trust/confidence surfacing
- auditability helpers
- cross-layer tests

## Create
- `core/contracts/trust_confidence.py`
- `tests/integration/test_trust_propagation.py`
- `tests/integration/test_confidence_propagation.py`
- `tests/integration/test_auditability.py`

## Modify
- `analytics/data_trust.py`
- `services/features/orchestration.py`
- `services/rank/orchestration.py`
- `services/execute/candidate_builder.py`
- `services/publish/publish_payloads.py`

---

## Task 6.1 — Trust / confidence envelope

### Stub
```python
# core/contracts/trust_confidence.py
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class TrustConfidenceEnvelope:
    trust_status: str
    provider_confidence: float | None = None
    feature_confidence: float | None = None
    rank_confidence: float | None = None
    execution_weight: float | None = None
```

### Acceptance
- contract is explicit and reusable

---

## Task 6.2 — Feature -> rank confidence propagation

### Stub
```python
# services/rank/orchestration.py
import pandas as pd


def attach_rank_confidence_from_features(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if "feature_confidence" in output.columns and "rank_confidence" not in output.columns:
        output["rank_confidence"] = output["feature_confidence"]
    return output
```

### Acceptance
- rank confidence is available when features expose confidence

---

## Task 6.3 — Rank -> execute confidence propagation

### Stub
```python
# services/execute/candidate_builder.py
def attach_execution_weight(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["execution_weight"] = output.get("rank_confidence", 1.0)
    return output
```

### Acceptance
- execution layer can consume ranking confidence without changing default order placement semantics

---

## Task 6.4 — Trust / confidence in publish

### Stub
```python
# services/publish/publish_payloads.py
def attach_publish_metadata(row: dict, trust_status: str) -> dict:
    return {
        **row,
        "trust_status": trust_status,
        "publish_confidence": row.get("rank_confidence"),
    }
```

### Acceptance
- operator sees trust and confidence context in output channels

---

## Task 6.5 — Auditability helpers

### Stub
```python
# shared helper
def attach_audit_fields(row: dict, *, run_id: str | None, stage: str | None, artifact_path: str | None) -> dict:
    return {
        **row,
        "audit_run_id": run_id,
        "audit_stage": stage,
        "audit_artifact_path": artifact_path,
    }
```

### Acceptance
- row-level or payload-level decisions remain traceable to artifacts and run IDs

---

## Task 6.6 — Cross-layer continuity checks

### Example test ideas
```python
def test_feature_confidence_flows_into_rank_confidence():
    ...

def test_rank_confidence_flows_into_execution_weight():
    ...

def test_publish_surfaces_trust_status():
    ...
```

### Acceptance
- trust/confidence continuity is integration-tested

---

## Phase 6 Acceptance Criteria
- explicit trust/confidence contract exists
- confidence can flow feature -> rank -> execute -> publish
- auditability metadata exists
- integration tests confirm propagation

## Codex Prompt — Phase 6
```text
Read docs/refactor/CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md and execute only Phase 6 — Cross-Layer Logic.

Implement trust and confidence contracts, propagation helpers, publish surfacing, auditability helpers, and integration tests.

Constraints:
- Preserve trust gating behavior.
- Keep propagation additive and backward-compatible.
- Preserve artifact-driven replayability.
```

---

## 4. Recommended Execution Order

Codex must execute exactly in this sequence:

1. Phase 1 — Ingest Enhancements
2. Phase 2 — Feature Enhancements
3. Phase 3 — Rank Enhancements
4. Phase 4 — Execute Enhancements
5. Phase 5 — Publish Enhancements
6. Phase 6 — Cross-Layer Logic

---

## 5. Recommended Working Pattern

For each phase:

1. read this roadmap
2. execute only that phase
3. create new files before rewiring existing code
4. add tests for new behavior
5. keep defaults conservative
6. summarize:
   - files changed
   - tests added
   - compatibility risks
   - config flags introduced

---

# 🚀 Phase 7 — Hardening & Deployment Readiness

## Objective
Strengthen reliability, security, CI/CD stability, and observability before production deployment.

---

## 🔹 Task 7.1 — Add Dependencies to pyproject.toml

### Code Stub
```toml
[project]
dependencies = [
    "pandas",
    "numpy",
    "duckdb",
    "pyarrow",
    "fastapi",
    "uvicorn",
    "pydantic",
    "python-dotenv",
    "quantstats",
    "plotly",
]

[project.optional-dependencies]
dev = [
    "pytest",
    "pytest-cov",
    "black",
    "ruff",
    "mypy",
]
```

---

## 🔹 Task 7.2 — Replace Bare except: Blocks

### Code Stub
```python
except Exception as exc:
    logger.warning("Feature store operation failed: %s", exc)
    raise
```

---

## 🔹 Task 7.3 — Add API Authentication Middleware

### Code Stub
```python
from fastapi import Request, HTTPException
import os

API_KEY = os.getenv("EXECUTION_API_KEY", "local-dev-key")

@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    if request.url.path.startswith("/api"):
        key = request.headers.get("x-api-key")
        if key != API_KEY:
            raise HTTPException(status_code=401, detail="Unauthorized")
    return await call_next(request)
```

---

## 🔹 Task 7.4 — Fix CI Cache and Failure Behavior

### Code Stub
```yaml
- name: Cache Features
  uses: actions/cache@v3
  with:
    path: data/features
    key: features-${{ github.sha }}

- name: Verify Data Availability
  run: |
    if [ ! -d "data" ]; then
      echo "Data directory missing."
      exit 1
    fi
```

---

## 🔹 Task 7.5 — Wire AlertManager Fan-Out

### Code Stub
```python
import requests
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_alert(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    })
```

---

# 📦 Backlog — Technical Debt & Scalability

## Item 10 — Split features/feature_store.py
Refactor into modular components:
```
features/
├── indicators.py
├── registry.py
├── io.py
├── confidence.py
└── orchestration.py
```

---

## Item 11 — Optimize DuckDB Connections
```python
import threading
duckdb_lock = threading.Lock()

with duckdb_lock:
    conn = duckdb.connect(db_path)
```

---

## Item 12 — Handle _apply_1yr_penalty
```python
if args.enable_1yr_penalty:
    scores = _apply_1yr_penalty(scores)
```

---

## Item 13 — Remove Tracked Virtual Environments
```bash
git rm -r --cached streamlit/Lib run/Lib
```

Add to `.gitignore`:
```
Lib/
venv/
.env/
```

---

## Item 14 — Add Concurrency Tests
```python
def test_concurrent_duckdb_access():
    import threading

    def run_pipeline():
        pass

    def query_api():
        pass

    t1 = threading.Thread(target=run_pipeline)
    t2 = threading.Thread(target=query_api)

    t1.start()
    t2.start()
    t1.join()
    t2.join()
```

---

## 📊 Recommended Execution Order

1. Phase 7 — Hardening & Deployment Readiness
2. Backlog Item 10 — Feature Store Refactor
3. Backlog Item 14 — Concurrency Testing
4. Backlog Item 11 — DuckDB Optimization
5. Backlog Item 12 — Ranking Penalty Cleanup
6. Backlog Item 13 — Repository Hygiene

---

## ▶️ Codex Execution Commands

### Execute Phase 7
```
Read docs/refactor/CODEX_READY_IMPLEMENTATION_ROADMAP_EXPANDED.md and execute only Phase 7.
```

### Execute Backlog Tasks
```
Execute Backlog Item 10 — Split features/feature_store.py.
Execute Backlog Item 11 — Optimize DuckDB connections.
Execute Backlog Item 12 — Handle _apply_1yr_penalty.
Execute Backlog Item 13 — Remove tracked virtual environments.
Execute Backlog Item 14 — Add concurrency tests.
```



## 6. Final Operator Note

This expanded roadmap is intentionally conservative and aligned with your current refactored architecture.

It does **not** attempt to:
- replace your technical ranking engine
- introduce ML-driven live decisions
- break artifact compatibility
- bypass trust-first execution logic

It is designed to let Codex move phase-by-phase, safely, and in a way that fits the current repository instead of fighting it.
