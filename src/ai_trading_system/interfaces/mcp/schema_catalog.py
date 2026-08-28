"""Column dictionary for the surfaces the MCP exposes.

The point of this module is to stop an agent grepping the repository to work
out what a column means, which store owns it, or which of the two stage
spellings it is looking at. Constants only — no I/O.

Sourced from ``docs/reference/ranking_factors.md``,
``docs/reference/database_schema.md``, and the DDL cited beside each surface.
"""

from __future__ import annotations

from typing import Any

from ai_trading_system.domains.opportunities.contracts import (
    LEGACY_STAGE_MAP,
    WeinsteinStage,
    is_transition,
    legacy_code_for,
    stage_family,
)
from ai_trading_system.domains.fundamentals.contracts import FundamentalThesisFamily


def _column(
    name: str, dtype: str, meaning: str, *, units: str | None = None
) -> dict[str, Any]:
    return {"name": name, "type": dtype, "meaning": meaning, "units": units}


OHLCV_SURFACE: dict[str, Any] = {
    "surface": "ohlcv",
    "tool": "get_ohlcv",
    "store": "ohlcv.duckdb",
    "tables": ["_catalog", "_catalog_feature_source", "_delivery"],
    "grain": "one row per trading session per (symbol_id, exchange)",
    "as_of_support": "EXACT",
    "notes": [
        "_catalog.close is UNADJUSTED. _catalog_feature_source exposes "
        "COALESCE(adjusted_*, raw) under the plain column names, which is the "
        "basis every technical indicator is computed on.",
        "get_ohlcv defaults to the adjusted basis; meta.price_basis always "
        "states which one was returned.",
    ],
    "columns": [
        _column("date", "date", "Exchange trading session."),
        _column("open", "float", "Session opening price.", units="INR"),
        _column("high", "float", "Session high.", units="INR"),
        _column("low", "float", "Session low.", units="INR"),
        _column("close", "float", "Session close.", units="INR"),
        _column("volume", "int", "Traded quantity.", units="shares"),
        _column(
            "delivery_pct",
            "float",
            "Share of traded volume taken to demat delivery; a conviction "
            "proxy. NSE only.",
            units="percent",
        ),
    ],
}

TECHNICALS_SURFACE: dict[str, Any] = {
    "surface": "technicals",
    "tool": "get_technical_features",
    "store": "feature_store/<family>/<exchange>/<SYMBOL>.parquet + ohlcv.duckdb",
    "tables": ["feat_phase1_symbol_features"],
    "grain": "one row per trading session per (symbol_id, exchange)",
    "as_of_support": "EXACT",
    "notes": [
        "Computed on the split-adjusted price basis; compare against "
        "get_ohlcv(adjusted=True), not raw candles.",
        "Families with no partition for a symbol are skipped and listed in "
        "meta.notes rather than failing the read.",
    ],
    "columns": [
        _column("date", "date", "Exchange trading session."),
        _column(
            "close",
            "float",
            "Adjusted close the indicators on this row were derived from.",
            units="INR",
        ),
        _column("rsi_14", "float", "14-period Relative Strength Index.", units="0-100"),
        _column(
            "adx_14", "float", "14-period Average Directional Index; trend strength."
        ),
        _column("plus_di_14", "float", "Positive directional indicator."),
        _column("minus_di_14", "float", "Negative directional indicator."),
        _column("sma_20", "float", "20-session simple moving average.", units="INR"),
        _column("sma_50", "float", "50-session simple moving average.", units="INR"),
        _column("sma_200", "float", "200-session simple moving average.", units="INR"),
        _column(
            "ema_12", "float", "12-session exponential moving average.", units="INR"
        ),
        _column(
            "ema_26", "float", "26-session exponential moving average.", units="INR"
        ),
        _column("macd_line", "float", "EMA(12) − EMA(26)."),
        _column("macd_signal_9", "float", "9-period EMA of the MACD line."),
        _column("macd_histogram", "float", "MACD line minus signal."),
        _column("atr_14", "float", "14-period Average True Range.", units="INR"),
        _column("bb_middle_20", "float", "Bollinger midline (SMA 20).", units="INR"),
        _column("bb_upper_20_2sd", "float", "Bollinger upper band.", units="INR"),
        _column("bb_lower_20_2sd", "float", "Bollinger lower band.", units="INR"),
        _column("roc_1", "float", "1-session rate of change.", units="percent"),
        _column("roc_5", "float", "5-session rate of change.", units="percent"),
        _column("roc_20", "float", "20-session rate of change.", units="percent"),
        _column("supertrend_10_3", "float", "Supertrend level (10, 3).", units="INR"),
        _column("supertrend_dir_10_3", "int", "Supertrend direction: 1 up, -1 down."),
        _column("realized_vol_20", "float", "20-session realized volatility."),
        _column("realized_vol_60", "float", "60-session realized volatility."),
        _column("beta_to_nifty_60", "float", "60-session beta against NIFTY."),
        _column("max_drawdown_63", "float", "Worst peak-to-trough over 63 sessions."),
        _column("atr_pct", "float", "ATR as a share of price.", units="percent"),
        _column(
            "avg_value_traded_20",
            "float",
            "20-session average traded value.",
            units="INR",
        ),
        _column(
            "liquidity_score",
            "float",
            "Cross-sectional turnover percentile.",
            units="0-1",
        ),
        _column(
            "delivery_pct_20d_avg",
            "float",
            "20-session mean delivery percentage.",
            units="percent",
        ),
        _column("delivery_trend_score", "float", "Direction of the delivery trend."),
    ],
}

STAGE_SURFACE: dict[str, Any] = {
    "surface": "stage",
    "tool": "get_stage_history",
    "store": "control_plane.duckdb + ohlcv.duckdb",
    "tables": [
        "weekly_stock_stage_history (granularity=weekly_governed)",
        "weekly_stage_snapshot (granularity=weekly_legacy)",
        "stage_history (granularity=daily)",
    ],
    "grain": "one observation per symbol per week (or per session, daily)",
    "as_of_support": "EXACT",
    "notes": [
        "Three stores hold stage state with different coverage. "
        "weekly_governed is the default because the legacy weekly store's "
        "coverage typically stops well before the governed store begins.",
        "weekly_stage_snapshot has no exchange column; its rows are keyed by "
        "symbol alone.",
        "Two vocabularies exist in the stores. Every row returned carries "
        "both: stage_label is canonical, stage_label_legacy is nullable.",
    ],
    "columns": [
        _column("observation_date", "date", "Date the observation applies to."),
        _column(
            "stage_label",
            "str",
            "Canonical WeinsteinStage value; always populated.",
        ),
        _column(
            "stage_label_legacy",
            "str|null",
            "Legacy S1..S4/UNDEFINED code. NULL for the four transition "
            "states, which the legacy vocabulary cannot express.",
        ),
        _column(
            "stage_family",
            "str",
            "Structural family (stage_1..stage_4, unknown). A transition "
            "reports the stage it is leaving, so family filters still match.",
        ),
        _column("is_transition", "bool", "True for the four transition states."),
        _column("stage_status", "str", "provisional or locked (governed store)."),
        _column("stage_confidence", "float", "Classifier confidence.", units="0-1"),
        _column("bars_in_stage", "int", "Weeks elapsed in the current stage."),
        _column("stage_entry_date", "date", "When the current stage began."),
        _column("ma30w", "float", "30-week moving average.", units="INR"),
        _column("ma30w_slope_4w", "float", "4-week slope of the 30-week MA."),
        _column("weekly_rs_score", "float", "Weekly relative strength score."),
    ],
    "vocabulary": {
        "canonical": [stage.value for stage in WeinsteinStage],
        "legacy": sorted(LEGACY_STAGE_MAP),
        "mapping": [
            {
                "canonical": stage.value,
                "legacy": legacy_code_for(stage),
                "family": stage_family(stage),
                "is_transition": is_transition(stage),
            }
            for stage in WeinsteinStage
        ],
    },
}

RANK_SURFACE: dict[str, Any] = {
    "surface": "rank",
    "tool": "get_rank_detail / get_rank_history / screen_universe",
    "store": "control_plane.duckdb",
    "tables": ["rank_history", "rank_universe_history"],
    "grain": "one row per (symbol_id, exchange, trade_date, universe_id)",
    "as_of_support": "EXACT",
    "notes": [
        "Read from rank_history rather than the ranked_signals.csv artifact: "
        "the table only holds rows from completed stage attempts, and it is "
        "pinned to the approved rank model version so scores from different "
        "model versions are never mixed.",
        "Factor weights are not uniform. Per docs/reference/ranking_factors.md "
        "relative strength carries 0.38, trend persistence 0.22, sector "
        "strength 0.22, proximity to highs 0.18; volume intensity, momentum "
        "acceleration and delivery are emitted but weighted 0.0.",
    ],
    "columns": [
        _column("trade_date", "date", "Decision date for this ranking."),
        _column("universe_id", "str", "Ranked universe, e.g. NSE_OPERATIONAL."),
        _column("rank_position", "int", "1 is the strongest.", units="ordinal"),
        _column("rank_percentile", "float", "Position as a percentile.", units="0-1"),
        _column("composite_score", "float", "Weighted factor sum.", units="0-100"),
        _column(
            "composite_score_adjusted",
            "float",
            "Composite plus Stage 2 bonuses minus penalties, clipped to 0-100.",
            units="0-100",
        ),
        _column("rs_score", "float", "Relative strength factor score.", units="0-100"),
        _column(
            "volume_score", "float", "Volume intensity factor score.", units="0-100"
        ),
        _column(
            "trend_score", "float", "Trend persistence factor score.", units="0-100"
        ),
        _column(
            "proximity_score", "float", "Proximity-to-52w-high score.", units="0-100"
        ),
        _column(
            "sector_score", "float", "Sector strength factor score.", units="0-100"
        ),
        _column(
            "rank_model_version", "str", "Rank model version this row was scored under."
        ),
        _column("rank_formula_name", "str", "Scoring formula, e.g. weighted_sum."),
        _column("rank_config_hash", "str", "Configuration hash pinning the weights."),
        _column("pipeline_run_id", "str", "Producing pipeline run."),
    ],
}

PATTERN_SURFACE: dict[str, Any] = {
    "surface": "pattern",
    "tool": "get_pattern_detail / get_pattern_history / screen_universe",
    "store": "control_plane.duckdb",
    "tables": ["pattern_history"],
    "grain": "one row per (symbol_id, exchange, trade_date, pattern_family, model_version)",
    "as_of_support": "EXACT",
    "notes": [
        "This is operational pattern history. ADR-0007 pattern-lane evidence is research/shadow-only and is never blended into this surface.",
        "Cross-sectional screening selects the highest-scoring operational pattern per symbol on one model-pinned date.",
    ],
    "columns": [
        _column("trade_date", "date", "Pattern observation date."),
        _column("pattern_family", "str", "Operational setup family."),
        _column("pattern_state", "str", "Lifecycle state of the setup."),
        _column("pattern_score", "float", "Pattern quality score.", units="0-100"),
        _column("setup_quality", "str", "Categorical setup quality."),
        _column(
            "pattern_promotion_state", "str", "Promotion state under pattern policy."
        ),
        _column(
            "pivot_price", "float", "Pattern pivot or breakout level.", units="INR"
        ),
        _column(
            "distance_to_pivot_pct",
            "float",
            "Close distance from pivot.",
            units="percent",
        ),
        _column("breakout_status", "str", "Observed breakout state."),
        _column(
            "breakout_attempt_flag", "bool", "Whether a breakout attempt was observed."
        ),
        _column("pattern_model_version", "str", "Pattern detector/model version."),
        _column("pattern_config_hash", "str", "Configuration content hash."),
        _column("pipeline_run_id", "str", "Producing pipeline run."),
    ],
}

SECTOR_SURFACE: dict[str, Any] = {
    "surface": "sector",
    "tool": "get_sector_overview / get_sector_constituents",
    "store": "control_plane.duckdb + masterdata.db",
    "tables": ["weekly_stock_stage_history", "symbols"],
    "grain": "one row per sector (overview) or per constituent",
    "as_of_support": "EXACT",
    "notes": [
        "Sector structure is aggregated from governed weekly stage "
        "observations, which carry point-in-time sector membership.",
        "Rank-artifact sector RS and rotation quadrant are latest-only and are "
        "deliberately not included, since they cannot be cut off by date.",
        "Two different tables are named sector_earnings_leadership (one in "
        "fundamentals.duckdb, one in ohlcv.duckdb) with different columns, "
        "which is why meta.source always names the file as well as the table.",
    ],
    "columns": [
        _column("sector_name", "str", "Canonical system sector."),
        _column("constituents_observed", "int", "Symbols with a governed observation."),
        _column("stage_1_count", "int", "Constituents in the stage_1 family."),
        _column("stage_2_count", "int", "Constituents in the stage_2 family."),
        _column("stage_3_count", "int", "Constituents in the stage_3 family."),
        _column("stage_4_count", "int", "Constituents in the stage_4 family."),
        _column("stage_2_pct", "float", "Share advancing.", units="percent"),
        _column("in_transition", "int", "Constituents in a transition state."),
        _column("stage_as_of", "date", "Date of the newest observation used."),
    ],
}

FUNDAMENTALS_SURFACE: dict[str, Any] = {
    "surface": "fundamentals",
    "tool": "get_fundamentals",
    "store": "fundamentals/screener_financials.db + fundamentals.duckdb",
    "tables": [
        "screener_financials",
        "screener_market_valuation",
        "screener_company_snapshot",
        "fundamental_scores",
        "fundamental_snapshot",
        "company_growth_features",
    ],
    "grain": "blocks per symbol; financials and growth are per fiscal period",
    "as_of_support": "EXACT",
    "notes": [
        "Cutoffs use the PUBLICATION date, not the fiscal period. A quarter "
        "ending 2025-12-31 is not knowable on 2026-01-05.",
        "screener_financials and company_growth_features carry available_at, "
        "the true publication timestamp. The score and snapshot tables carry "
        "only snapshot_date, the export date, used as a publication proxy and "
        "declared in meta.as_of_basis.",
        "Standalone and consolidated rows live under separate keys and are "
        "never blended. Standalone is the pipeline default.",
    ],
    "columns": [
        _column("report_date", "date", "Fiscal period end."),
        _column("available_at", "date", "When the figure became knowable."),
        _column("statement_basis", "str", "standalone or consolidated."),
        _column(
            "fundamental_score",
            "float",
            "Weighted composite of the sub-scores.",
            units="0-100",
        ),
        _column(
            "quality_score", "float", "ROCE, ROE, margins, Piotroski.", units="0-100"
        ),
        _column(
            "growth_score",
            "float",
            "Sales and profit growth over 3y/5y.",
            units="0-100",
        ),
        _column(
            "balance_sheet_score", "float", "Debt, CFO and FCF health.", units="0-100"
        ),
        _column(
            "valuation_score",
            "float",
            "Sector-relative inverted valuation percentiles.",
            units="0-100",
        ),
        _column(
            "ownership_score",
            "float",
            "Pledge, promoter, DII and FII holding.",
            units="0-100",
        ),
        _column("fundamental_tier", "str", "A, B, C or Reject."),
        _column("hard_red_flag", "bool", "A disqualifying condition was hit."),
        _column("pe", "float", "Price to trailing earnings.", units="ratio"),
        _column("roce", "float", "Return on capital employed.", units="percent"),
        _column("roe", "float", "Return on equity.", units="percent"),
        _column("debt_to_equity", "float", "Leverage.", units="ratio"),
        _column("promoter_holding", "float", "Promoter stake.", units="percent"),
        _column(
            "pledged_pct",
            "float",
            "Pledged share of promoter holding.",
            units="percent",
        ),
        _column(
            "sales_yoy_growth", "float", "Year-on-year sales growth.", units="percent"
        ),
        _column(
            "profit_yoy_growth", "float", "Year-on-year profit growth.", units="percent"
        ),
    ],
}

FUNDAMENTAL_DISCOVERY_SURFACE: dict[str, Any] = {
    "surface": "fundamental_discovery",
    "tool": "get_fundamental_thesis / get_fundamental_thesis_history / screen_fundamental_theses / get_fundamental_lane_overview",
    "store": "fundamentals.duckdb",
    "tables": ["fundamental_thesis_classification", "fundamental_thesis_projection"],
    "grain": "immutable source classification plus one daily projection per listing/source/policy",
    "as_of_support": "EXACT",
    "notes": [
        "Shadow-only operational discovery lane; it does not expose research-screener filings, annual reports, or qualitative claims.",
        "Projection joins classification by symbol, exchange, source_data_hash, taxonomy_version and rule_version.",
        "Blockers and exclusions are evidence, not failed numeric scores. Generic get_fundamentals scores never substitute for thesis state.",
        "Standalone and consolidated classifications remain separate.",
    ],
    "vocabulary": {
        "thesis_families": [family.value for family in FundamentalThesisFamily],
        "taxonomy_version": "Meaning and precedence of thesis-family labels.",
        "rule_version": "Thresholds, blockers and warnings used by all seven evaluations.",
        "admission_policy_version": "Daily structural/context eligibility policy; independent from accounting classification.",
    },
    "columns": [
        _column(
            "classification",
            "object",
            "Immutable accounting/source classification block.",
        ),
        _column(
            "projection",
            "object",
            "Daily structural stage, eligibility, blockers and context.",
        ),
        _column(
            "evaluations",
            "array|null",
            "Evaluate-all records for all seven thesis families.",
        ),
        _column(
            "change",
            "object|null",
            "Previous thesis/source hash when a prior classification exists.",
        ),
        _column(
            "primary_thesis", "str|null", "Highest-precedence passing thesis family."
        ),
        _column(
            "secondary_theses", "array", "Other passing families in precedence order."
        ),
        _column(
            "classification_status",
            "str",
            "Qualification or fail-closed evidence status.",
        ),
        _column("statement_basis", "str", "standalone or consolidated; never blended."),
        _column("source_report_date", "date|null", "Newest fiscal source date."),
        _column(
            "source_available_at", "date|null", "When source evidence became knowable."
        ),
        _column("source_data_hash", "str", "Immutable source-evidence hash."),
        _column("taxonomy_version", "str", "Thesis vocabulary/precedence version."),
        _column("rule_version", "str", "Classification rule-content version."),
        _column("projection_date", "date", "Daily context date."),
        _column("structural_stage", "str", "Point-in-time structural stage."),
        _column("admission_eligible", "bool", "Shadow admission eligibility."),
        _column("blockers", "array", "Fail-closed eligibility blockers."),
        _column("daily_context", "object", "Daily technical/structural context."),
        _column("admission_policy_version", "str", "Daily projection policy version."),
    ],
}

SECTOR_LEADERSHIP_SURFACE: dict[str, Any] = {
    "surface": "sector_leadership",
    "tool": "get_sector_leadership",
    "store": "promoted rank artifacts + fundamentals.duckdb",
    "tables": ["sector_earnings_leadership", "valuation_cycle_features"],
    "grain": "one row per sector in the latest available snapshots",
    "as_of_support": "AS_OF_UNSUPPORTED",
    "notes": [
        "RS, momentum and quadrant artifacts are latest-only; historical requests return no rows."
    ],
    "columns": [
        _column("sector_name", "str", "Sector identity."),
        _column("relative_strength", "object", "Latest relative-strength evidence."),
        _column("momentum", "object", "Latest momentum evidence."),
        _column("quadrant", "str|null", "Latest rotation quadrant."),
        _column("earnings", "object|null", "Latest sector earnings-leadership record."),
        _column("valuation", "object|null", "Latest sector valuation-cycle record."),
    ],
}

GOVERNANCE_SURFACE: dict[str, Any] = {
    "surface": "governance",
    "tool": "get_pipeline_run / get_data_quality_status / get_artifact_lineage / get_data_freshness",
    "store": "control_plane.duckdb",
    "tables": ["pipeline_run", "pipeline_stage_run", "pipeline_artifact", "dq_result"],
    "grain": "run, stage attempt, artifact, DQ result, or freshness surface",
    "as_of_support": "EXACT",
    "notes": [
        "Artifact authority requires the exact producer attempt to be completed; these tools never promote, retry, repair or migrate."
    ],
    "columns": [
        _column("run_id", "str", "Logical pipeline run identity."),
        _column("run_date", "date", "Pipeline decision date."),
        _column("status", "str", "Persisted run, stage or DQ status."),
        _column("artifact_type", "str", "Registered artifact semantic type."),
        _column("content_hash", "str|null", "Artifact content hash."),
        _column("producer_status", "str", "Exact producing stage-attempt status."),
        _column("rule_id", "str", "Data-quality rule identity."),
        _column("severity", "str", "Data-quality severity."),
        _column("latest_date", "date|null", "Latest knowable surface date."),
        _column("age_days", "int|null", "Age relative to requested cutoff or today."),
        _column("freshness_status", "str", "CURRENT, STALE or MISSING."),
    ],
}

LIFECYCLE_SURFACE: dict[str, Any] = {
    "surface": "lifecycle",
    "tool": "get_candidate_status / get_candidate_history / get_investigator_evidence / get_opportunity_episode",
    "store": "control_plane.duckdb",
    "tables": [
        "candidate_episode",
        "candidate_snapshot",
        "candidate_transition",
        "candidate_evidence_observation",
        "candidate_fundamental_observation",
    ],
    "grain": "canonical opportunity episode and append-only observations",
    "as_of_support": "EXACT",
    "notes": [
        "Candidate lifecycle, Investigator evidence and fundamental observations remain distinct blocks; none are execution state."
    ],
    "columns": [
        _column(
            "episode",
            "object",
            "Canonical candidate episode identity and lifecycle bounds.",
        ),
        _column(
            "latest_snapshot", "object|null", "Newest knowable candidate snapshot."
        ),
        _column("snapshots", "array", "Point-in-time lifecycle snapshots."),
        _column("transitions", "array", "Append-only lifecycle transitions."),
        _column(
            "evidence_observations",
            "array",
            "Investigator and other evidence observations.",
        ),
        _column("rank_observations", "array", "Opportunity/rank observations."),
        _column(
            "fundamental_observations",
            "array",
            "Append-only candidate fundamental thesis observations.",
        ),
    ],
}

UNIVERSE_SUMMARY_SURFACE: dict[str, Any] = {
    "surface": "universe_summary",
    "tool": "summarize_universe",
    "store": "composed read over control_plane.duckdb + fundamentals.duckdb",
    "tables": [
        "rank_history",
        "rank_universe_history",
        "weekly_stock_stage_history",
        "pattern_history",
        "fundamental_thesis_projection",
    ],
    "grain": "one aggregate for one pinned, filtered rank cross-section",
    "as_of_support": "EXACT",
    "notes": [
        "Counts are computed from the complete matched set before screen_universe applies its 500-row response cap.",
        "The default scope is full_universe; shortlist remains a separate explicit scope.",
    ],
    "columns": [
        _column("matched_count", "int", "Rows matching every supplied filter."),
        _column("composite_score", "object", "Minimum, maximum, and average score."),
        _column("stage_family_counts", "object", "Counts by structural stage family."),
        _column("sector_counts", "object", "Counts by sector."),
        _column(
            "pattern_family_counts", "object", "Counts by operational pattern family."
        ),
        _column(
            "fundamental_thesis_counts", "object", "Counts by primary thesis family."
        ),
        _column(
            "rejection_reason_counts",
            "object",
            "Counts of recorded rank rejection evidence.",
        ),
        _column(
            "fundamental_blocker_counts",
            "object",
            "Counts of fundamental admission blockers.",
        ),
        _column(
            "evidence_missing_counts",
            "object",
            "Missing stage, pattern, and thesis counts.",
        ),
        _column(
            "market_regime", "str|null", "Regime used by the rank selection policy."
        ),
        _column(
            "regime_freshness_status",
            "str|null",
            "ALIGNED, STALE, or INCOMPLETE regime evidence.",
        ),
    ],
}

SYMBOL_EXPLANATION_SURFACE: dict[str, Any] = {
    "surface": "symbol_explanation",
    "tool": "explain_symbol",
    "store": "composed MCP evidence",
    "tables": [
        "rank_universe_history",
        "rank_history",
        "weekly_stock_stage_history",
        "pattern_history",
        "fundamental_thesis_projection",
        "candidate_episode",
    ],
    "grain": "one evidence explanation per (symbol_id, exchange, cutoff)",
    "as_of_support": "EXACT",
    "notes": [
        "Evidence-only: selection reasons are recorded policy evidence, not a trade recommendation.",
        "A missing block remains explicit and is never filled from a future or unrelated source.",
    ],
    "columns": [
        _column("decision_date", "date|null", "Pinned rank decision date."),
        _column(
            "selection",
            "object",
            "Full-universe and shortlist membership plus recorded reasons.",
        ),
        _column("full_universe_rank", "object|null", "Full analytical rank evidence."),
        _column("shortlist_rank", "object|null", "Actionable shortlist rank evidence."),
        _column("stage", "object|null", "Governed structural stage evidence."),
        _column("pattern", "array|null", "Operational pattern observations."),
        _column(
            "fundamental_thesis",
            "object|null",
            "Classification, projection, evaluations, and blockers.",
        ),
        _column("candidate", "object|null", "Canonical shadow candidate state."),
        _column(
            "evidence_quality",
            "object",
            "Alignment, missing blocks, and stale surfaces.",
        ),
    ],
}

SYMBOL_COMPARISON_SURFACE: dict[str, Any] = {
    "surface": "symbol_comparison",
    "tool": "compare_symbols",
    "store": "composed MCP evidence",
    "tables": [
        "rank_universe_history",
        "weekly_stock_stage_history",
        "pattern_history",
        "fundamental_thesis_projection",
    ],
    "grain": "one compact row per requested listing, maximum ten",
    "as_of_support": "EXACT",
    "notes": [
        "Every row uses the same requested cutoff and exchange; output is evidence-only."
    ],
    "columns": [
        _column("symbol_id", "str", "Canonical ticker."),
        _column("decision_date", "date|null", "Pinned rank decision date."),
        _column("rank_position", "int|null", "Full-universe rank position."),
        _column("composite_score", "float|null", "Full-universe composite score."),
        _column(
            "in_shortlist", "bool", "Whether selected into the same-date shortlist."
        ),
        _column("selection_reasons", "array", "Recorded exclusion/selection evidence."),
        _column("stage_label", "str|null", "Canonical structural stage."),
        _column("pattern_family", "str|null", "Highest-scored operational pattern."),
        _column("primary_thesis", "str|null", "Primary fundamental thesis."),
        _column("fundamental_blockers", "array|null", "Admission blockers."),
        _column("candidate_status", "str|null", "Canonical shadow episode status."),
        _column("missing_blocks", "array", "Evidence blocks absent at the cutoff."),
    ],
}

MARKET_SNAPSHOT_SURFACE: dict[str, Any] = {
    "surface": "market_snapshot",
    "tool": "get_market_snapshot",
    "store": "composed MCP evidence",
    "tables": [
        "rank_universe_history",
        "rank_history",
        "dq_result",
        "pipeline_run",
        "fundamental_thesis_projection",
    ],
    "grain": "one bounded market-orientation response per exchange and cutoff",
    "as_of_support": "EXACT with latest-only sector-leadership block",
    "notes": [
        "Historical requests do not substitute current sector leadership; that block reports AS_OF_UNSUPPORTED.",
        "This is an orientation surface, not a recommendation or execution interface.",
    ],
    "columns": [
        _column("universe", "object", "Complete analytical-universe summary."),
        _column("shortlist", "object", "Actionable-shortlist summary."),
        _column("fundamental_lane", "object", "Thesis and eligibility distribution."),
        _column(
            "sector_leadership", "array", "Latest-only sector leadership evidence."
        ),
        _column("freshness", "array", "Freshness by MCP surface."),
        _column("latest_pipeline_run", "object|null", "Newest knowable pipeline run."),
        _column("data_quality", "object", "DQ evidence attached to that run."),
    ],
}

MARKET_WINNERS_SURFACE: dict[str, Any] = {
    "surface": "market_winners",
    "tool": "get_market_winners",
    "store": "ohlcv.duckdb",
    "tables": ["_catalog_feature_source"],
    "grain": "one listing's realized adjusted-price return per requested window",
    "as_of_support": "EXACT",
    "notes": [
        "This is hindsight market performance, not a strategy backtest or recommendation.",
        "The row reports its actual first and last observed sessions; recently listed securities may cover less than the nominal calendar window.",
    ],
    "columns": [
        _column("symbol_id", "str", "Canonical ticker."),
        _column("exchange", "str", "Listing exchange."),
        _column("start_date", "date", "First observed session in the window."),
        _column("end_date", "date", "Last observed session in the window."),
        _column("start_close", "float", "Adjusted close on the first observed session.", units="INR"),
        _column("end_close", "float", "Adjusted close on the last observed session.", units="INR"),
        _column("observed_sessions", "int", "Distinct sessions contributing to the return."),
        _column("return_pct", "float", "Realized adjusted close-to-close return.", units="percent"),
    ],
}

RANK_PERFORMANCE_SURFACE: dict[str, Any] = {
    "surface": "rank_performance",
    "tool": "get_ranked_winners / get_rank_performance_summary / get_symbol_backtest_history",
    "store": "research.duckdb",
    "tables": ["rank_cohort_performance_trusted"],
    "grain": "one matured ranked-cohort observation or bounded aggregate",
    "as_of_support": "EXACT",
    "notes": [
        "Only 5, 10, 20, and 60 trading-day persisted horizons are supported.",
        "Pending horizons and persisted anomaly rows are excluded; inserted_at is also cut off for historical requests.",
    ],
    "columns": [
        _column("run_date", "date", "Date the symbol entered the ranked cohort."),
        _column("symbol_id", "str", "Ranked ticker."),
        _column("exchange", "str", "Listing exchange."),
        _column("rank_position", "int", "Rank position at cohort formation."),
        _column("composite_score", "float", "Composite score at cohort formation."),
        _column("watchlist_bucket", "str|null", "Persisted publication bucket."),
        _column("sector_name", "str|null", "Persisted sector at cohort formation."),
        _column("return_pct", "float", "Matured forward return for the chosen horizon.", units="percent"),
        _column("matured_at", "date", "Session when the forward horizon matured."),
        _column("occurrence_count", "int", "Cohort occurrences for a deduplicated listing."),
        _column("average_return_pct", "float", "Average return across those occurrences.", units="percent"),
        _column("source_type", "str", "Operational artifact or historical research source."),
        _column("source_run_id", "str|null", "Persisted producing run identifier."),
        _column("inserted_at", "timestamp", "Recording time used for point-in-time cutoff."),
    ],
}

STRATEGY_BACKTEST_SURFACE: dict[str, Any] = {
    "surface": "strategy_backtest",
    "tool": "get_backtest_runs / get_backtest_result / get_backtest_trades",
    "store": "control_plane.duckdb",
    "tables": ["strategy_optimization_run", "strategy_iteration_result", "strategy_backtest_trade"],
    "grain": "one run, iteration/fold metric, or persisted simulated trade",
    "as_of_support": "EXACT",
    "notes": [
        "Backtest evidence is simulated and does not represent broker executions.",
        "Result metrics retain benchmark and fold provenance; trade results retain the selected iteration.",
    ],
    "columns": [
        _column("optimization_run_id", "str", "Persisted optimization/backtest run identifier."),
        _column("strategy_id", "str", "Strategy evaluated by the run."),
        _column("iteration", "int", "Selected trial; -1 is baseline."),
        _column("fold_index", "int", "Walk-forward fold; -1 is aggregate."),
        _column("fitness", "float|null", "Persisted optimizer objective."),
        _column("cagr", "float|null", "Compound annual growth rate.", units="percent"),
        _column("sharpe", "float|null", "Persisted Sharpe ratio."),
        _column("sortino", "float|null", "Persisted Sortino ratio."),
        _column("max_drawdown_pct", "float|null", "Maximum drawdown.", units="percent"),
        _column("win_rate", "float|null", "Winning-trade rate.", units="percent"),
        _column("profit_factor", "float|null", "Gross profit divided by gross loss."),
        _column("total_return_pct", "float|null", "Simulated total return.", units="percent"),
        _column("benchmark_return_pct", "float|null", "Persisted benchmark return.", units="percent"),
        _column("entry_date", "date", "Simulated trade entry date."),
        _column("exit_date", "date|null", "Simulated trade exit date."),
        _column("pnl_pct", "float|null", "Simulated trade return.", units="percent"),
    ],
}

SURFACES: dict[str, dict[str, Any]] = {
    "ohlcv": OHLCV_SURFACE,
    "technicals": TECHNICALS_SURFACE,
    "stage": STAGE_SURFACE,
    "rank": RANK_SURFACE,
    "pattern": PATTERN_SURFACE,
    "sector": SECTOR_SURFACE,
    "sector_leadership": SECTOR_LEADERSHIP_SURFACE,
    "fundamentals": FUNDAMENTALS_SURFACE,
    "fundamental_discovery": FUNDAMENTAL_DISCOVERY_SURFACE,
    "governance": GOVERNANCE_SURFACE,
    "lifecycle": LIFECYCLE_SURFACE,
    "universe_summary": UNIVERSE_SUMMARY_SURFACE,
    "symbol_explanation": SYMBOL_EXPLANATION_SURFACE,
    "symbol_comparison": SYMBOL_COMPARISON_SURFACE,
    "market_snapshot": MARKET_SNAPSHOT_SURFACE,
    "market_winners": MARKET_WINNERS_SURFACE,
    "rank_performance": RANK_PERFORMANCE_SURFACE,
    "strategy_backtest": STRATEGY_BACKTEST_SURFACE,
}

SURFACE_NAMES = tuple(SURFACES)


def describe_schema(surface: str | None = None) -> dict[str, Any]:
    """Return the column dictionary for one surface, or the index of all."""

    if surface is None or not str(surface).strip():
        return {
            "surfaces": [
                {
                    "surface": name,
                    "tool": spec["tool"],
                    "store": spec["store"],
                    "grain": spec["grain"],
                    "as_of_support": spec["as_of_support"],
                    "column_count": len(spec["columns"]),
                }
                for name, spec in SURFACES.items()
            ]
        }

    key = str(surface).strip().lower()
    if key not in SURFACES:
        raise ValueError(
            f"Unknown surface: {surface!r} (expected one of {list(SURFACE_NAMES)})"
        )
    return SURFACES[key]


__all__ = ["SURFACES", "SURFACE_NAMES", "describe_schema"]
