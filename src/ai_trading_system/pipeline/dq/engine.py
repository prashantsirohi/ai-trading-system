"""Rule-based DQ engine for staged pipeline validation."""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd

from ai_trading_system.domains.ingest.trust import (
    effective_symbol_threshold,
    load_critical_symbol_universe,
    load_data_trust_summary,
)
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import (
    DataQualityCriticalError,
    DataQualityRepairableError,
    StageContext,
    StageResult,
)


# Hard-floor rules that are NEVER relaxed even when dq_mode=relaxed.
# These represent data integrity issues that downstream stages cannot work around.
HARD_FLOOR_RULES: frozenset[str] = frozenset({
    # Ingest data-integrity floor
    "ingest_catalog_not_empty",
    "ingest_required_fields_not_null",
    "ingest_ohlc_consistency",
    "ingest_duplicate_ohlcv_key",
    # Features stage produced no output → downstream cannot run
    "features_snapshot_created",
    "features_registry_not_empty",
    # Rank artifact missing is an internal logic failure, not an external DQ issue
    "rank_artifact_not_empty",
})


@dataclass
class DQRuleFailure:
    """Represents a single DQ evaluation outcome.

    Bands (set by evaluators or inferred at evaluate() time):
        green           — failed_count == 0
        amber           — non-critical / informational; never blocks
        red_repairable  — critical but external/fixable; relaxable
        red_block       — critical hard-floor; never relaxed
    """

    rule_id: str
    severity: str
    status: str
    failed_count: int
    message: str
    sample_uri: str | None = None
    band: str = ""
    relaxed_from: str | None = None


class DataQualityEngine:
    """Evaluates configured DQ rules and blocks downstream work on critical failures."""

    def __init__(self, registry: RegistryStore):
        self.registry = registry

    def evaluate(self, context: StageContext, result: StageResult) -> List[DQRuleFailure]:
        dq_mode = str(context.params.get("dq_mode", "relaxed") or "relaxed").lower()

        failures: List[DQRuleFailure] = []
        for rule in self.registry.get_rules_for_stage(context.stage_name):
            outcome = self._evaluate_rule(rule, context, result)
            outcome = self._apply_relaxation(outcome, dq_mode=dq_mode)
            self.registry.record_dq_result(
                run_id=context.run_id,
                stage_name=context.stage_name,
                rule_id=outcome.rule_id,
                severity=outcome.severity,
                status=outcome.status,
                failed_count=outcome.failed_count,
                message=outcome.message,
                sample_uri=outcome.sample_uri,
                band=outcome.band or None,
                relaxed_from=outcome.relaxed_from,
            )
            failures.append(outcome)

        red_block_failures = [item for item in failures if item.band == "red_block" and item.status == "failed"]
        if red_block_failures:
            joined = "; ".join(f"{item.rule_id}: {item.message}" for item in red_block_failures)
            raise DataQualityCriticalError(joined)

        red_repairable_failures = [
            item for item in failures if item.band == "red_repairable" and item.status == "failed"
        ]
        if red_repairable_failures:
            joined = "; ".join(f"{item.rule_id}: {item.message}" for item in red_repairable_failures)
            raise DataQualityRepairableError(joined)
        return failures

    def _apply_relaxation(self, outcome: DQRuleFailure, *, dq_mode: str) -> DQRuleFailure:
        """Downgrade red_repairable → amber when dq_mode == relaxed.

        Hard-floor rules are never relaxed. Passes are untouched.
        """
        if outcome.status != "failed":
            return outcome
        if outcome.rule_id in HARD_FLOOR_RULES:
            outcome.band = "red_block"
            return outcome
        if not outcome.band:
            # Default: critical severity → red_repairable; non-critical → amber
            outcome.band = "red_repairable" if outcome.severity == "critical" else "amber"
        if outcome.band == "red_repairable" and dq_mode == "relaxed":
            outcome.relaxed_from = "red_repairable"
            outcome.band = "amber"
            outcome.message = f"[RELAXED] {outcome.message}"
        return outcome

    def _evaluate_rule(
        self,
        rule: Dict[str, str],
        context: StageContext,
        result: StageResult,
    ) -> DQRuleFailure:
        rule_sql = rule.get("rule_sql")
        if rule_sql:
            return self._evaluate_sql_rule(rule, context, result)
        evaluator = getattr(self, f"_rule_{rule['rule_id']}")
        return evaluator(context, result, rule["severity"])

    def _evaluate_sql_rule(
        self,
        rule: Dict[str, str],
        context: StageContext,
        result: StageResult,
    ) -> DQRuleFailure:
        sql = self._render_rule_sql(rule["rule_sql"], context, result)
        failed_count = self._scalar(context.db_path, sql)
        sample_uri = None
        if context.stage_name == "rank":
            artifact = context.artifact_for("rank", "ranked_signals")
            sample_uri = artifact.uri if artifact else None
        return self._make_result(
            rule["rule_id"],
            rule["severity"],
            failed_count,
            rule["description"] if failed_count else f"{rule['rule_id']} passed.",
            sample_uri=sample_uri,
        )

    def _rule_ingest_catalog_not_empty(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        failed_count = 0 if self._count_catalog_rows(context.db_path) > 0 else 1
        return self._make_result(
            "ingest_catalog_not_empty",
            severity,
            failed_count,
            "OHLCV catalog contains rows." if failed_count == 0 else "OHLCV catalog is empty after ingest.",
        )

    def _rule_ingest_required_fields_not_null(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        catalog_run_filter = self._catalog_run_filter(context.db_path, context.run_id)
        query = f"""
            SELECT COUNT(*)
            FROM _catalog
            WHERE {catalog_run_filter}
              AND (
                    symbol_id IS NULL
                 OR exchange IS NULL
                 OR timestamp IS NULL
                 OR open IS NULL
                 OR high IS NULL
                 OR low IS NULL
                 OR close IS NULL
                 OR volume IS NULL
              )
        """
        failed_count = self._scalar(context.db_path, query)
        return self._make_result(
            "ingest_required_fields_not_null",
            severity,
            failed_count,
            "Required ingest columns are populated."
            if failed_count == 0
            else "Required OHLCV fields contain null values.",
        )

    def _rule_ingest_ohlc_consistency(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        catalog_run_filter = self._catalog_run_filter(context.db_path, context.run_id)
        query = f"""
            SELECT COUNT(*)
            FROM _catalog
            WHERE {catalog_run_filter}
              AND (
                    high < GREATEST(open, close)
                 OR low > LEAST(open, close)
                 OR high < low
              )
        """
        failed_count = self._scalar(context.db_path, query)
        return self._make_result(
            "ingest_ohlc_consistency",
            severity,
            failed_count,
            "OHLC values are internally consistent."
            if failed_count == 0
            else "Found OHLC rows with invalid high/low relationships.",
        )

    def _rule_features_snapshot_created(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        snapshot_id = result.metadata.get("snapshot_id")
        failed_count = 0 if snapshot_id is not None else 1
        return self._make_result(
            "features_snapshot_created",
            severity,
            failed_count,
            "Feature snapshot reference created."
            if failed_count == 0
            else "Features stage did not produce a snapshot_id.",
        )

    def _rule_features_registry_not_empty(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        feature_rows = int(result.metadata.get("feature_rows", 0) or 0)
        failed_count = 0 if feature_rows > 0 else 1
        return self._make_result(
            "features_registry_not_empty",
            severity,
            failed_count,
            "Feature stage reported computed rows."
            if failed_count == 0
            else "Features stage reported zero computed rows.",
        )

    def _rule_ingest_recent_universe_price_jump_anomaly(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        lookback_days = int(context.params.get("dq_jump_lookback_days", 7) or 7)
        min_symbol_count = int(context.params.get("dq_jump_min_symbols", 500) or 500)
        pct_gt30_threshold = float(context.params.get("dq_jump_pct_gt30_threshold", 20.0) or 20.0)
        pct_gt50_threshold = float(context.params.get("dq_jump_pct_gt50_threshold", 10.0) or 10.0)
        median_abs_threshold = float(context.params.get("dq_jump_median_abs_pct_threshold", 15.0) or 15.0)

        query = f"""
            WITH ordered AS (
                SELECT
                    symbol_id,
                    exchange,
                    CAST(timestamp AS DATE) AS trade_date,
                    close,
                    LAG(close) OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY timestamp
                    ) AS prev_close
                FROM _catalog
                WHERE exchange = 'NSE'
            ),
            moves AS (
                SELECT
                    trade_date,
                    ABS(((close / NULLIF(prev_close, 0)) - 1) * 100.0) AS abs_pct_change
                FROM ordered
                WHERE prev_close IS NOT NULL
                  AND close IS NOT NULL
                  AND trade_date >= DATE {self._sql_literal(context.run_date)} - INTERVAL {lookback_days} DAY
            ),
            daily AS (
                SELECT
                    trade_date,
                    COUNT(*) AS symbols_with_prev,
                    MEDIAN(abs_pct_change) AS median_abs_pct,
                    SUM(CASE WHEN abs_pct_change > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_gt30,
                    SUM(CASE WHEN abs_pct_change > 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_gt50,
                    SUM(CASE WHEN abs_pct_change > 100 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_gt100
                FROM moves
                GROUP BY trade_date
            )
            SELECT
                trade_date,
                symbols_with_prev,
                median_abs_pct,
                pct_gt30,
                pct_gt50,
                pct_gt100
            FROM daily
            WHERE symbols_with_prev >= {min_symbol_count}
              AND (
                    pct_gt30 >= {pct_gt30_threshold}
                 OR pct_gt50 >= {pct_gt50_threshold}
                 OR median_abs_pct >= {median_abs_threshold}
              )
            ORDER BY trade_date DESC
        """
        offending = self._fetchdf(context.db_path, query)
        failed_count = len(offending)
        if failed_count == 0:
            return self._make_result(
                "ingest_recent_universe_price_jump_anomaly",
                severity,
                0,
                "Recent universe-wide price movement looks within expected bounds.",
            )

        preview_rows: list[str] = []
        for _, row in offending.head(5).iterrows():
            preview_rows.append(
                f"{str(row['trade_date'])[:10]}: median={float(row['median_abs_pct']):.2f}%"
                f", >30%={float(row['pct_gt30']):.2f}%"
                f", >50%={float(row['pct_gt50']):.2f}%"
            )
        message = (
            "Detected recent universe-wide price jump anomaly on "
            + "; ".join(preview_rows)
            + "."
        )
        return self._make_result(
            "ingest_recent_universe_price_jump_anomaly",
            severity,
            failed_count,
            message,
        )

    def _rule_ingest_provider_coverage_low(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        trust_summary = result.metadata.get("trust_summary") or load_data_trust_summary(
            context.db_path,
            run_date=context.run_date,
        )
        if trust_summary.get("status") in {"missing", "legacy"}:
            return self._make_result(
                "ingest_provider_coverage_low",
                severity,
                0,
                "Provider coverage check skipped because trust metadata is not yet available for this catalog schema.",
            )
        latest_stats = trust_summary.get("latest_provider_stats", {}) or {}
        total_rows = int(latest_stats.get("total_rows", 0) or 0)
        primary_rows = int(latest_stats.get("primary_rows", 0) or 0)
        fallback_rows = int(latest_stats.get("fallback_rows", 0) or 0)
        unknown_rows = int(latest_stats.get("unknown_rows", 0) or 0)
        min_rows = int(context.params.get("dq_provider_min_rows", 100) or 100)
        min_primary_pct = float(context.params.get("dq_min_primary_provider_pct", 75.0) or 75.0)
        max_fallback_pct = float(context.params.get("dq_max_fallback_provider_pct", 25.0) or 25.0)
        max_unknown_pct = float(context.params.get("dq_max_unknown_provider_pct", 0.0) or 0.0)
        primary_pct = (primary_rows * 100.0 / total_rows) if total_rows else 0.0
        fallback_pct = (fallback_rows * 100.0 / total_rows) if total_rows else 0.0
        unknown_pct = (unknown_rows * 100.0 / total_rows) if total_rows else 0.0
        coverage_failed = total_rows >= min_rows and (
            primary_pct < min_primary_pct or fallback_pct > max_fallback_pct
        )
        unknown_failed = total_rows > 0 and unknown_pct > max_unknown_pct
        failed = int(coverage_failed or unknown_failed)
        message = (
            f"Latest provider coverage primary={primary_pct:.2f}% fallback={fallback_pct:.2f}% "
            f"unknown={unknown_pct:.2f}% rows={total_rows}."
        )
        return self._make_result(
            "ingest_provider_coverage_low",
            severity,
            failed,
            message if failed else f"{message} Coverage within thresholds.",
        )

    def _rule_ingest_unresolved_dates_present(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        unresolved_dates = list(result.metadata.get("unresolved_dates") or [])
        unresolved_date_count = int(len(unresolved_dates))
        unresolved_symbol_date_count = int(result.metadata.get("unresolved_symbol_date_count", 0) or 0)
        unresolved_symbol_count = int(
            result.metadata.get("unresolved_symbol_count", unresolved_symbol_date_count) or 0
        )
        active_eligible_symbol_count = int(result.metadata.get("active_eligible_symbol_count", 0) or 0)
        unresolved_symbol_ratio_pct = (
            (unresolved_symbol_count * 100.0 / active_eligible_symbol_count)
            if active_eligible_symbol_count > 0
            else 0.0
        )

        # Backward-compatible date tolerance (legacy: dq_allowed_unresolved_dates), with a safer
        # default that tolerates a single-date micro-gap while still blocking broad unresolved windows.
        allowed = int(context.params.get("dq_allowed_unresolved_dates", 0) or 0)
        max_unresolved_dates = int(context.params.get("dq_max_unresolved_dates", max(1, allowed)) or max(1, allowed))
        # Backward-compatible param fallback:
        # `dq_max_unresolved_symbol_dates` historically modeled breadth, so keep it as
        # default source until `dq_max_unresolved_symbols` is explicitly provided.
        max_unresolved_symbols = int(
            context.params.get(
                "dq_max_unresolved_symbols",
                context.params.get("dq_max_unresolved_symbol_dates", 10),
            )
            or 10
        )
        max_unresolved_symbol_ratio_pct = float(context.params.get("dq_max_unresolved_symbol_ratio_pct", 1.0) or 1.0)
        effective_max_unresolved_symbols = effective_symbol_threshold(
            base_threshold=max_unresolved_symbols,
            ratio_threshold=(max_unresolved_symbol_ratio_pct / 100.0),
            eligible_total=active_eligible_symbol_count,
        )

        date_threshold_failed = unresolved_date_count > max_unresolved_dates
        symbol_threshold_failed = unresolved_symbol_count > effective_max_unresolved_symbols
        ratio_threshold_failed = unresolved_symbol_ratio_pct > max_unresolved_symbol_ratio_pct

        # Breadth is the primary safety signal. A multi-date gap affecting only a tiny,
        # contained symbol set should degrade the run, but not hard-block it.
        failed_count = int(symbol_threshold_failed or ratio_threshold_failed)
        if unresolved_dates:
            message = (
                f"Unresolved trade dates remain quarantined: {', '.join(unresolved_dates[:5])}. "
                f"unresolved_symbol_dates={unresolved_symbol_count} "
                f"unresolved_symbol_date_pairs={unresolved_symbol_date_count} "
                f"eligible_symbols={active_eligible_symbol_count} "
                f"ratio={unresolved_symbol_ratio_pct:.2f}% "
                f"(max_dates={max_unresolved_dates}, "
                f"max_symbols={max_unresolved_symbols}, "
                f"effective_max_symbols={effective_max_unresolved_symbols}, "
                f"max_ratio={max_unresolved_symbol_ratio_pct:.2f}%)."
            )
            if date_threshold_failed and not failed_count:
                message += " Date threshold exceeded, but symbol breadth remains within tolerated limits."
        else:
            message = "No unresolved dates remain after ingest."
        return self._make_result(
            "ingest_unresolved_dates_present",
            severity,
            failed_count,
            message,
        )

    def _rule_ingest_segment_distribution_drift(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        if not self._table_exists(context.db_path, "_catalog"):
            return self._make_result(
                "ingest_segment_distribution_drift",
                severity,
                0,
                "Catalog table not present; skipping segment-drift check.",
            )
        column_rows = self._fetchall(
            context.db_path,
            "SELECT column_name FROM information_schema.columns WHERE table_name = '_catalog'",
        )
        catalog_columns = {str(row[0]) for row in column_rows}
        if "trading_segment" not in catalog_columns:
            return self._make_result(
                "ingest_segment_distribution_drift",
                severity,
                0,
                "trading_segment column not yet populated; skipping segment-drift check.",
            )
        max_jump_pct = float(context.params.get("dq_segment_drift_max_jump_pct", 5.0) or 5.0)
        lookback_days = int(context.params.get("dq_segment_drift_lookback_days", 7) or 7)
        baseline_lookback = int(context.params.get("dq_segment_drift_baseline_days", 30) or 30)
        run_date_lit = self._sql_literal(context.run_date)
        rows = self._fetchall(
            context.db_path,
            f"""
            WITH recent AS (
                SELECT
                    SUM(CASE WHEN COALESCE(trading_segment, 'unknown') <> 'regular' THEN 1 ELSE 0 END) * 100.0 /
                        NULLIF(COUNT(*), 0) AS non_regular_pct
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND CAST(timestamp AS DATE) >= DATE {run_date_lit} - INTERVAL {lookback_days} DAY
                  AND CAST(timestamp AS DATE) <= DATE {run_date_lit}
            ),
            baseline AS (
                SELECT
                    SUM(CASE WHEN COALESCE(trading_segment, 'unknown') <> 'regular' THEN 1 ELSE 0 END) * 100.0 /
                        NULLIF(COUNT(*), 0) AS non_regular_pct
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND CAST(timestamp AS DATE) >= DATE {run_date_lit} - INTERVAL {baseline_lookback} DAY
                  AND CAST(timestamp AS DATE) <  DATE {run_date_lit} - INTERVAL {lookback_days} DAY
            )
            SELECT recent.non_regular_pct, baseline.non_regular_pct
            FROM recent, baseline
            """,
        )
        if not rows or rows[0][0] is None or rows[0][1] is None:
            return self._make_result(
                "ingest_segment_distribution_drift",
                severity,
                0,
                "Insufficient data to evaluate segment drift.",
            )
        recent_pct = float(rows[0][0])
        baseline_pct = float(rows[0][1])
        delta = recent_pct - baseline_pct
        failed_count = int(delta > max_jump_pct)
        message = (
            f"Segment distribution stable (recent_non_regular_pct={recent_pct:.2f}%, "
            f"baseline_non_regular_pct={baseline_pct:.2f}%, delta={delta:+.2f}%, "
            f"max_jump_pct={max_jump_pct:.2f}%)."
            if failed_count == 0
            else (
                f"Non-regular segment share jumped recent_non_regular_pct={recent_pct:.2f}% "
                f"vs baseline_non_regular_pct={baseline_pct:.2f}% (delta={delta:+.2f}%, "
                f"max_jump_pct={max_jump_pct:.2f}%). Likely an NSE reclassification batch — "
                f"review _catalog series distribution."
            )
        )
        return self._make_result(
            "ingest_segment_distribution_drift",
            severity,
            failed_count,
            message,
        )

    def _rule_ingest_latest_trade_date_quarantine_clear(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        return self._evaluate_active_quarantine_rule(
            context=context,
            severity=severity,
            rule_id="ingest_latest_trade_date_quarantine_clear",
            message_prefix="after ingest",
            include_trade_dates=True,
        )

    def _rule_features_trust_quarantine_clear(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        return self._evaluate_active_quarantine_rule(
            context=context,
            severity=severity,
            rule_id="features_trust_quarantine_clear",
            message_prefix="in the current trust window",
            include_trade_dates=False,
        )

    def _evaluate_active_quarantine_rule(
        self,
        *,
        context: StageContext,
        severity: str,
        rule_id: str,
        message_prefix: str,
        include_trade_dates: bool,
    ) -> DQRuleFailure:
        if not self._table_exists(context.db_path, "_catalog_quarantine"):
            return self._make_result(
                rule_id,
                severity,
                0,
                f"No quarantine table present; treating {message_prefix} as clear.",
            )
        raw_lookback = context.params.get("dq_features_quarantine_lookback_days", 7)
        lookback_days = 7 if raw_lookback in (None, "") else int(raw_lookback)
        trust_summary = load_data_trust_summary(context.db_path, run_date=context.run_date)
        latest_trade_date = str((trust_summary.get("latest_provider_stats") or {}).get("trade_date") or context.run_date)
        active_rows = self._fetchall(
            context.db_path,
            f"""
            SELECT CAST(trade_date AS DATE) AS trade_date, symbol_id
            FROM _catalog_quarantine
            WHERE exchange = 'NSE'
              AND status = 'active'
              AND trade_date >= DATE {self._sql_literal(context.run_date)} - INTERVAL {lookback_days} DAY
            """,
        )
        quarantined_rows = int(len(active_rows))
        quarantined_symbols = {
            str(symbol_id).strip().upper()
            for _, symbol_id in active_rows
            if str(symbol_id).strip()
        }
        critical_symbols = load_critical_symbol_universe(
            context.db_path,
            run_date=latest_trade_date,
        )
        if not critical_symbols and quarantined_symbols:
            critical_symbols = set(quarantined_symbols)

        latest_critical_symbols = {
            str(symbol_id).strip().upper()
            for trade_date, symbol_id in active_rows
            if str(trade_date) == latest_trade_date
            and str(symbol_id).strip().upper() in critical_symbols
        }
        latest_noncritical_symbols = {
            str(symbol_id).strip().upper()
            for trade_date, symbol_id in active_rows
            if str(trade_date) == latest_trade_date
            and str(symbol_id).strip().upper() not in critical_symbols
        }
        critical_universe_count = int(len(critical_symbols))
        critical_quarantined_symbol_count = int(len(latest_critical_symbols))
        critical_quarantined_symbol_ratio_pct = (
            (critical_quarantined_symbol_count * 100.0 / critical_universe_count)
            if critical_universe_count > 0
            else (100.0 if critical_quarantined_symbol_count > 0 else 0.0)
        )
        max_quarantined_symbols = int(context.params.get("dq_features_max_quarantined_symbols", 10) or 10)
        max_quarantined_symbol_ratio_pct = float(
            context.params.get("dq_features_max_quarantined_symbol_ratio_pct", 1.0) or 1.0
        )
        effective_max_quarantined_symbols = effective_symbol_threshold(
            base_threshold=max_quarantined_symbols,
            ratio_threshold=(max_quarantined_symbol_ratio_pct / 100.0),
            eligible_total=critical_universe_count,
        )

        failed_count = int(
            critical_quarantined_symbol_count > effective_max_quarantined_symbols
            or critical_quarantined_symbol_ratio_pct > max_quarantined_symbol_ratio_pct
        )
        active_trade_dates = sorted(
            {
                str(trade_date)
                for trade_date, _symbol_id in active_rows
                if trade_date is not None
            }
        )
        trade_dates_detail = ""
        if include_trade_dates and active_trade_dates:
            trade_dates_detail = f"trade_dates={', '.join(active_trade_dates[:10])}, "
        message = (
            f"No active quarantine rows remain {message_prefix}."
            if quarantined_rows == 0
            else (
                f"Active quarantine rows remain {message_prefix} "
                f"({trade_dates_detail}rows={quarantined_rows}, symbols={len(quarantined_symbols)}, "
                f"latest_trade_date={latest_trade_date}, "
                f"latest_critical_symbols={critical_quarantined_symbol_count}, "
                f"latest_noncritical_symbols={len(latest_noncritical_symbols)}, "
                f"critical_universe={critical_universe_count}, "
                f"critical_ratio={critical_quarantined_symbol_ratio_pct:.2f}% "
                f"max_symbols={max_quarantined_symbols}, "
                f"effective_max_symbols={effective_max_quarantined_symbols}, "
                f"max_ratio={max_quarantined_symbol_ratio_pct:.2f}%)."
            )
        )
        return self._make_result(
            rule_id,
            severity,
            failed_count,
            message,
        )

    def _rule_rank_artifact_not_empty(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        artifact = context.require_artifact("rank", "ranked_signals")
        row_count = int(artifact.row_count or 0)
        failed_count = 0 if row_count > 0 else 1
        return self._make_result(
            "rank_artifact_not_empty",
            severity,
            failed_count,
            "Ranking artifact contains rows."
            if failed_count == 0
            else "Ranking artifact is empty.",
            sample_uri=artifact.uri,
        )

    def _rule_rank_required_columns_present(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        artifact = context.require_artifact("rank", "ranked_signals")
        df = pd.read_csv(Path(artifact.uri))
        required = {"symbol_id", "composite_score"}
        missing = sorted(required - set(df.columns))
        failed_count = len(missing)
        return self._make_result(
            "rank_required_columns_present",
            severity,
            failed_count,
            "Ranking artifact has required columns."
            if failed_count == 0
            else f"Missing required rank columns: {', '.join(missing)}",
            sample_uri=artifact.uri,
        )

    def _render_rule_sql(self, template: str, context: StageContext, result: StageResult) -> str:
        """Render a DQ SQL template with stage-aware placeholders."""
        rank_artifact = context.artifact_for("rank", "ranked_signals")
        replacements = {
            "rank_artifact_uri": self._sql_literal(rank_artifact.uri if rank_artifact else ""),
            "feature_rows": str(int(result.metadata.get("feature_rows", 0) or 0)),
            "snapshot_id": "NULL"
            if result.metadata.get("snapshot_id") is None
            else str(int(result.metadata["snapshot_id"])),
            "run_date_literal": self._sql_literal(context.run_date),
            "run_id_literal": self._sql_literal(context.run_id),
            "catalog_run_filter": self._catalog_run_filter(context.db_path, context.run_id),
            "expected_rank_min_rows": str(
                int(
                    context.params.get(
                        "expected_rank_min_rows",
                        2 if context.params.get("canary", False) else 5,
                    )
                )
            ),
        }
        return template.format(**replacements)

    def _count_catalog_rows(self, db_path: Path) -> int:
        return self._scalar(db_path, "SELECT COUNT(*) FROM _catalog")

    def _scalar(self, db_path: Path, query: str) -> int:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            row = conn.execute(query).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def _fetchdf(self, db_path: Path, query: str) -> pd.DataFrame:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            return conn.execute(query).fetchdf()
        finally:
            conn.close()

    def _fetchone(self, db_path: Path, query: str) -> tuple | None:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            return conn.execute(query).fetchone()
        finally:
            conn.close()

    def _fetchall(self, db_path: Path, query: str) -> list[tuple]:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            return conn.execute(query).fetchall()
        finally:
            conn.close()

    def _table_exists(self, db_path: Path, table_name: str) -> bool:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = ?
                """,
                [table_name],
            ).fetchone()
            return bool(row and int(row[0]) > 0)
        finally:
            conn.close()

    def _column_exists(self, db_path: Path, table_name: str, column_name: str) -> bool:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = ?
                  AND column_name = ?
                """,
                [table_name, column_name],
            ).fetchone()
            return bool(row and int(row[0]) > 0)
        finally:
            conn.close()

    def _catalog_run_filter(self, db_path: Path, run_id: str) -> str:
        if self._column_exists(db_path, "_catalog", "ingest_run_id"):
            return f"COALESCE(ingest_run_id, '') = {self._sql_literal(run_id)}"
        return "TRUE"

    def _sql_literal(self, value: str) -> str:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    def _make_result(
        self,
        rule_id: str,
        severity: str,
        failed_count: int,
        message: str,
        sample_uri: str | None = None,
    ) -> DQRuleFailure:
        return DQRuleFailure(
            rule_id=rule_id,
            severity=severity,
            status="passed" if failed_count == 0 else "failed",
            failed_count=failed_count,
            message=message,
            sample_uri=sample_uri,
        )

    def _rule_rank_delivery_pct_range(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        rank_artifact = context.artifact_for("rank", "ranked_signals")
        if not rank_artifact or not rank_artifact.uri:
            return self._make_result("rank_delivery_pct_range", severity, 0, "No ranked_signals artifact")
        try:
            import pandas as pd
            df = pd.read_csv(rank_artifact.uri)
            if "delivery_pct" not in df.columns:
                return self._make_result("rank_delivery_pct_range", severity, 0, "delivery_pct column not in ranked_signals")
            invalid = df[(df["delivery_pct"].notna()) & ((df["delivery_pct"] < 0) | (df["delivery_pct"] > 100))]
            failed_count = len(invalid)
            return self._make_result(
                "rank_delivery_pct_range",
                severity,
                failed_count,
                "Delivery percentage values are valid." if failed_count == 0 else f"Found {failed_count} rows with invalid delivery_pct."
            )
        except Exception as exc:
            return self._make_result("rank_delivery_pct_range", severity, 0, f"Error evaluating rule: {exc}")

    def _rule_rank_sector_coverage_threshold(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        rank_artifact = context.artifact_for("rank", "ranked_signals")
        if not rank_artifact or not rank_artifact.uri:
            return self._make_result("rank_sector_coverage_threshold", severity, 0, "No ranked_signals artifact")
        try:
            import pandas as pd
            df = pd.read_csv(rank_artifact.uri)
            if "sector_name" not in df.columns:
                return self._make_result("rank_sector_coverage_threshold", severity, 0, "sector_name column not in ranked_signals")
            total = len(df)
            missing = df["sector_name"].isna().sum()
            missing_pct = (missing / total) if total > 0 else 0
            failed_count = 1 if missing_pct > 0.10 else 0
            return self._make_result(
                "rank_sector_coverage_threshold",
                severity,
                failed_count,
                "Sector coverage is adequate." if failed_count == 0 else f"Sector coverage gap: {missing_pct*100:.1f}% (>10% threshold)"
            )
        except Exception as exc:
            return self._make_result("rank_sector_coverage_threshold", severity, 0, f"Error evaluating rule: {exc}")
