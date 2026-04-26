"""Rule-based DQ engine for staged pipeline validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd

from ai_trading_system.domains.ingest.trust import load_data_trust_summary
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import DataQualityCriticalError, StageContext, StageResult


@dataclass
class DQRuleFailure:
    """Represents a single DQ evaluation outcome."""

    rule_id: str
    severity: str
    status: str
    failed_count: int
    message: str
    sample_uri: str | None = None


class DataQualityEngine:
    """Evaluates configured DQ rules and blocks downstream work on critical failures."""

    def __init__(self, registry: RegistryStore):
        self.registry = registry

    def evaluate(self, context: StageContext, result: StageResult) -> List[DQRuleFailure]:
        failures: List[DQRuleFailure] = []
        for rule in self.registry.get_rules_for_stage(context.stage_name):
            outcome = self._evaluate_rule(rule, context, result)
            self.registry.record_dq_result(
                run_id=context.run_id,
                stage_name=context.stage_name,
                rule_id=outcome.rule_id,
                severity=outcome.severity,
                status=outcome.status,
                failed_count=outcome.failed_count,
                message=outcome.message,
                sample_uri=outcome.sample_uri,
            )
            failures.append(outcome)

        critical_failures = [
            item for item in failures if item.severity == "critical" and item.status == "failed"
        ]
        if critical_failures:
            joined = "; ".join(f"{item.rule_id}: {item.message}" for item in critical_failures)
            raise DataQualityCriticalError(joined)
        return failures

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

        date_threshold_failed = unresolved_date_count > max_unresolved_dates
        symbol_threshold_failed = unresolved_symbol_count > max_unresolved_symbols
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

    def _rule_features_trust_quarantine_clear(
        self, context: StageContext, result: StageResult, severity: str
    ) -> DQRuleFailure:
        if not self._table_exists(context.db_path, "_catalog_quarantine"):
            return self._make_result(
                "features_trust_quarantine_clear",
                severity,
                0,
                "No quarantine table present; treating current trust window as clear.",
            )
        raw_lookback = context.params.get("dq_features_quarantine_lookback_days", 7)
        lookback_days = 7 if raw_lookback in (None, "") else int(raw_lookback)
        query = f"""
            SELECT
                COUNT(*) AS quarantined_rows,
                COUNT(DISTINCT symbol_id) AS quarantined_symbols
            FROM _catalog_quarantine
            WHERE exchange = 'NSE'
              AND status = 'active'
              AND trade_date >= DATE {self._sql_literal(context.run_date)} - INTERVAL {lookback_days} DAY
        """
        stats_row = self._fetchone(context.db_path, query)
        quarantined_rows = int(stats_row[0] or 0) if stats_row else 0
        quarantined_symbols = int(stats_row[1] or 0) if stats_row else 0

        trust_summary = load_data_trust_summary(context.db_path, run_date=context.run_date)
        latest_total_rows = int(
            ((trust_summary.get("latest_provider_stats") or {}).get("total_rows", 0) or 0)
        )
        quarantined_symbol_ratio_pct = (
            (quarantined_symbols * 100.0 / latest_total_rows)
            if latest_total_rows > 0
            else (100.0 if quarantined_symbols > 0 else 0.0)
        )
        max_quarantined_symbols = int(context.params.get("dq_features_max_quarantined_symbols", 10) or 10)
        max_quarantined_symbol_ratio_pct = float(
            context.params.get("dq_features_max_quarantined_symbol_ratio_pct", 1.0) or 1.0
        )

        failed_count = int(
            quarantined_symbols > max_quarantined_symbols
            or quarantined_symbol_ratio_pct > max_quarantined_symbol_ratio_pct
        )
        message = (
            "No active quarantine rows remain in the current trust window."
            if quarantined_rows == 0
            else (
                "Active quarantine rows remain in the current trust window "
                f"(rows={quarantined_rows}, symbols={quarantined_symbols}, "
                f"ratio={quarantined_symbol_ratio_pct:.2f}% "
                f"max_symbols={max_quarantined_symbols}, "
                f"max_ratio={max_quarantined_symbol_ratio_pct:.2f}%)."
            )
        )
        return self._make_result(
            "features_trust_quarantine_clear",
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
