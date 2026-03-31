"""Rule-based DQ engine for staged pipeline validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd

from analytics.registry import RegistryStore
from core.contracts import DataQualityCriticalError, StageContext, StageResult


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
        query = """
            SELECT COUNT(*)
            FROM _catalog
            WHERE symbol_id IS NULL
               OR exchange IS NULL
               OR timestamp IS NULL
               OR open IS NULL
               OR high IS NULL
               OR low IS NULL
               OR close IS NULL
               OR volume IS NULL
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
        query = """
            SELECT COUNT(*)
            FROM _catalog
            WHERE high < GREATEST(open, close)
               OR low > LEAST(open, close)
               OR high < low
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
