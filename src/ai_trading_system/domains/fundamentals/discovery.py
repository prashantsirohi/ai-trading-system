"""Point-in-time, policy-versioned fundamental thesis discovery."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any, Iterable

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import (
    FUNDAMENTAL_DISCOVERY_TAXONOMY_VERSION,
    FUNDAMENTAL_THESIS_PRECEDENCE,
    FUNDAMENTAL_THESIS_RULE_VERSION,
    FundamentalThesisEvaluation,
    FundamentalThesisFamily,
    FundamentalThesisSnapshot,
)


ALLOWED_ADMISSION_STAGES = frozenset(
    {"transition_4_to_1", "stage_1_basing", "transition_1_to_2", "stage_2_advancing"}
)
FINANCIAL_KEYWORDS = (
    "BANK",
    "FINANCE",
    "FINANCIAL",
    "INSURANCE",
    "NBFC",
    "ASSET MANAGEMENT",
)
CYCLICAL_KEYWORDS = (
    "ALUMINIUM", "CEMENT", "COAL", "COMMODIT", "COPPER", "IRON", "METAL",
    "MINING", "OIL", "PAPER", "PETRO", "POWER", "STEEL", "SUGAR", "TEXTILE",
)
FUNDAMENTAL_THESIS_RULE_CONTENT: dict[str, Any] = {
    "primary_precedence": [family.value for family in FUNDAMENTAL_THESIS_PRECEDENCE],
    "security_eligibility": {
        "confirmed_sme_blocks": True,
        "missing_sme_evidence_blocks": False,
        "numeric_boolean_values_supported": True,
    },
    "quality_compounder": {"roce_min": 20, "debt_to_equity_max": 0.5, "opm_min": 18, "sales_growth_3y_min": 15, "profit_growth_3y_min": 10, "fcf_pat_min": 0.7},
    "high_growth_emerging": {"sales_growth_min": 30, "negative_fcf_requires_positive_cfo": True, "negative_fcf_debt_to_equity_max": 1},
    "earnings_acceleration": {"sales_yoy_min": 15, "profit_yoy_min": 25, "profit_qoq_min": 10, "operating_leverage_spread_min": 10, "opm_change_bps_min": 100},
    "undervalued_quality": {"roce_min": 12, "debt_to_equity_max": 1, "positive_cfo": True, "positive_profit": True},
    "cashflow_balance_sheet_inflection": {"fcf_pat_min": 0.8, "fcf_pat_improvement_min": 0.3, "debt_reduction_min": 0.2, "interest_coverage_min": 3},
    "turnaround_cyclical_recovery": {"profit_yoy_min": 50, "sales_yoy_min": 10, "opm_change_bps_min": 100, "cyclical_peak_valuation_block": True},
    "capital_return_income": {"fcf_pat_min": 0.9, "debt_to_equity_max": 0.3, "dividend_yield_min": 1.5, "payout_range": [20, 80]},
}


def ensure_discovery_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_sync_receipt (
            receipt_id VARCHAR PRIMARY KEY,
            sync_batch_ids_json VARCHAR NOT NULL,
            statement_basis VARCHAR NOT NULL,
            started_at TIMESTAMP,
            finished_at TIMESTAMP NOT NULL,
            status VARCHAR NOT NULL,
            symbols_total INTEGER NOT NULL,
            symbols_succeeded INTEGER NOT NULL,
            symbols_skipped INTEGER NOT NULL,
            symbols_failed INTEGER NOT NULL,
            changed_symbols_json VARCHAR NOT NULL,
            failed_symbols_json VARCHAR NOT NULL,
            source_versions_json VARCHAR NOT NULL,
            receipt_json VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_thesis_classification (
            classification_id VARCHAR PRIMARY KEY, symbol_id VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL, as_of DATE NOT NULL, source_data_hash VARCHAR NOT NULL,
            statement_basis VARCHAR NOT NULL, source_report_date DATE, source_available_at DATE,
            primary_thesis VARCHAR, secondary_theses_json VARCHAR NOT NULL,
            classification_status VARCHAR NOT NULL, evaluations_json VARCHAR NOT NULL,
            evidence_json VARCHAR NOT NULL, taxonomy_version VARCHAR NOT NULL,
            rule_version VARCHAR NOT NULL, semantic_payload_hash VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
        )
        """
    )
    conn.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS uq_fundamental_thesis_source_policy
           ON fundamental_thesis_classification(
               symbol_id, exchange, source_data_hash, taxonomy_version, rule_version
           )"""
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_thesis_projection (
            projection_id VARCHAR PRIMARY KEY, symbol_id VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL, as_of DATE NOT NULL, source_data_hash VARCHAR NOT NULL,
            primary_thesis VARCHAR, secondary_theses_json VARCHAR NOT NULL,
            structural_stage VARCHAR NOT NULL, admission_eligible BOOLEAN NOT NULL,
            admission_blockers_json VARCHAR NOT NULL, daily_context_json VARCHAR NOT NULL,
            taxonomy_version VARCHAR NOT NULL, rule_version VARCHAR NOT NULL,
            admission_version VARCHAR NOT NULL, semantic_payload_hash VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
        )
        """
    )
    conn.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS uq_fundamental_thesis_projection
           ON fundamental_thesis_projection(
               symbol_id, exchange, as_of, source_data_hash, rule_version, admission_version
           )"""
    )


def record_sync_receipt(
    conn: duckdb.DuckDBPyConnection,
    *,
    receipt_id: str,
    sync_batch_ids: Iterable[str],
    statement_basis: str,
    status: str,
    symbols_total: int,
    succeeded_symbols: Iterable[str],
    skipped_symbols: Iterable[str],
    failed_symbols: Iterable[str],
    source_versions: dict[str, dict[str, Any]],
) -> None:
    ensure_discovery_schema(conn)
    succeeded = sorted(set(succeeded_symbols))
    skipped = sorted(set(skipped_symbols))
    failed = sorted(set(failed_symbols))
    payload = {
        "receipt_id": receipt_id,
        "sync_batch_ids": sorted(set(sync_batch_ids)),
        "statement_basis": statement_basis,
        "status": status,
        "symbols_total": symbols_total,
        "changed_symbols": succeeded,
        "skipped_symbols": skipped,
        "failed_symbols": failed,
        "source_versions": source_versions,
    }
    conn.execute(
        """
        INSERT INTO fundamental_sync_receipt (
            receipt_id, sync_batch_ids_json, statement_basis, finished_at, status,
            symbols_total, symbols_succeeded, symbols_skipped, symbols_failed,
            changed_symbols_json, failed_symbols_json, source_versions_json, receipt_json
        ) VALUES (?, ?, ?, current_timestamp AT TIME ZONE 'UTC', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(receipt_id) DO NOTHING
        """,
        [
            receipt_id, _json(payload["sync_batch_ids"]), statement_basis, status,
            symbols_total, len(succeeded), len(skipped), len(failed), _json(succeeded),
            _json(failed), _json(source_versions), _json(payload),
        ],
    )


def classify_fundamental_universe(
    frame: pd.DataFrame,
    *,
    as_of: str | date,
) -> tuple[list[FundamentalThesisSnapshot], pd.DataFrame]:
    """Classify one row per mastered security and return long-form evaluations."""

    as_of_date = pd.Timestamp(as_of).date()
    snapshots: list[FundamentalThesisSnapshot] = []
    evaluation_rows: list[dict[str, Any]] = []
    if frame is None or frame.empty:
        return snapshots, pd.DataFrame()
    for _, row in frame.iterrows():
        symbol = str(row.get("symbol_id") or row.get("symbol") or "").upper().strip()
        exchange = str(row.get("exchange") or "NSE").upper().strip()
        evaluations = evaluate_theses(row)
        passed = [item.family for item in evaluations if item.passed]
        primary = passed[0] if passed else None
        source_hash = fundamental_source_hash(row)
        source_report_date = _date(row.get("source_report_date") or row.get("report_date"))
        source_available_at = _date(row.get("source_available_at") or row.get("available_at"))
        status, common_blockers = _classification_status(row, as_of_date, passed)
        evidence = _evidence(row)
        admission_blockers = _daily_blockers(row, primary, evidence, common_blockers)
        eligible = bool(primary and status == "QUALIFIED" and not admission_blockers)
        snapshot = FundamentalThesisSnapshot(
            symbol_id=symbol,
            exchange=exchange,
            as_of=as_of_date,
            primary_thesis=primary,
            secondary_theses=tuple(passed[1:]),
            evaluations=tuple(evaluations),
            source_data_hash=source_hash,
            statement_basis=str(row.get("statement_basis") or "unknown").strip().lower(),
            source_report_date=source_report_date,
            source_available_at=source_available_at,
            classification_status=status,
            admission_eligible=eligible,
            admission_blockers=tuple(dict.fromkeys(admission_blockers)),
            evidence=evidence,
        )
        snapshots.append(snapshot)
        for item in evaluations:
            evaluation_rows.append(
                {
                    "as_of": as_of_date,
                    "symbol_id": symbol,
                    "exchange": exchange,
                    "family": item.family.value,
                    "passed": item.passed,
                    "observed_json": _json(dict(item.observed)),
                    "required_json": _json(dict(item.required)),
                    "blockers_json": _json(list(item.blockers)),
                    "warnings_json": _json(list(item.warnings)),
                    "source_data_hash": source_hash,
                    "statement_basis": str(row.get("statement_basis") or "unknown"),
                    "source_report_date": source_report_date,
                    "source_available_at": source_available_at,
                    "taxonomy_version": FUNDAMENTAL_DISCOVERY_TAXONOMY_VERSION,
                    "rule_version": FUNDAMENTAL_THESIS_RULE_VERSION,
                }
            )
    return snapshots, pd.DataFrame(evaluation_rows)


def project_cached_classification(
    row: pd.Series,
    cached: dict[str, Any],
    *,
    as_of: str | date,
) -> FundamentalThesisSnapshot:
    """Apply current daily context to an immutable cached accounting classification."""

    evaluations = tuple(
        FundamentalThesisEvaluation(
            family=FundamentalThesisFamily(item["family"]),
            passed=bool(item["passed"]),
            observed=item.get("observed") or {},
            required=item.get("required") or {},
            blockers=tuple(item.get("blockers") or ()),
            warnings=tuple(item.get("warnings") or ()),
            rule_version=str(item.get("rule_version") or FUNDAMENTAL_THESIS_RULE_VERSION),
        )
        for item in json.loads(str(cached["evaluations_json"]))
    )
    primary = FundamentalThesisFamily(cached["primary_thesis"]) if cached.get("primary_thesis") else None
    secondary = tuple(
        FundamentalThesisFamily(value)
        for value in json.loads(str(cached.get("secondary_theses_json") or "[]"))
    )
    evidence = _evidence(row)
    status = str(cached["classification_status"])
    static_blockers = () if status in {"QUALIFIED", "UNCLASSIFIED_FUNDAMENTAL"} else (status,)
    blockers = list(_daily_blockers(row, primary, evidence, static_blockers))
    cached_available = _date(cached.get("source_available_at"))
    as_of_date = pd.Timestamp(as_of).date()
    if cached_available and (as_of_date - cached_available).days > 550:
        blockers.append("STALE_FUNDAMENTAL_SOURCE")
        status = "STALE"
    blockers = list(dict.fromkeys(blockers))
    return FundamentalThesisSnapshot(
        symbol_id=str(row.get("symbol_id") or row.get("symbol") or "").upper().strip(),
        exchange=str(row.get("exchange") or "NSE").upper().strip(),
        as_of=as_of_date,
        primary_thesis=primary,
        secondary_theses=secondary,
        evaluations=evaluations,
        source_data_hash=str(cached["source_data_hash"]),
        statement_basis=str(cached["statement_basis"]),
        source_report_date=_date(cached.get("source_report_date")),
        source_available_at=_date(cached.get("source_available_at")),
        classification_status=status,
        admission_eligible=bool(primary and status == "QUALIFIED" and not blockers),
        admission_blockers=tuple(blockers),
        evidence=evidence,
    )


def _daily_blockers(
    row: pd.Series,
    primary: FundamentalThesisFamily | None,
    evidence: dict[str, Any],
    initial: Iterable[str],
) -> tuple[str, ...]:
    blockers = list(initial)
    if _stage(row) not in ALLOWED_ADMISSION_STAGES:
        blockers.append("STAGE_BLOCKED")
    if not _truthy(row.get("daily_context_complete", True)):
        blockers.append("DAILY_CONTEXT_INCOMPLETE")
    if primary is FundamentalThesisFamily.UNDERVALUED_QUALITY and not _valuation_is_cheap(evidence):
        blockers.append("VALUATION_NOT_SUPPORTIVE")
    if (
        primary is FundamentalThesisFamily.TURNAROUND_CYCLICAL_RECOVERY
        and any(word in evidence["industry_text"] for word in CYCLICAL_KEYWORDS)
        and evidence["valuation_history_bucket"] == "EXPENSIVE_VS_HISTORY"
    ):
        blockers.append("CYCLICAL_PEAK_VALUATION_RISK")
    return tuple(dict.fromkeys(blockers))


def evaluate_theses(row: pd.Series) -> tuple[FundamentalThesisEvaluation, ...]:
    values = _evidence(row)
    rules = {
        FundamentalThesisFamily.QUALITY_COMPOUNDER: _quality_compounder,
        FundamentalThesisFamily.HIGH_GROWTH_EMERGING: _high_growth,
        FundamentalThesisFamily.EARNINGS_ACCELERATION: _earnings_acceleration,
        FundamentalThesisFamily.UNDERVALUED_QUALITY: _undervalued_quality,
        FundamentalThesisFamily.CASHFLOW_BALANCE_SHEET_INFLECTION: _cashflow_inflection,
        FundamentalThesisFamily.TURNAROUND_CYCLICAL_RECOVERY: _turnaround,
        FundamentalThesisFamily.CAPITAL_RETURN_INCOME: _capital_return,
    }
    return tuple(rules[family](values) for family in FUNDAMENTAL_THESIS_PRECEDENCE)


def quarantined_snapshot(
    row: pd.Series, *, as_of: str | date, error: Exception
) -> FundamentalThesisSnapshot:
    evaluations = tuple(
        FundamentalThesisEvaluation(
            family=family,
            passed=False,
            observed={},
            required={},
            blockers=("CLASSIFICATION_FAILURE",),
            warnings=(type(error).__name__,),
        )
        for family in FUNDAMENTAL_THESIS_PRECEDENCE
    )
    return FundamentalThesisSnapshot(
        symbol_id=str(row.get("symbol_id") or row.get("symbol") or "").upper().strip(),
        exchange=str(row.get("exchange") or "NSE").upper().strip(),
        as_of=pd.Timestamp(as_of).date(),
        primary_thesis=None,
        secondary_theses=(),
        evaluations=evaluations,
        source_data_hash=fundamental_source_hash(row),
        statement_basis=str(row.get("statement_basis") or "unknown"),
        source_report_date=_date(row.get("source_report_date") or row.get("report_date")),
        source_available_at=_date(row.get("source_available_at") or row.get("available_at")),
        classification_status="QUARANTINED",
        admission_eligible=False,
        admission_blockers=("CLASSIFICATION_FAILURE",),
        evidence={"error_type": type(error).__name__},
    )


def snapshots_to_frame(snapshots: Iterable[FundamentalThesisSnapshot]) -> pd.DataFrame:
    rows = []
    for item in snapshots:
        rows.append(
            {
                "as_of": item.as_of,
                "symbol_id": item.symbol_id,
                "exchange": item.exchange,
                "primary_thesis": item.primary_thesis.value if item.primary_thesis else "",
                "secondary_theses_json": _json([value.value for value in item.secondary_theses]),
                "classification_status": item.classification_status,
                "admission_eligible": item.admission_eligible,
                "admission_blockers_json": _json(list(item.admission_blockers)),
                "statement_basis": item.statement_basis,
                "source_report_date": item.source_report_date,
                "source_available_at": item.source_available_at,
                "source_data_hash": item.source_data_hash,
                "evidence_json": _json(dict(item.evidence)),
                "evaluations_json": _json([_evaluation_dict(value) for value in item.evaluations]),
                "taxonomy_version": item.taxonomy_version,
                "rule_version": item.rule_version,
                "admission_version": item.admission_version,
            }
        )
    return pd.DataFrame(rows)


def snapshots_to_evaluations_frame(
    snapshots: Iterable[FundamentalThesisSnapshot],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        for item in snapshot.evaluations:
            rows.append(
                {
                    "as_of": snapshot.as_of,
                    "symbol_id": snapshot.symbol_id,
                    "exchange": snapshot.exchange,
                    "family": item.family.value,
                    "passed": item.passed,
                    "observed_json": _json(dict(item.observed)),
                    "required_json": _json(dict(item.required)),
                    "blockers_json": _json(list(item.blockers)),
                    "warnings_json": _json(list(item.warnings)),
                    "source_data_hash": snapshot.source_data_hash,
                    "statement_basis": snapshot.statement_basis,
                    "source_report_date": snapshot.source_report_date,
                    "source_available_at": snapshot.source_available_at,
                    "taxonomy_version": snapshot.taxonomy_version,
                    "rule_version": snapshot.rule_version,
                }
            )
    return pd.DataFrame(rows)


def persist_discovery(
    conn: duckdb.DuckDBPyConnection,
    snapshots: Iterable[FundamentalThesisSnapshot],
) -> tuple[int, int]:
    """Persist immutable classifications and daily projections idempotently."""

    ensure_discovery_schema(conn)
    created_classifications = 0
    created_projections = 0
    for item in snapshots:
        evaluation_json = _json([_evaluation_dict(value) for value in item.evaluations])
        evidence_json = _json(dict(item.evidence))
        classification_payload = {
            "symbol_id": item.symbol_id,
            "exchange": item.exchange,
            "source_data_hash": item.source_data_hash,
            "primary_thesis": item.primary_thesis.value if item.primary_thesis else None,
            "secondary": [value.value for value in item.secondary_theses],
            "evaluations": json.loads(evaluation_json),
            "taxonomy_version": item.taxonomy_version,
            "rule_version": item.rule_version,
        }
        semantic_hash = _hash(classification_payload)
        classification_id = "fundamental-classification-" + _hash(
            {
                "symbol_id": item.symbol_id,
                "exchange": item.exchange,
                "source_data_hash": item.source_data_hash,
                "taxonomy_version": item.taxonomy_version,
                "rule_version": item.rule_version,
            }
        )[:24]
        before = conn.execute(
            "SELECT COUNT(*) FROM fundamental_thesis_classification WHERE classification_id = ?",
            [classification_id],
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO fundamental_thesis_classification VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp AT TIME ZONE 'UTC'
            ) ON CONFLICT DO NOTHING
            """,
            [
                classification_id, item.symbol_id, item.exchange, item.as_of,
                item.source_data_hash, item.statement_basis, item.source_report_date,
                item.source_available_at, item.primary_thesis.value if item.primary_thesis else None,
                _json([value.value for value in item.secondary_theses]), item.classification_status,
                evaluation_json, evidence_json, item.taxonomy_version, item.rule_version, semantic_hash,
            ],
        )
        created_classifications += int(before == 0)
        projection_payload = {
            **classification_payload,
            "as_of": item.as_of.isoformat(),
            "eligible": item.admission_eligible,
            "blockers": list(item.admission_blockers),
        }
        projection_hash = _hash(projection_payload)
        projection_id = f"fundamental-projection-{projection_hash[:24]}"
        before_projection = conn.execute(
            "SELECT COUNT(*) FROM fundamental_thesis_projection WHERE projection_id = ?",
            [projection_id],
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO fundamental_thesis_projection VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp AT TIME ZONE 'UTC'
            ) ON CONFLICT DO NOTHING
            """,
            [
                projection_id, item.symbol_id, item.exchange, item.as_of, item.source_data_hash,
                item.primary_thesis.value if item.primary_thesis else None,
                _json([value.value for value in item.secondary_theses]),
                str(item.evidence.get("structural_stage") or "unknown"), item.admission_eligible,
                _json(list(item.admission_blockers)), evidence_json, item.taxonomy_version,
                item.rule_version, item.admission_version, projection_hash,
            ],
        )
        created_projections += int(before_projection == 0)
    return created_classifications, created_projections


def fundamental_source_hash(row: pd.Series) -> str:
    # This hash deliberately excludes stage, rank, price/valuation, patterns and
    # Investigator evidence.  Those are daily projection inputs, not accounting
    # source versions, so an unchanged filing reuses its immutable classification.
    fundamental_fields = {
        "symbol_id", "symbol", "exchange", "statement_basis", "source_report_date",
        "report_date", "source_available_at", "available_at", "industry_group", "industry",
        "sector_name", "is_not_sme", "roce", "roe", "debt_to_equity",
        "debt_to_equity_prev", "opm", "opm_pct", "sales_growth_3y", "profit_growth_3y",
        "sales_yoy_pct", "sales_yoy_growth", "profit_yoy_pct", "profit_yoy_growth",
        "profit_qoq_pct", "profit_qoq_growth", "operating_profit_yoy_pct",
        "operating_profit_yoy_growth", "opm_yoy_change_bps", "opm_yoy_change",
        "cash_from_operations_last_year", "cash_from_operations_previous_year", "cfo", "cfo_prev",
        "free_cash_flow_last_year", "free_cash_flow_previous_year", "fcf", "fcf_prev",
        "fcf_pat", "fcf_pat_prev", "net_profit_cr", "net_profit", "pat",
        "net_profit_previous_year", "profit_same_q_ly", "net_profit_same_q_ly",
        "interest_coverage", "dividend_yield", "dividend_payout_pct", "low_base_flag",
        "quarterly_result_bucket",
    }
    payload = {
        str(key): _clean(value)
        for key, value in sorted(row.to_dict().items())
        if str(key) in fundamental_fields
    }
    return _hash(payload)


def _quality_compounder(v: dict[str, Any]) -> FundamentalThesisEvaluation:
    req = {"roce_min": 20, "debt_to_equity_max": 0.5, "opm_min": 18, "sales_growth_3y_min": 15,
           "profit_growth_3y_min": 10, "fcf_pat_min": 0.7}
    checks = [_n(v, "roce") >= 20, _n(v, "debt_to_equity", 999) <= 0.5, _n(v, "opm") >= 18,
              _n(v, "sales_growth_3y") >= 15, _n(v, "profit_growth_3y") >= 10, _n(v, "fcf_pat") >= 0.7]
    return _evaluation(FundamentalThesisFamily.QUALITY_COMPOUNDER, v, req, checks)


def _high_growth(v: dict[str, Any]) -> FundamentalThesisEvaluation:
    growth = max(_n(v, "sales_growth_3y"), _n(v, "sales_yoy_pct"))
    cash_ok = _n(v, "cfo") > 0 and (_n(v, "fcf", -999) >= 0 or _n(v, "debt_to_equity", 999) <= 1)
    req = {"sales_growth_min": 30, "profit_direction_min": 0, "opm_change_min": 0,
           "cash_rule": "CFO positive; negative FCF allowed only with D/E <= 1"}
    checks = [growth >= 30, max(_n(v, "profit_growth_3y", -999), _n(v, "profit_yoy_pct", -999)) >= 0,
              _n(v, "opm_yoy_change_bps", -999) >= 0, cash_ok]
    return _evaluation(FundamentalThesisFamily.HIGH_GROWTH_EMERGING, {**v, "selected_sales_growth": growth}, req, checks)


def _earnings_acceleration(v: dict[str, Any]) -> FundamentalThesisEvaluation:
    results = _n(v, "sales_yoy_pct") >= 15 and _n(v, "profit_yoy_pct") >= 25 and _n(v, "profit_qoq_pct") >= 10
    leverage = (
        _n(v, "sales_yoy_pct") >= 10
        and _n(v, "operating_profit_yoy_pct") - _n(v, "sales_yoy_pct") >= 10
        and _n(v, "opm_yoy_change_bps") >= 100
        and _n(v, "net_profit") > 0
    )
    req = {"results_acceleration": "sales YoY >=15, profit YoY >=25, profit QoQ >=10",
           "operating_leverage": "sales YoY >=10, OP growth spread >=10pp, OPM +100bps, PAT positive"}
    return _evaluation(FundamentalThesisFamily.EARNINGS_ACCELERATION,
                       {**v, "results_acceleration_pass": results, "operating_leverage_pass": leverage}, req,
                       [results or leverage])


def _undervalued_quality(v: dict[str, Any]) -> FundamentalThesisEvaluation:
    req = {"daily_projection_requires_below_history_or_pb_below": 1.5, "roce_min": 12, "debt_to_equity_max": 1,
           "cfo_positive": True, "profit_positive": True, "result_not_deteriorating": True}
    checks = [_n(v, "roce") >= 12, _n(v, "debt_to_equity", 999) <= 1, _n(v, "cfo") > 0,
              _n(v, "net_profit") > 0, v["quarterly_result_bucket"] != "DETERIORATING"]
    return _evaluation(FundamentalThesisFamily.UNDERVALUED_QUALITY, v, req, checks)


def _cashflow_inflection(v: dict[str, Any]) -> FundamentalThesisEvaluation:
    cash_turn = (_n(v, "cfo_prev", 1) <= 0 < _n(v, "cfo")) or (_n(v, "fcf_prev", 1) <= 0 < _n(v, "fcf"))
    conversion = _n(v, "fcf_pat") >= 0.8 and _n(v, "fcf_pat") - _n(v, "fcf_pat_prev") >= 0.3
    deleveraging = _n(v, "debt_to_equity_prev") > 0 and _n(v, "debt_to_equity", 999) <= _n(v, "debt_to_equity_prev") * 0.8 and _n(v, "interest_coverage") >= 3
    req = {"latest_cfo_positive": True, "one_of": "cash turn, FCF/PAT +0.3 to >=0.8, or D/E -20% with coverage >=3"}
    return _evaluation(FundamentalThesisFamily.CASHFLOW_BALANCE_SHEET_INFLECTION,
                       {**v, "cash_turn": cash_turn, "conversion_improved": conversion, "deleveraging": deleveraging},
                       req, [_n(v, "cfo") > 0, cash_turn or conversion or deleveraging])


def _turnaround(v: dict[str, Any]) -> FundamentalThesisEvaluation:
    recovery = (_n(v, "profit_same_q_ly", 1) <= 0 < _n(v, "net_profit")) or _n(v, "profit_yoy_pct") > 50
    cyclical = any(word in v["industry_text"] for word in CYCLICAL_KEYWORDS)
    req = {"loss_to_profit_or_profit_yoy_gt": 50, "sales_yoy_min": 10, "opm_change_bps_min": 100,
           "profit_positive": True, "daily_projection_blocks_cyclical_peak_valuation": True}
    checks = [recovery, _n(v, "sales_yoy_pct") > 10, _n(v, "opm_yoy_change_bps") >= 100,
              _n(v, "net_profit") > 0]
    warnings = ("LOW_BASE_REVIEW",) if v["low_base_flag"] else ()
    return _evaluation(FundamentalThesisFamily.TURNAROUND_CYCLICAL_RECOVERY,
                       {**v, "recovery": recovery, "cyclical": cyclical}, req, checks,
                       warnings=warnings)


def _capital_return(v: dict[str, Any]) -> FundamentalThesisEvaluation:
    distribution = _n(v, "dividend_yield") >= 1.5 or 20 <= _n(v, "dividend_payout_pct", -1) <= 80
    req = {"cfo_positive": True, "fcf_pat_min": 0.9, "debt_to_equity_max": 0.3,
           "profit_positive": True, "dividend_yield_min_or_payout_range": "1.5% or 20-80%"}
    checks = [_n(v, "cfo") > 0, _n(v, "fcf_pat") >= 0.9, _n(v, "debt_to_equity", 999) <= 0.3,
              _n(v, "net_profit") > 0, distribution]
    return _evaluation(FundamentalThesisFamily.CAPITAL_RETURN_INCOME, {**v, "distribution": distribution}, req, checks)


def _evaluation(
    family: FundamentalThesisFamily,
    observed: dict[str, Any],
    required: dict[str, Any],
    checks: list[bool],
    *,
    warnings: tuple[str, ...] = (),
) -> FundamentalThesisEvaluation:
    missing = [key for key in _required_inputs(family) if observed.get(key) is None]
    blockers = tuple(["MISSING_REQUIRED_EVIDENCE"] if missing else [])
    passed = bool(not blockers and all(checks))
    compact = {key: value for key, value in observed.items() if key in _observed_keys(family) or key.endswith("_pass")}
    if missing:
        compact["missing_fields"] = missing
    return FundamentalThesisEvaluation(family, passed, compact, required, blockers, warnings)


def _required_inputs(family: FundamentalThesisFamily) -> tuple[str, ...]:
    mapping = {
        FundamentalThesisFamily.QUALITY_COMPOUNDER: ("roce", "debt_to_equity", "opm", "sales_growth_3y", "profit_growth_3y", "fcf_pat"),
        FundamentalThesisFamily.HIGH_GROWTH_EMERGING: ("sales_growth_3y", "cfo", "debt_to_equity"),
        FundamentalThesisFamily.EARNINGS_ACCELERATION: ("sales_yoy_pct", "profit_yoy_pct", "profit_qoq_pct", "opm_yoy_change_bps"),
        FundamentalThesisFamily.UNDERVALUED_QUALITY: ("roce", "debt_to_equity", "cfo", "net_profit"),
        FundamentalThesisFamily.CASHFLOW_BALANCE_SHEET_INFLECTION: ("cfo", "cfo_prev"),
        FundamentalThesisFamily.TURNAROUND_CYCLICAL_RECOVERY: ("sales_yoy_pct", "profit_yoy_pct", "opm_yoy_change_bps", "net_profit"),
        FundamentalThesisFamily.CAPITAL_RETURN_INCOME: ("cfo", "fcf_pat", "debt_to_equity", "net_profit"),
    }
    return mapping[family]


def _observed_keys(family: FundamentalThesisFamily) -> set[str]:
    return set(_required_inputs(family)) | {
        "fcf", "profit_same_q_ly", "operating_profit_yoy_pct", "quarterly_result_bucket",
        "interest_coverage", "dividend_yield",
        "dividend_payout_pct", "cash_turn", "conversion_improved", "deleveraging",
        "recovery", "cyclical", "distribution", "selected_sales_growth",
    }


def _classification_status(row: pd.Series, as_of: date, passed: list[FundamentalThesisFamily]) -> tuple[str, tuple[str, ...]]:
    if not str(row.get("symbol_id") or row.get("symbol") or "").strip():
        return "EXCLUDED", ("UNRESOLVED_IDENTITY",)
    text = f"{row.get('industry_group', '')} {row.get('industry', '')} {row.get('sector_name', '')}".upper()
    if any(word in text for word in FINANCIAL_KEYWORDS):
        return "UNSUPPORTED_FINANCIAL_MODEL", ("UNSUPPORTED_FINANCIAL_MODEL",)
    is_not_sme = _boolish(row.get("is_not_sme"))
    if is_not_sme is False:
        return "EXCLUDED", ("SME_INELIGIBLE",)
    basis = str(row.get("statement_basis") or "").strip().lower()
    if basis not in {"standalone", "consolidated"}:
        return "EXCLUDED", ("UNRESOLVED_STATEMENT_BASIS",)
    available = _date(row.get("source_available_at") or row.get("available_at"))
    if available is None:
        return "EXCLUDED", ("MISSING_SOURCE_FRESHNESS",)
    if available and available > as_of:
        return "EXCLUDED", ("FUTURE_DATED_INPUT",)
    if available and (as_of - available).days > 550:
        return "STALE", ("STALE_FUNDAMENTAL_SOURCE",)
    if passed:
        return "QUALIFIED", ()
    return "UNCLASSIFIED_FUNDAMENTAL", ()


def _valuation_is_cheap(evidence: dict[str, Any]) -> bool:
    return (
        evidence["valuation_history_bucket"] in {"DEEPLY_BELOW_HISTORY", "BELOW_OWN_MEDIAN"}
        or _n(evidence, "pb", 999) < 1.5
    )


def _evidence(row: pd.Series) -> dict[str, Any]:
    net_profit = _maybe(row, "net_profit_cr", "net_profit", "pat")
    fcf = _maybe(row, "free_cash_flow_last_year", "fcf")
    fcf_prev = _maybe(row, "free_cash_flow_previous_year", "fcf_prev")
    return {
        "roce": _maybe(row, "roce"),
        "roe": _maybe(row, "roe"),
        "debt_to_equity": _maybe(row, "debt_to_equity"),
        "debt_to_equity_prev": _maybe(row, "debt_to_equity_prev"),
        "opm": _maybe(row, "opm", "opm_pct"),
        "sales_growth_3y": _maybe(row, "sales_growth_3y"),
        "profit_growth_3y": _maybe(row, "profit_growth_3y"),
        "sales_yoy_pct": _maybe(row, "sales_yoy_pct", scale_fallback=("sales_yoy_growth", 100)),
        "profit_yoy_pct": _maybe(row, "profit_yoy_pct", scale_fallback=("profit_yoy_growth", 100)),
        "profit_qoq_pct": _maybe(row, "profit_qoq_pct", scale_fallback=("profit_qoq_growth", 100)),
        "operating_profit_yoy_pct": _maybe(row, "operating_profit_yoy_pct", scale_fallback=("operating_profit_yoy_growth", 100)),
        "opm_yoy_change_bps": _maybe(row, "opm_yoy_change_bps", scale_fallback=("opm_yoy_change", 100)),
        "cfo": _maybe(row, "cash_from_operations_last_year", "cfo"),
        "cfo_prev": _maybe(row, "cash_from_operations_previous_year", "cfo_prev"),
        "fcf": fcf,
        "fcf_prev": fcf_prev,
        "fcf_pat": _maybe(row, "fcf_pat") if _maybe(row, "fcf_pat") is not None else _ratio(fcf, net_profit),
        "fcf_pat_prev": _maybe(row, "fcf_pat_prev") if _maybe(row, "fcf_pat_prev") is not None else _ratio(fcf_prev, _maybe(row, "net_profit_previous_year")),
        "net_profit": net_profit,
        "profit_same_q_ly": _maybe(row, "profit_same_q_ly", "net_profit_same_q_ly"),
        "interest_coverage": _maybe(row, "interest_coverage"),
        "dividend_yield": _maybe(row, "dividend_yield"),
        "dividend_payout_pct": _maybe(row, "dividend_payout_pct"),
        "valuation_history_bucket": str(row.get("valuation_history_bucket") or "UNKNOWN").upper(),
        "quarterly_result_bucket": str(row.get("quarterly_result_bucket") or "UNKNOWN").upper(),
        "pb": _maybe(row, "pb", "price_to_book"),
        "low_base_flag": _truthy(row.get("low_base_flag")),
        "industry_text": f"{row.get('industry_group', '')} {row.get('industry', '')} {row.get('sector_name', '')}".upper(),
        "structural_stage": _stage(row),
        "composite_score": _maybe(row, "composite_score", "composite_score_adjusted"),
        "pattern_score": _maybe(row, "pattern_score"),
        "breakout_score": _maybe(row, "breakout_score"),
        "investigator_score": _maybe(row, "investigator_score", "final_score"),
    }


def _stage(row: pd.Series) -> str:
    return str(row.get("structural_stage") or row.get("effective_stage") or row.get("provisional_stage") or "unknown").strip().lower()


def _maybe(row: pd.Series, *keys: str, scale_fallback: tuple[str, float] | None = None) -> float | None:
    for key in keys:
        value = row.get(key)
        if value is not None and not pd.isna(value):
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    if scale_fallback:
        value = row.get(scale_fallback[0])
        if value is not None and not pd.isna(value):
            try:
                return float(value) * scale_fallback[1]
            except (TypeError, ValueError):
                pass
    return None


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / abs(float(denominator))


def _n(values: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = values.get(key)
    return float(default) if value is None else float(value)


def _date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    try:
        return pd.Timestamp(value).date()
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    return _boolish(value) is True


def _boolish(value: Any) -> bool | None:
    """Normalize persisted boolean-ish scalars without treating missing as false."""

    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric == 1.0:
            return True
        if numeric == 0.0:
            return False
        return None
    text = str(value).strip().lower()
    if text in {"1", "1.0", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "0.0", "false", "f", "no", "n"}:
        return False
    return None


def _evaluation_dict(item: FundamentalThesisEvaluation) -> dict[str, Any]:
    return {
        "family": item.family.value,
        "passed": item.passed,
        "observed": dict(item.observed),
        "required": dict(item.required),
        "blockers": list(item.blockers),
        "warnings": list(item.warnings),
        "rule_version": item.rule_version,
    }


def _clean(value: Any) -> Any:
    if value is None or (not isinstance(value, (list, dict, tuple)) and pd.isna(value)):
        return None
    if isinstance(value, (date, pd.Timestamp)):
        return str(value)[:10]
    if isinstance(value, float):
        return round(value, 10)
    return value


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


__all__ = [
    "ALLOWED_ADMISSION_STAGES",
    "FUNDAMENTAL_THESIS_RULE_CONTENT",
    "classify_fundamental_universe",
    "ensure_discovery_schema",
    "evaluate_theses",
    "fundamental_source_hash",
    "persist_discovery",
    "project_cached_classification",
    "quarantined_snapshot",
    "record_sync_receipt",
    "snapshots_to_frame",
    "snapshots_to_evaluations_frame",
]
