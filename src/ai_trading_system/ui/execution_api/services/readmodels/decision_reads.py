"""Shared DuckDB read models for persisted decision-layer facts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths


class DecisionReadError(RuntimeError):
    """Raised when a requested decision view cannot be selected unambiguously."""


@dataclass(frozen=True)
class DecisionVersion:
    model_version: str
    config_hash: str


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    clean = frame.astype(object).where(pd.notna(frame), None)
    rows = clean.to_dict(orient="records")
    for row in rows:
        for key, value in tuple(row.items()):
            if hasattr(value, "isoformat"):
                rendered = value.isoformat()
                row[key] = rendered[:10] if key.endswith("date") else rendered
            elif isinstance(value, str) and key.endswith(("reasons", "fields", "sources")):
                try:
                    row[key] = json.loads(value)
                except (TypeError, ValueError):
                    pass
    return rows


class DecisionReadRepository:
    table: str
    domain: str
    version_column: str
    config_column: str
    date_column = "trade_date"

    def __init__(self, project_root: Path | str, *, db_path: Path | None = None):
        self.project_root = Path(project_root)
        self.db_path = db_path or (
            get_domain_paths(project_root=project_root, data_domain="operational").root_dir
            / "control_plane.duckdb"
        )

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if not self.db_path.exists():
            raise DecisionReadError(f"Decision database is unavailable: {self.db_path}")
        return duckdb.connect(str(self.db_path), read_only=True)

    def _version(
        self,
        conn: duckdb.DuckDBPyConnection,
        as_of: str,
        model_version: str | None,
        config_hash: str | None,
    ) -> DecisionVersion:
        if model_version:
            if config_hash:
                return DecisionVersion(model_version, config_hash)
            rows = conn.execute(
                f"SELECT DISTINCT {self.config_column} FROM {self.table} "
                f"WHERE {self.date_column} <= CAST(? AS DATE) AND {self.version_column} = ?",
                [as_of, model_version],
            ).fetchall()
            if len(rows) != 1:
                raise DecisionReadError("model_version requires config_hash when zero or multiple configurations exist")
            return DecisionVersion(model_version, str(rows[0][0]))
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        if "decision_model_deployment" not in tables:
            raise DecisionReadError("decision_model_deployment is unavailable")
        rows = conn.execute(
            """SELECT model_version, config_hash
               FROM decision_model_deployment
               WHERE decision_domain = ? AND environment = 'production'
                 AND status = 'approved' AND effective_from <= CAST(? AS DATE)
                 AND (effective_to IS NULL OR effective_to >= CAST(? AS DATE))
               QUALIFY effective_from = MAX(effective_from) OVER ()""",
            [self.domain, as_of, as_of],
        ).fetchall()
        if len(rows) != 1:
            raise DecisionReadError(
                f"Expected one approved {self.domain} version for {as_of}; found {len(rows)}"
            )
        return DecisionVersion(str(rows[0][0]), str(rows[0][1]))

    def _as_of(self, conn: duckdb.DuckDBPyConnection, requested: str | None) -> str | None:
        if requested:
            return requested
        row = conn.execute(f"SELECT MAX({self.date_column}) FROM {self.table}").fetchone()
        return str(row[0]) if row and row[0] else None

    def _response(
        self, frame: pd.DataFrame, *, as_of: str | None, version: DecisionVersion | None,
        error: str | None = None,
    ) -> dict[str, Any]:
        run_id = None
        if not frame.empty and "pipeline_run_id" in frame:
            run_id = str(frame.iloc[0]["pipeline_run_id"])
        return {
            "metadata": {
                "domain": self.domain, "data_source": "DUCKDB", "as_of_date": as_of,
                "model_version": version.model_version if version else None,
                "config_hash": version.config_hash if version else None,
                "pipeline_run_id": run_id, "row_count": len(frame),
                "fallback_used": False, "fallback_reason": None, "error": error,
            },
            "rows": _records(frame),
        }

    def history(
        self, symbol_id: str, *, exchange: str = "NSE", start_date: str | None = None,
        end_date: str | None = None, model_version: str | None = None,
        config_hash: str | None = None, limit: int = 500, offset: int = 0,
        extra_clauses: Iterable[str] = (), extra_params: Iterable[Any] = (),
    ) -> dict[str, Any]:
        with self._connect() as conn:
            as_of = self._as_of(conn, end_date)
            if not as_of:
                return self._response(pd.DataFrame(), as_of=None, version=None)
            version = self._version(conn, as_of, model_version, config_hash)
            clauses = ["symbol_id = ?", "exchange = ?", f"{self.version_column} = ?", f"{self.config_column} = ?"]
            params: list[Any] = [symbol_id.upper(), exchange, version.model_version, version.config_hash]
            if start_date:
                clauses.append(f"{self.date_column} >= CAST(? AS DATE)")
                params.append(start_date)
            if end_date:
                clauses.append(f"{self.date_column} <= CAST(? AS DATE)")
                params.append(end_date)
            clauses.extend(extra_clauses)
            params.extend(extra_params)
            frame = conn.execute(
                f"SELECT * FROM {self.table} WHERE {' AND '.join(clauses)} "
                f"ORDER BY {self.date_column} LIMIT ? OFFSET ?",
                [*params, min(max(limit, 1), 2000), max(offset, 0)],
            ).fetchdf()
            return self._response(frame, as_of=as_of, version=version)


class RankHistoryReadRepository(DecisionReadRepository):
    table, domain = "rank_history", "rank"
    version_column, config_column = "rank_model_version", "rank_config_hash"

    def get_current_rankings(self, *, trade_date: str | None = None, universe_id: str = "NSE_OPERATIONAL", model_version: str | None = None, config_hash: str | None = None, limit: int = 500) -> dict[str, Any]:
        with self._connect() as conn:
            as_of = self._as_of(conn, trade_date)
            if not as_of:
                return self._response(pd.DataFrame(), as_of=None, version=None)
            version = self._version(conn, as_of, model_version, config_hash)
            frame = conn.execute(
                """SELECT *, LAG(rank_position, 1) OVER symbol_history AS previous_rank,
                          LAG(rank_position, 5) OVER symbol_history - rank_position AS rank_improvement_5d,
                          LAG(rank_position, 20) OVER symbol_history - rank_position AS rank_improvement_20d,
                          MIN(rank_position) OVER (PARTITION BY symbol_id, exchange, universe_id, rank_model_version) AS best_rank
                   FROM rank_history
                   WHERE trade_date <= CAST(? AS DATE) AND universe_id = ?
                     AND rank_model_version = ? AND rank_config_hash = ?
                   WINDOW symbol_history AS (PARTITION BY symbol_id, exchange, universe_id, rank_model_version ORDER BY trade_date)
                   QUALIFY trade_date = MAX(trade_date) OVER ()
                   ORDER BY rank_position NULLS LAST LIMIT ?""",
                [as_of, universe_id, version.model_version, version.config_hash, min(max(limit, 1), 2000)],
            ).fetchdf()
            return self._response(frame, as_of=as_of, version=version)


class StageHistoryReadRepository(DecisionReadRepository):
    table, domain = "stage_history", "stage"
    version_column, config_column = "stage_model_version", "stage_config_hash"

    def get_current_stage_snapshot(self, *, trade_date: str | None = None, model_version: str | None = None, config_hash: str | None = None, limit: int = 2000) -> dict[str, Any]:
        with self._connect() as conn:
            as_of = self._as_of(conn, trade_date)
            if not as_of:
                return self._response(pd.DataFrame(), as_of=None, version=None)
            version = self._version(conn, as_of, model_version, config_hash)
            frame = conn.execute(
                "SELECT * FROM stage_history WHERE trade_date = CAST(? AS DATE) AND stage_model_version = ? AND stage_config_hash = ? ORDER BY symbol_id LIMIT ?",
                [as_of, version.model_version, version.config_hash, min(max(limit, 1), 5000)],
            ).fetchdf()
            return self._response(frame, as_of=as_of, version=version)


class Stage1AnalyticsReadRepository(DecisionReadRepository):
    table, domain = "stage1_history", "stage1"
    version_column, config_column = "stage1_model_version", "stage1_config_hash"

    def get_stage1_daily_history(self, symbol_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.history(symbol_id, **kwargs)

    def get_current_analytics(self, *, trade_date: str | None = None, model_version: str | None = None, config_hash: str | None = None, limit: int = 2000) -> dict[str, Any]:
        with self._connect() as conn:
            as_of = self._as_of(conn, trade_date)
            if not as_of:
                return self._response(pd.DataFrame(), as_of=None, version=None)
            version = self._version(conn, as_of, model_version, config_hash)
            frame = conn.execute(
                "SELECT * FROM stage1_history WHERE trade_date = CAST(? AS DATE) AND stage1_model_version = ? AND stage1_config_hash = ? ORDER BY stage1_emerging_rank NULLS LAST, symbol_id LIMIT ?",
                [as_of, version.model_version, version.config_hash, min(max(limit, 1), 5000)],
            ).fetchdf()
            return self._response(frame, as_of=as_of, version=version)


class PatternHistoryReadRepository(DecisionReadRepository):
    table, domain = "pattern_history", "pattern"
    version_column, config_column = "pattern_model_version", "pattern_config_hash"

    def get_current_patterns(self, *, trade_date: str | None = None, pattern_family: str | None = None, model_version: str | None = None, config_hash: str | None = None, limit: int = 2000) -> dict[str, Any]:
        with self._connect() as conn:
            as_of = self._as_of(conn, trade_date)
            if not as_of:
                return self._response(pd.DataFrame(), as_of=None, version=None)
            version = self._version(conn, as_of, model_version, config_hash)
            clauses, params = ["trade_date = CAST(? AS DATE)", "pattern_model_version = ?", "pattern_config_hash = ?"], [as_of, version.model_version, version.config_hash]
            if pattern_family:
                clauses.append("pattern_family = ?")
                params.append(pattern_family)
            frame = conn.execute(
                f"SELECT * FROM pattern_history WHERE {' AND '.join(clauses)} ORDER BY symbol_id, pattern_family LIMIT ?",
                [*params, min(max(limit, 1), 5000)],
            ).fetchdf()
            return self._response(frame, as_of=as_of, version=version)


class Stage1LifecycleReadRepository:
    def __init__(self, project_root: Path | str, *, db_path: Path | None = None):
        self.db_path = db_path or (get_domain_paths(project_root=project_root, data_domain="operational").root_dir / "control_plane.duckdb")

    def _query(self, sql: str, params: list[Any]) -> pd.DataFrame:
        if not self.db_path.exists():
            return pd.DataFrame()
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            return conn.execute(sql, params).fetchdf()

    def get_current_candidates(self) -> pd.DataFrame:
        return self._query("SELECT * FROM investigator_stage1_current ORDER BY as_of_trade_date DESC, symbol_id", [])

    def get_current_candidate(self, symbol_id: str, exchange: str = "NSE") -> pd.DataFrame:
        return self._query("SELECT * FROM investigator_stage1_current WHERE UPPER(symbol_id) = ? AND exchange = ?", [symbol_id.upper(), exchange])

    def get_candidate_history(self, symbol_id: str, start_date: str | None = None, end_date: str | None = None, exchange: str = "NSE", limit: int = 1000) -> pd.DataFrame:
        clauses, params = ["UPPER(symbol_id) = ?", "exchange = ?"], [symbol_id.upper(), exchange]
        if start_date:
            clauses.append("trade_date >= CAST(? AS DATE)")
            params.append(start_date)
        if end_date:
            clauses.append("trade_date <= CAST(? AS DATE)")
            params.append(end_date)
        return self._query(f"SELECT * FROM investigator_stage1_state WHERE {' AND '.join(clauses)} ORDER BY trade_date LIMIT ?", [*params, min(max(limit, 1), 2000)])

    def get_candidate_transitions(self, symbol_id: str, start_date: str | None = None, end_date: str | None = None, exchange: str = "NSE", limit: int = 1000) -> pd.DataFrame:
        clauses, params = ["UPPER(symbol_id) = ?", "exchange = ?"], [symbol_id.upper(), exchange]
        if start_date:
            clauses.append("trade_date >= CAST(? AS DATE)")
            params.append(start_date)
        if end_date:
            clauses.append("trade_date <= CAST(? AS DATE)")
            params.append(end_date)
        return self._query(f"SELECT * FROM investigator_stage1_transition WHERE {' AND '.join(clauses)} ORDER BY trade_date LIMIT ?", [*params, min(max(limit, 1), 2000)])

    def reconciliation(self) -> dict[str, int]:
        if not self.db_path.exists():
            return {key: 0 for key in ("missing_current_rows", "duplicate_current_keys", "date_mismatches", "state_mismatches", "stale_current_rows")}
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            row = conn.execute("""WITH latest AS (
                    SELECT * FROM investigator_stage1_state
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id, exchange ORDER BY trade_date DESC, updated_at DESC NULLS LAST) = 1
                ) SELECT
                    (SELECT COUNT(*) FROM latest h LEFT JOIN investigator_stage1_current c USING(symbol_id, exchange) WHERE c.symbol_id IS NULL),
                    (SELECT COUNT(*) FROM (SELECT symbol_id, exchange, COUNT(*) n FROM investigator_stage1_current GROUP BY 1,2 HAVING n > 1)),
                    (SELECT COUNT(*) FROM investigator_stage1_current c JOIN latest h USING(symbol_id, exchange) WHERE c.as_of_trade_date <> h.trade_date),
                    (SELECT COUNT(*) FROM investigator_stage1_current c JOIN latest h USING(symbol_id,exchange) WHERE c.stage1_lifecycle_state IS DISTINCT FROM h.stage1_lifecycle_state),
                    (SELECT COUNT(*) FROM investigator_stage1_current c JOIN latest h USING(symbol_id,exchange) WHERE c.as_of_trade_date < h.trade_date)""").fetchone()
        keys = ("missing_current_rows", "duplicate_current_keys", "date_mismatches", "state_mismatches", "stale_current_rows")
        return dict(zip(keys, map(int, row)))


__all__ = [
    "DecisionReadError", "RankHistoryReadRepository", "StageHistoryReadRepository",
    "Stage1AnalyticsReadRepository", "PatternHistoryReadRepository", "Stage1LifecycleReadRepository",
]


class DecisionOperatorReadService:
    """Compose point-in-time decision facts without reimplementing analytics."""

    def __init__(self, project_root: Path | str, *, stale_session_tolerance: int = 1):
        self.project_root = Path(project_root)
        self.stale_session_tolerance = max(int(stale_session_tolerance), 0)
        self.rank = RankHistoryReadRepository(project_root)
        self.stage = StageHistoryReadRepository(project_root)
        self.stage1 = Stage1AnalyticsReadRepository(project_root)
        self.pattern = PatternHistoryReadRepository(project_root)
        self.lifecycle = Stage1LifecycleReadRepository(project_root)

    @staticmethod
    def _index(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
        return {(str(row.get("symbol_id", "")).upper(), str(row.get("exchange") or "NSE")): row for row in rows}

    def current(self, *, trade_date: str | None = None, limit: int = 500) -> dict[str, Any]:
        domains: dict[str, dict[str, Any]] = {}
        errors: dict[str, str] = {}
        calls = {
            "rank": lambda: self.rank.get_current_rankings(trade_date=trade_date, limit=limit),
            "stage": lambda: self.stage.get_current_stage_snapshot(trade_date=trade_date, limit=limit),
            "pattern": lambda: self.pattern.get_current_patterns(trade_date=trade_date, limit=limit),
            "stage1": lambda: self.stage1.get_current_analytics(trade_date=trade_date, limit=limit),
        }
        for name, call in calls.items():
            try:
                domains[name] = call()
            except (DecisionReadError, duckdb.Error) as exc:
                errors[name] = str(exc)
                domains[name] = {"metadata": {"domain": name, "data_source": "DUCKDB", "row_count": 0, "fallback_used": False, "error": str(exc)}, "rows": []}
        lifecycle_frame = self.lifecycle.get_current_candidates()
        lifecycle_rows = _records(lifecycle_frame)
        domains["stage1_lifecycle"] = {
            "metadata": {"domain": "stage1_lifecycle", "data_source": "DUCKDB", "as_of_date": max((str(r.get("as_of_trade_date")) for r in lifecycle_rows), default=None), "row_count": len(lifecycle_rows), "fallback_used": False, "error": None},
            "rows": lifecycle_rows,
        }
        indexes = {name: self._index(payload["rows"]) for name, payload in domains.items()}
        try:
            from ai_trading_system.ui.execution_api.services.readmodels.stage1_operator import get_stage1_context_by_symbol
            operator_context = get_stage1_context_by_symbol(self.project_root)
        except Exception:
            operator_context = {}
        keys = set().union(*(set(index) for index in indexes.values()))
        rows: list[dict[str, Any]] = []
        for key in sorted(keys):
            rank = indexes["rank"].get(key, {})
            stage = indexes["stage"].get(key, {})
            lifecycle = indexes["stage1_lifecycle"].get(key, {})
            analytics = indexes["stage1"].get(key, {})
            patterns = [row for row in domains["pattern"]["rows"] if (str(row.get("symbol_id", "")).upper(), str(row.get("exchange") or "NSE")) == key]
            rows.append({
                "symbol_id": key[0], "exchange": key[1],
                **{k: rank.get(k) for k in ("trade_date", "rank_position", "composite_score", "previous_rank", "rank_improvement_5d", "rank_improvement_20d", "best_rank")},
                **{k: stage.get(k) for k in ("stage_label", "stage_confidence", "stage_reason", "stage_input_complete")},
                **{k: lifecycle.get(k) for k in ("stage1_lifecycle_state", "stage1_substate", "stage1_maturity_score", "stage1_emerging_rank", "golden_cross_status", "pattern_promotion_state", "distance_to_pivot_pct")},
                **{k: analytics.get(k) for k in ("stage1_score_band", "stage1_eligible", "stage1_block_reasons", "stage1_emerging_score", "golden_cross_progression_score", "promotion_eligibility")},
                **{k: operator_context.get(key[0], {}).get(k) for k in ("operator_status", "operator_priority", "operator_action", "operator_reason")},
                "patterns": patterns,
                "rank_as_of_date": domains["rank"]["metadata"].get("as_of_date") if rank else None,
                "stage_as_of_date": domains["stage"]["metadata"].get("as_of_date") if stage else None,
                "stage1_as_of_date": (
                    domains["stage1"]["metadata"].get("as_of_date") or lifecycle.get("as_of_trade_date")
                ) if analytics or lifecycle else None,
                "pattern_as_of_date": domains["pattern"]["metadata"].get("as_of_date") if patterns else None,
            })
        for row in rows:
            dates = [row.get(name) for name in ("rank_as_of_date", "stage_as_of_date", "stage1_as_of_date", "pattern_as_of_date")]
            parsed = [pd.Timestamp(value) for value in dates if value]
            if len(parsed) < 4:
                status = "INCOMPLETE"
            else:
                spread = (max(parsed) - min(parsed)).days
                status = "ALIGNED" if spread == 0 else ("PARTIALLY_STALE" if spread <= self.stale_session_tolerance else "STALE")
            row["data_freshness_status"] = status
        return {"rows": rows[: min(max(limit, 1), 2000)], "sources": {name: payload["metadata"] for name, payload in domains.items()}, "errors": errors}

    def decision_history(self, symbol_id: str, **kwargs: Any) -> dict[str, Any]:
        histories: dict[str, Any] = {}
        for name, repo in (("rank", self.rank), ("stage", self.stage), ("stage1", self.stage1), ("pattern", self.pattern)):
            try:
                histories[name] = repo.history(symbol_id, **kwargs)
            except DecisionReadError as exc:
                histories[name] = {"metadata": {"domain": name, "row_count": 0, "error": str(exc)}, "rows": []}
        lifecycle = self.lifecycle.get_candidate_history(symbol_id, kwargs.get("start_date"), kwargs.get("end_date"), kwargs.get("exchange", "NSE"), kwargs.get("limit", 500))
        transitions = self.lifecycle.get_candidate_transitions(symbol_id, kwargs.get("start_date"), kwargs.get("end_date"), kwargs.get("exchange", "NSE"), kwargs.get("limit", 500))
        all_dates = sorted({str(row.get("trade_date")) for payload in histories.values() for row in payload["rows"] if row.get("trade_date")})
        aligned = []
        for trade_date_value in all_dates:
            row: dict[str, Any] = {"trade_date": trade_date_value}
            for name, payload in histories.items():
                matches = [item for item in payload["rows"] if str(item.get("trade_date")) == trade_date_value]
                row[name] = matches if name == "pattern" else (matches[0] if matches else None)
            aligned.append(row)
        return {"symbol_id": symbol_id.upper(), "histories": histories, "lifecycle": _records(lifecycle), "transitions": _records(transitions), "aligned": aligned}


__all__.append("DecisionOperatorReadService")
