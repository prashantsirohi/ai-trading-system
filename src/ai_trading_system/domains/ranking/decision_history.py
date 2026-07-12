"""Transactional persistence and reads for derived decision-layer facts."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

import duckdb
import pandas as pd


_JSON_COLUMNS = {
    "stage_input_missing_fields", "stage1_block_reasons", "stage1_adjustment_reasons",
    "stage1_missing_components", "promotion_block_reasons", "candidate_sources",
    "transition_reason_codes",
}


def _hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()[:16]


def _json_array(value: Any) -> str:
    if value is None or (not isinstance(value, (list, tuple, set, dict)) and pd.isna(value)):
        return "[]"
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            value = parsed if isinstance(parsed, list) else [value]
        except (TypeError, ValueError):
            value = [part for part in value.split("|") if part] if "|" in value else ([value] if value else [])
    if not isinstance(value, (list, tuple, set)):
        value = [value]
    return json.dumps(sorted({str(item) for item in value if str(item)}), separators=(",", ":"))


def _first(frame: pd.DataFrame, names: Iterable[str], default: Any = pd.NA) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return frame[name]
    return pd.Series(default, index=frame.index)


def _base(frame: pd.DataFrame, *, run_date: str, exchange: str = "NSE") -> pd.DataFrame:
    out = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return out
    out.loc[:, "symbol_id"] = _first(out, ("symbol_id", "symbol", "Symbol")).astype("string").str.upper().str.strip()
    out.loc[:, "exchange"] = _first(out, ("exchange",), exchange).fillna(exchange).astype("string").replace("", exchange)
    out.loc[:, "trade_date"] = pd.to_datetime(_first(out, ("trade_date", "signal_date", "date"), run_date), errors="coerce").dt.date
    return out.loc[out["symbol_id"].notna() & out["symbol_id"].ne("") & out["trade_date"].notna()].copy()


def _table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def _upsert(conn: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame, keys: list[str]) -> int:
    if frame.empty:
        return 0
    columns = [column for column in _table_columns(conn, table) if column in frame.columns and column not in {"created_at", "updated_at"}]
    if not columns:
        return 0
    data = frame[columns].copy()
    for column in _JSON_COLUMNS & set(data.columns):
        data.loc[:, column] = data[column].map(_json_array)
    conn.register("decision_history_frame", data)
    try:
        updates = [column for column in columns if column not in keys]
        update_sql = ", ".join(f"{column}=excluded.{column}" for column in updates)
        if "updated_at" in _table_columns(conn, table):
            update_sql += (", " if update_sql else "") + "updated_at=now()"
        conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) SELECT {', '.join(columns)} FROM decision_history_frame "
            f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {update_sql}"
        )
    finally:
        conn.unregister("decision_history_frame")
    return len(data)


class DecisionHistoryRepository:
    """Uses RegistryStore transactions; it never opens an independent DB path."""

    def __init__(self, registry: Any):
        self.registry = registry

    @staticmethod
    def normalize_mode(value: Any) -> str:
        mode = str(value or "LIVE").upper()
        if mode not in {"LIVE", "REPLAY", "BACKFILL", "REBUILD_CURRENT"}:
            raise ValueError(f"Unsupported decision_write_mode: {value}")
        return mode

    def persist_rank_outputs(self, context: Any, outputs: dict[str, pd.DataFrame]) -> dict[str, Any]:
        params = dict(context.params or {})
        rank = _base(outputs.get("ranked_signals", pd.DataFrame()), run_date=context.run_date)
        stage_candidate = outputs.get("stock_scan", pd.DataFrame())
        if not isinstance(stage_candidate, pd.DataFrame) or stage_candidate.empty:
            stage_candidate = rank
        stage_source = _base(stage_candidate, run_date=context.run_date)
        stage1 = _base(outputs.get("stage1_scan", pd.DataFrame()), run_date=context.run_date)
        pattern = _base(outputs.get("pattern_scan", pd.DataFrame()), run_date=context.run_date)
        universe_id = str(params.get("universe_id") or params.get("rank_universe_id") or "NSE_OPERATIONAL")
        rank_version = str(params.get("rank_model_version") or params.get("rank_mode") or "baseline_v1")
        rank_formula = str(params.get("rank_formula_name") or "WEIGHTED_COMPOSITE")
        rank_hash = _hash({"rank_model_version": rank_version, "rank_mode": params.get("rank_mode"), "factor_weights": params.get("rank_factor_weights")})
        stage_version = str(params.get("stage_model_version") or "daily_stage_v1")
        stage_hash = _hash({"stage_model_version": stage_version, "weekly_stage_gate": params.get("weekly_stage_gate")})
        pattern_version = str(params.get("pattern_model_version") or "pattern_v1")
        pattern_hash = _hash({key: value for key, value in params.items() if str(key).startswith("pattern_")})

        if not rank.empty:
            rank.loc[:, "universe_id"] = universe_id
            implicit_position = pd.Series(range(1, len(rank) + 1), index=rank.index, dtype="int64")
            rank.loc[:, "rank_position"] = _first(rank, ("rank_position", "rank"), implicit_position)
            rank.loc[:, "rank_percentile"] = _first(rank, ("rank_percentile", "active_rank_pctile"))
            rank.loc[:, "rs_score"] = _first(rank, ("relative_strength", "rel_strength_score", "rs_score"))
            rank.loc[:, "volume_score"] = _first(rank, ("vol_intensity_score", "volume_intensity", "volume_score"))
            rank.loc[:, "trend_score"] = _first(rank, ("trend_score_score", "trend_persistence", "trend_score"))
            rank.loc[:, "proximity_score"] = _first(rank, ("prox_high_score", "proximity_to_highs", "proximity_score"))
            rank.loc[:, "sector_score"] = _first(rank, ("sector_strength_score", "sector_strength", "sector_score"))
            rank.loc[:, "rank_model_version"], rank.loc[:, "rank_formula_name"], rank.loc[:, "rank_config_hash"] = rank_version, rank_formula, rank_hash
        if not stage_source.empty:
            stage_source.loc[:, "trade_date"] = pd.Timestamp(context.run_date).date()
            stage_source.loc[:, "stage_family"] = _first(stage_source, ("stage_family",), "BROAD_STAGE")
            stage_source.loc[:, "stage_label"] = _first(stage_source, ("stage_label", "weekly_stage_label", "stage2_label"))
            stage_source.loc[:, "stage_confidence"] = _first(stage_source, ("stage_confidence", "stage_score", "stage2_score"))
            stage_source.loc[:, "stage_model_version"], stage_source.loc[:, "stage_config_hash"] = stage_version, stage_hash
        if not stage1.empty:
            aliases = {
                "stage1_model_status": ("stage1_model_status", "model_status"),
                "structural_repair_score": ("structural_repair_score", "stage1_structural_repair_score"),
                "accumulation_score": ("accumulation_score", "stage1_accumulation_score"),
                "rs_acceleration_score": ("rs_acceleration_score", "stage1_rs_acceleration_score"),
                "base_quality_score": ("base_quality_score", "stage1_base_quality_score"),
                "sector_rotation_score": ("sector_rotation_score", "stage1_sector_rotation_score"),
                "pattern_readiness_score": ("pattern_readiness_score", "stage1_pattern_readiness_score"),
                "golden_cross_progression_score": ("golden_cross_progression_score", "stage1_golden_cross_progression_score"),
                "ma_gap_pct": ("ma_gap_pct", "sma50_sma200_gap_pct"),
                "ma_gap_delta_5d": ("ma_gap_delta_5d", "sma50_sma200_gap_delta_5d"),
                "ma_gap_delta_20d": ("ma_gap_delta_20d", "sma50_sma200_gap_delta_20d"),
                "ma_gap_delta_60d": ("ma_gap_delta_60d", "sma50_sma200_gap_delta_60d"),
            }
            for target, sources in aliases.items():
                stage1.loc[:, target] = _first(stage1, sources)
            stage1.loc[:, "stage1_model_version"] = _first(stage1, ("stage1_model_version",), "v1").fillna("v1")
            stage1.loc[:, "stage1_config_hash"] = _first(stage1, ("stage1_config_hash",), _hash(params.get("stage1_maturity", {}))).fillna(_hash(params.get("stage1_maturity", {})))
        if not pattern.empty:
            pattern.loc[:, "pattern_family"] = _first(pattern, ("pattern_family", "pattern_type"), "UNKNOWN").fillna("UNKNOWN")
            pattern.loc[:, "pattern_model_version"], pattern.loc[:, "pattern_config_hash"] = pattern_version, pattern_hash
            pattern.loc[:, "pivot_price"] = _first(pattern, ("pivot_price", "breakout_level"))

        for frame in (rank, stage_source, stage1, pattern):
            if not frame.empty:
                frame.loc[:, "pipeline_run_id"] = context.run_id
                frame.loc[:, "source_attempt"] = context.attempt_number

        summary: dict[str, Any] = {"pipeline_run_id": context.run_id, "trade_date": context.run_date, "persistence_valid": False, "validation_errors": []}
        with self.registry._writer() as conn:  # noqa: SLF001
            summary["same_date_rerun_detected"] = bool(
                conn.execute(
                    "SELECT COUNT(*) FROM rank_history WHERE trade_date=CAST(? AS DATE) AND rank_model_version=?",
                    [context.run_date, rank_version],
                ).fetchone()[0]
            )
            summary["rank_history_rows_upserted"] = _upsert(conn, "rank_history", rank, ["symbol_id", "exchange", "trade_date", "universe_id", "rank_model_version"])
            summary["stage_history_rows_upserted"] = _upsert(conn, "stage_history", stage_source, ["symbol_id", "exchange", "trade_date", "stage_model_version"])
            summary["stage1_history_rows_upserted"] = _upsert(conn, "stage1_history", stage1, ["symbol_id", "exchange", "trade_date", "stage1_model_version"])
            summary["pattern_history_rows_upserted"] = _upsert(conn, "pattern_history", pattern, ["symbol_id", "exchange", "trade_date", "pattern_family", "pattern_model_version"])
            for table, keys in (("rank_history", "symbol_id, exchange, trade_date, universe_id, rank_model_version"), ("stage_history", "symbol_id, exchange, trade_date, stage_model_version"), ("stage1_history", "symbol_id, exchange, trade_date, stage1_model_version"), ("pattern_history", "symbol_id, exchange, trade_date, pattern_family, pattern_model_version")):
                duplicate_count = conn.execute(f"SELECT COUNT(*) FROM (SELECT {keys}, COUNT(*) n FROM {table} GROUP BY {keys} HAVING n > 1)").fetchone()[0]
                if duplicate_count:
                    summary["validation_errors"].append(f"{table}: duplicate keys={duplicate_count}")
            if summary["validation_errors"]:
                raise RuntimeError("Decision persistence validation failed: " + "; ".join(summary["validation_errors"]))
        summary["persistence_valid"] = True
        return summary

    def persist_lifecycle(self, context: Any, state: pd.DataFrame, transitions: pd.DataFrame) -> dict[str, Any]:
        mode = self.normalize_mode(context.params.get("decision_write_mode"))
        lifecycle = _base(state, run_date=context.run_date)
        events = _base(transitions, run_date=context.run_date)
        for frame in (lifecycle, events):
            if not frame.empty:
                frame.loc[:, "exchange"] = frame["exchange"].fillna("NSE")
                frame.loc[:, "pipeline_run_id"] = context.run_id
                frame.loc[:, "run_id"] = context.run_id
                frame.loc[:, "source_attempt"] = context.attempt_number
                frame.loc[:, "attempt_number"] = context.attempt_number
        if not lifecycle.empty:
            lifecycle.loc[:, "stage1_lifecycle_model_version"] = _first(lifecycle, ("stage1_lifecycle_model_version",), "v1").fillna("v1")
            lifecycle.loc[:, "stage1_lifecycle_config_hash"] = _first(lifecycle, ("stage1_lifecycle_config_hash",), "unknown").fillna("unknown")
        if not events.empty:
            events.loc[:, "stage1_lifecycle_model_version"] = _first(events, ("stage1_lifecycle_model_version",), "v1").fillna("v1")
            events.loc[:, "stage1_lifecycle_config_hash"] = _first(events, ("stage1_lifecycle_config_hash",), "unknown").fillna("unknown")
            events.loc[:, "transition_id"] = events.apply(lambda row: _hash([row.get(name) for name in ("symbol_id", "exchange", "trade_date", "from_lifecycle_state", "to_lifecycle_state", "transition_type", "stage1_lifecycle_model_version")]), axis=1)
        summary = {"pipeline_run_id": context.run_id, "trade_date": context.run_date, "decision_write_mode": mode, "persistence_valid": False, "validation_errors": []}
        if "execution_eligible" in lifecycle.columns and lifecycle["execution_eligible"].astype("string").str.lower().isin({"true", "1", "yes"}).any():
            raise RuntimeError("Stage-1 lifecycle persistence cannot make rows execution eligible")
        with self.registry._writer() as conn:  # noqa: SLF001
            summary["same_date_rerun_detected"] = bool(
                conn.execute(
                    "SELECT COUNT(*) FROM investigator_stage1_state WHERE trade_date=CAST(? AS DATE)",
                    [context.run_date],
                ).fetchone()[0]
            )
            summary["stage1_state_rows_upserted"] = _upsert(conn, "investigator_stage1_state", lifecycle, ["symbol_id", "exchange", "trade_date", "stage1_lifecycle_model_version"])
            before = conn.execute("SELECT COUNT(*) FROM investigator_stage1_transition").fetchone()[0]
            if not events.empty:
                columns = [column for column in _table_columns(conn, "investigator_stage1_transition") if column in events.columns and column != "created_at"]
                data = events[columns].copy()
                for column in _JSON_COLUMNS & set(data.columns):
                    data.loc[:, column] = data[column].map(_json_array)
                conn.register("decision_transition_frame", data)
                try:
                    conn.execute(f"INSERT OR IGNORE INTO investigator_stage1_transition ({', '.join(columns)}) SELECT {', '.join(columns)} FROM decision_transition_frame")
                finally:
                    conn.unregister("decision_transition_frame")
            after = conn.execute("SELECT COUNT(*) FROM investigator_stage1_transition").fetchone()[0]
            summary["stage1_transition_rows_inserted"] = after - before
            summary["duplicate_transition_rows_skipped"] = len(events) - (after - before)
            current_written = 0
            if mode in {"LIVE", "REBUILD_CURRENT"} and not lifecycle.empty:
                current = lifecycle.copy()
                current.loc[:, "as_of_trade_date"] = current["trade_date"]
                current.loc[:, "lifecycle_model_version"] = current["stage1_lifecycle_model_version"]
                current.loc[:, "lifecycle_config_hash"] = current["stage1_lifecycle_config_hash"]
                columns = [column for column in _table_columns(conn, "investigator_stage1_current") if column in current.columns and column not in {"created_at", "updated_at"}]
                data = current[columns].copy()
                for column in _JSON_COLUMNS & set(data.columns):
                    data.loc[:, column] = data[column].map(_json_array)
                conn.register("decision_current_frame", data)
                try:
                    updates = [column for column in columns if column not in {"symbol_id", "exchange"}]
                    conn.execute(
                        f"INSERT INTO investigator_stage1_current ({', '.join(columns)}) SELECT {', '.join(columns)} FROM decision_current_frame "
                        f"ON CONFLICT(symbol_id, exchange) DO UPDATE SET {', '.join(f'{c}=excluded.{c}' for c in updates)}, updated_at=now() "
                        "WHERE excluded.as_of_trade_date >= investigator_stage1_current.as_of_trade_date"
                    )
                finally:
                    conn.unregister("decision_current_frame")
                current_written = len(data)
            summary["stage1_current_rows_upserted"] = current_written
            summary.update(self._reconcile(conn))
            current_is_stale = mode in {"LIVE", "REBUILD_CURRENT"} and summary["current_rows_older_than_latest_history"]
            if summary["duplicate_current_keys"] or summary["current_rows_ahead_of_history"] or summary["current_state_mismatches"] or current_is_stale:
                summary["validation_errors"].append("current-state reconciliation failed")
            if summary["validation_errors"]:
                raise RuntimeError("Lifecycle persistence validation failed: " + "; ".join(summary["validation_errors"]))
        summary["persistence_valid"] = True
        return summary

    @staticmethod
    def _reconcile(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
        duplicate = conn.execute("SELECT COUNT(*) FROM (SELECT symbol_id, exchange, COUNT(*) n FROM investigator_stage1_current GROUP BY 1,2 HAVING n > 1)").fetchone()[0]
        ahead = conn.execute("SELECT COUNT(*) FROM investigator_stage1_current c LEFT JOIN investigator_stage1_state h ON h.symbol_id=c.symbol_id AND h.exchange=c.exchange AND h.trade_date=c.as_of_trade_date WHERE h.symbol_id IS NULL").fetchone()[0]
        mismatch = conn.execute("SELECT COUNT(*) FROM investigator_stage1_current c JOIN investigator_stage1_current_derived d USING(symbol_id, exchange) WHERE c.as_of_trade_date=d.trade_date AND c.stage1_lifecycle_state IS DISTINCT FROM d.stage1_lifecycle_state").fetchone()[0]
        older = conn.execute("SELECT COUNT(*) FROM investigator_stage1_current c JOIN (SELECT symbol_id, exchange, MAX(trade_date) latest FROM investigator_stage1_state GROUP BY 1,2) h USING(symbol_id, exchange) WHERE c.as_of_trade_date < h.latest").fetchone()[0]
        return {"duplicate_current_keys": duplicate, "current_rows_ahead_of_history": ahead, "current_state_mismatches": mismatch, "current_rows_older_than_latest_history": older}

    def _history(self, table: str, symbol_id: str, start_date: str | None, end_date: str | None, exchange: str | None = None) -> pd.DataFrame:
        clauses, params = ["symbol_id = ?"], [symbol_id.upper()]
        if exchange:
            clauses.append("exchange = ?")
            params.append(exchange)
        if start_date:
            clauses.append("trade_date >= CAST(? AS DATE)")
            params.append(start_date)
        if end_date:
            clauses.append("trade_date <= CAST(? AS DATE)")
            params.append(end_date)
        with self.registry._reader() as conn:  # noqa: SLF001
            return conn.execute(f"SELECT * FROM {table} WHERE {' AND '.join(clauses)} ORDER BY trade_date", params).fetchdf()

    def get_rank_history(self, symbol_id: str, start_date: str | None = None, end_date: str | None = None, *, exchange: str | None = None) -> pd.DataFrame: return self._history("rank_history", symbol_id, start_date, end_date, exchange)
    def get_stage_history(self, symbol_id: str, start_date: str | None = None, end_date: str | None = None, *, exchange: str | None = None) -> pd.DataFrame: return self._history("stage_history", symbol_id, start_date, end_date, exchange)
    def get_stage1_history(self, symbol_id: str, start_date: str | None = None, end_date: str | None = None, *, exchange: str | None = None) -> pd.DataFrame: return self._history("stage1_history", symbol_id, start_date, end_date, exchange)
    def get_stage1_transitions(self, symbol_id: str, start_date: str | None = None, end_date: str | None = None, *, exchange: str | None = None) -> pd.DataFrame: return self._history("investigator_stage1_transition", symbol_id, start_date, end_date, exchange)
    def get_pattern_history(self, symbol_id: str, start_date: str | None = None, end_date: str | None = None, *, exchange: str | None = None) -> pd.DataFrame: return self._history("pattern_history", symbol_id, start_date, end_date, exchange)

    def get_stage1_current_state(self, symbol_id: str, *, exchange: str = "NSE") -> pd.DataFrame:
        with self.registry._reader() as conn:  # noqa: SLF001
            return conn.execute("SELECT * FROM investigator_stage1_current WHERE symbol_id = ? AND exchange = ?", [symbol_id.upper(), exchange]).fetchdf()

    def get_stage1_history_for_dates(self, start_date: str, end_date: str) -> pd.DataFrame:
        with self.registry._reader() as conn:  # noqa: SLF001
            return conn.execute("SELECT * FROM stage1_history WHERE trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE) ORDER BY trade_date, symbol_id", [start_date, end_date]).fetchdf()

    def get_stage1_cohort(self, trade_date: str, *, minimum_score: float | None = None) -> pd.DataFrame:
        sql, params = "SELECT * FROM stage1_history WHERE trade_date = CAST(? AS DATE)", [trade_date]
        if minimum_score is not None:
            sql += " AND stage1_maturity_score >= ?"
            params.append(minimum_score)
        with self.registry._reader() as conn:  # noqa: SLF001
            return conn.execute(sql + " ORDER BY stage1_emerging_rank NULLS LAST", params).fetchdf()

    def get_stage_transition_cohort(self, from_state: str, to_state: str, start_date: str, end_date: str) -> pd.DataFrame:
        with self.registry._reader() as conn:  # noqa: SLF001
            return conn.execute("SELECT * FROM investigator_stage1_transition WHERE from_lifecycle_state = ? AND to_lifecycle_state = ? AND trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE) ORDER BY trade_date, symbol_id", [from_state, to_state, start_date, end_date]).fetchdf()
