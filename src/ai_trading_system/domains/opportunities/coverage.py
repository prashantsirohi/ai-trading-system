"""Full-universe weekly structural coverage for Phase 3B shadow routing."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.opportunities.contracts import WeinsteinStage
from ai_trading_system.domains.opportunities.routing import (
    ScanRoutingConfig,
    StageCoverageConfig,
    StageDiscoveryReason,
)
from ai_trading_system.domains.ranking.stage_classifier import classify_latest
from ai_trading_system.domains.ranking.weekly import to_weekly


LEGACY_STAGE_MAP = {
    "S1": WeinsteinStage.STAGE_1,
    "S2": WeinsteinStage.STAGE_2,
    "S3": WeinsteinStage.STAGE_3,
    "S4": WeinsteinStage.STAGE_4,
    "UNDEFINED": WeinsteinStage.UNKNOWN,
}


def load_daily_universe(
    ohlcv_db_path: Path,
    *,
    exchange: str,
    as_of: str,
) -> pd.DataFrame:
    """Load all non-benchmark daily bars as-of without a rank-derived cap."""
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        columns = {row[0] for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = '_catalog'"
        ).fetchall()}
        benchmark = "AND NOT COALESCE(is_benchmark, FALSE)" if "is_benchmark" in columns else ""
        return conn.execute(
            f"""
            SELECT symbol_id, exchange, timestamp, open, high, low, close, volume
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) - INTERVAL 800 DAY AND CAST(? AS DATE)
              {benchmark}
            ORDER BY symbol_id, timestamp
            """,
            [exchange, as_of, as_of],
        ).fetchdf()
    finally:
        conn.close()


def load_sector_mapping(master_db_path: Path) -> tuple[dict[str, tuple[str, str]], list[str]]:
    """Return latest-only observed sector mapping and its limitations."""
    if not master_db_path.exists():
        return {}, ["masterdata_missing"]
    conn = sqlite3.connect(str(master_db_path))
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(symbols)").fetchall()}
        if not columns:
            return {}, ["symbols_table_missing"]
        sector_expr = "COALESCE(sector, industry, '')" if "industry" in columns else "COALESCE(sector, '')"
        rows = conn.execute(
            f"SELECT UPPER(symbol_id), {sector_expr} FROM symbols WHERE symbol_id IS NOT NULL"
        ).fetchall()
        mapping = {
            str(symbol).strip().upper(): (str(sector).strip().lower().replace(" ", "_"), str(sector).strip())
            for symbol, sector in rows if str(sector or "").strip()
        }
        return mapping, ["sector_membership_latest_only"]
    finally:
        conn.close()


def is_completed_trading_week(run_date: date, master_db_path: Path) -> bool:
    """Whether run_date is the final scheduled session in its Monday-Friday week."""
    if run_date.weekday() >= 4:
        return True
    holidays: set[date] = set()
    if master_db_path.exists():
        conn = sqlite3.connect(str(master_db_path))
        try:
            exists = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='nse_holidays'"
            ).fetchone()[0]
            if exists:
                holidays = {
                    date.fromisoformat(str(row[0])[:10])
                    for row in conn.execute("SELECT date FROM nse_holidays").fetchall()
                }
        finally:
            conn.close()
    remaining = [run_date + pd.Timedelta(days=offset) for offset in range(1, 5 - run_date.weekday())]
    return all(pd.Timestamp(day).date() in holidays for day in remaining)


def build_stage_coverage(
    daily: pd.DataFrame,
    *,
    as_of: str,
    sector_mapping: dict[str, tuple[str, str]],
    config: StageCoverageConfig,
    lock_current_week: bool,
    market_regime: str = "unknown",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Classify every eligible symbol and retain explicit exclusion reasons."""
    rows: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    if daily.empty:
        return pd.DataFrame(), pd.DataFrame(columns=["symbol_id", "reason", "scope"])
    turnover_by_symbol = {
        str(symbol).strip().upper(): float((frame["close"] * frame["volume"]).tail(60).median())
        for symbol, frame in daily.groupby("symbol_id", sort=True)
    }
    positive_turnover = pd.Series([value for value in turnover_by_symbol.values() if value > 0], dtype=float)
    liquidity_floor = float(positive_turnover.quantile(config.minimum_liquidity_score)) if not positive_turnover.empty else 0.0
    for symbol, frame in daily.groupby("symbol_id", sort=True):
        symbol = str(symbol).strip().upper()
        frame = frame.sort_values("timestamp").copy()
        latest = frame.iloc[-1]
        if not _valid_ohlcv(frame):
            exclusions.append(_exclusion(symbol, "invalid_ohlcv"))
            continue
        median_turnover = turnover_by_symbol[symbol]
        if float(latest["close"]) < config.minimum_price or median_turnover <= 0 or median_turnover < liquidity_floor:
            exclusions.append(_exclusion(symbol, "illiquid"))
            continue
        weekly = to_weekly(frame.rename(columns={"timestamp": "date"}))
        if len(frame) < 180 or len(weekly) < 30:
            exclusions.append(_exclusion(symbol, "insufficient_weekly_history"))
            continue
        sector_id, sector_name = sector_mapping.get(symbol, ("", ""))
        if not sector_name:
            exclusions.append(_exclusion(symbol, "missing_sector_mapping", scope="sector"))
        source_sessions = frame.assign(_week=pd.to_datetime(frame["timestamp"]).dt.to_period("W-FRI"))
        current_period = source_sessions["_week"].iloc[-1]
        current_sessions = source_sessions.loc[source_sessions["_week"].eq(current_period), "timestamp"]
        source_start = pd.Timestamp(current_sessions.min()).date()
        source_end = pd.Timestamp(current_sessions.max()).date()

        prior_weekly = weekly.iloc[:-1]
        prior_result = classify_latest(prior_weekly, symbol=symbol) if len(prior_weekly) >= 30 else None
        prior_stage = prior_result.stage_label if prior_result else None
        current = classify_latest(weekly, symbol=symbol, prior_stage=prior_stage)
        current_stage = LEGACY_STAGE_MAP.get(current.stage_label, WeinsteinStage.UNKNOWN)
        previous_locked = LEGACY_STAGE_MAP.get(prior_stage or "UNDEFINED", WeinsteinStage.UNKNOWN)
        provisional_stage = current_stage
        locked_stage = current_stage if lock_current_week else previous_locked
        if not lock_current_week and previous_locked is WeinsteinStage.STAGE_1 and current_stage is WeinsteinStage.STAGE_2:
            provisional_stage = WeinsteinStage.TRANSITION_1_TO_2
        if not lock_current_week and previous_locked is WeinsteinStage.STAGE_2 and current_stage is WeinsteinStage.STAGE_3:
            provisional_stage = WeinsteinStage.TRANSITION_2_TO_3
        confidence = round(float(current.stage_confidence) * 100.0, 2)
        effective = provisional_stage if lock_current_week or confidence >= 75.0 else locked_stage
        if effective is WeinsteinStage.UNKNOWN:
            confidence = 0.0
        status = "locked" if lock_current_week else "provisional"
        ma30 = _float(current.ma30w)
        close = float(weekly["close"].iloc[-1])
        rs = _return_pct(weekly["close"], 13)
        rs_slope = rs - _return_pct(weekly["close"].iloc[:-1], 13)
        slope = _float(current.ma30w_slope_4w)
        previous_slope = _float(weekly["ma30w_slope_4w"].iloc[-2])
        base = _base_features(weekly)
        payload = {
            "symbol_id": symbol,
            "exchange": str(latest["exchange"]),
            "sector_id": sector_id or None,
            "sector_name": sector_name or None,
            "market_regime": market_regime,
            "as_of": as_of,
            "source_week_start": source_start,
            "source_week_end": source_end,
            "provisional_stage": provisional_stage.value,
            "locked_stage": locked_stage.value,
            "effective_stage": effective.value,
            "stage_status": status,
            "stage_confidence_score": confidence,
            "stage_confidence_band": _confidence_band(confidence),
            "confidence_components": json.dumps({"legacy_classifier_score": confidence, "ma_slope": slope, "weekly_rs_slope": rs_slope}, sort_keys=True),
            "previous_locked_stage": previous_locked.value,
            "stage_transition": _transition(previous_locked, effective),
            "stage_transition_reason": "weekly_structure_changed" if previous_locked != effective else "no_change",
            "weeks_in_locked_stage": int(current.bars_in_stage if lock_current_week else (prior_result.bars_in_stage if prior_result else 0)),
            "provisional_persistence_days": max((date.fromisoformat(as_of) - source_start).days, 0) if not lock_current_week else 0,
            "weekly_close": close,
            "weekly_ma_30": ma30,
            "weekly_ma_30_slope": slope,
            "weekly_ma_30_slope_acceleration": None if slope is None or previous_slope is None else slope - previous_slope,
            "price_vs_weekly_ma_30_pct": None if not ma30 else (close / ma30 - 1.0) * 100.0,
            "weekly_rs": rs,
            "weekly_rs_slope": rs_slope,
            "weekly_rs_vs_sector": None,
            "base_age_weeks": base["base_duration_weeks"],
            "failed_breakout_count": base["failed_breakout_count"],
            "classifier_version": config.stage_classifier_version,
            "confidence_formula_version": config.confidence_formula_version,
            "median_turnover": median_turnover,
            **base,
        }
        payload["source_artifact_hash"] = _hash(payload)
        rows.append(payload)
    output = pd.DataFrame(rows)
    if not output.empty:
        sector_median = output.groupby("sector_id", dropna=False)["weekly_rs"].transform("median")
        output.loc[:, "weekly_rs_vs_sector"] = pd.to_numeric(output["weekly_rs"], errors="coerce") - sector_median
        for index, row in output.iterrows():
            payload = row.drop(labels=["source_artifact_hash"]).to_dict()
            output.at[index, "source_artifact_hash"] = _hash(payload)
    return output, pd.DataFrame(exclusions)


def build_sector_coverage(stock: pd.DataFrame, *, config: StageCoverageConfig) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    mapped = stock.loc[stock.get("sector_id", pd.Series(index=stock.index, dtype=object)).notna()].copy()
    for sector_id, group in mapped.groupby("sector_id", sort=True):
        stages = group["effective_stage"].astype(str)
        eligible = len(group)
        classified = int(stages.ne(WeinsteinStage.UNKNOWN.value).sum())
        coverage = classified / eligible if eligible else 0.0
        pct = lambda value: round(float(stages.eq(value).mean() * 100.0), 2)  # noqa: E731
        above_rising = (
            pd.to_numeric(group["price_vs_weekly_ma_30_pct"], errors="coerce").gt(0)
            & pd.to_numeric(group["weekly_ma_30_slope"], errors="coerce").gt(0)
        )
        velocity = float(pd.to_numeric(group["weekly_ma_30_slope_acceleration"], errors="coerce").median() or 0.0)
        if eligible < config.minimum_sector_constituents or coverage < config.minimum_sector_stage_coverage_ratio:
            effective = WeinsteinStage.UNKNOWN
        elif pct(WeinsteinStage.STAGE_4.value) >= 40:
            effective = WeinsteinStage.STAGE_4
        elif pct(WeinsteinStage.STAGE_3.value) + pct(WeinsteinStage.TRANSITION_2_TO_3.value) >= 35 and velocity < 0:
            effective = WeinsteinStage.STAGE_3
        elif pct(WeinsteinStage.STAGE_2.value) >= 50 and float(above_rising.mean()) >= 0.60:
            effective = WeinsteinStage.STAGE_2
        elif pct(WeinsteinStage.TRANSITION_1_TO_2.value) >= 20 and velocity > 0:
            effective = WeinsteinStage.TRANSITION_1_TO_2
        else:
            effective = WeinsteinStage.STAGE_1
        confidence = (
            0.0
            if effective is WeinsteinStage.UNKNOWN
            else round(min(100.0, coverage * 70.0 + abs(velocity) * 300.0), 2)
        )
        row = {
            "sector_id": sector_id,
            "sector_name": str(group["sector_name"].iloc[0]),
            "as_of": group["as_of"].iloc[0],
            "source_week_start": group["source_week_start"].min(),
            "source_week_end": group["source_week_end"].max(),
            "provisional_stage": effective.value if group["stage_status"].eq("provisional").any() else WeinsteinStage.UNKNOWN.value,
            "locked_stage": effective.value if group["stage_status"].eq("locked").all() else WeinsteinStage.UNKNOWN.value,
            "effective_stage": effective.value,
            "stage_status": "provisional" if group["stage_status"].eq("provisional").any() else "locked",
            "stage_confidence_score": confidence,
            "stage_confidence_band": _confidence_band(confidence),
            "pct_stage_1": pct(WeinsteinStage.STAGE_1.value),
            "pct_transition_1_to_2": pct(WeinsteinStage.TRANSITION_1_TO_2.value),
            "pct_stage_2": pct(WeinsteinStage.STAGE_2.value),
            "pct_transition_2_to_3": pct(WeinsteinStage.TRANSITION_2_TO_3.value),
            "pct_stage_3": pct(WeinsteinStage.STAGE_3.value),
            "pct_stage_4": pct(WeinsteinStage.STAGE_4.value),
            "pct_above_rising_30w_ma": round(float(above_rising.mean() * 100.0), 2),
            "median_weekly_rs_slope": _float(pd.to_numeric(group["weekly_rs_slope"], errors="coerce").median()),
            "median_price_vs_30w_ma": _float(pd.to_numeric(group["price_vs_weekly_ma_30_pct"], errors="coerce").median()),
            "stage_breadth_velocity": velocity,
            "eligible_constituents": eligible,
            "classified_constituents": classified,
            "coverage_ratio": coverage,
            "classifier_version": config.stage_classifier_version,
            "aggregation_rule_version": config.sector_stage_rule_version,
        }
        row["source_artifact_hash"] = _hash(row)
        rows.append(row)
    return pd.DataFrame(rows)


def build_light_pattern_scan(stock: pd.DataFrame, *, config: ScanRoutingConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    allowed = {WeinsteinStage.STAGE_1.value, WeinsteinStage.TRANSITION_1_TO_2.value}
    rows: list[dict[str, Any]] = []
    promoted: list[dict[str, Any]] = []
    for row in stock.loc[stock["effective_stage"].isin(allowed)].to_dict(orient="records"):
        reasons: list[str] = []
        if row["effective_stage"] == WeinsteinStage.STAGE_1.value:
            reasons.append(StageDiscoveryReason.STAGE_1_BASE.value)
        else:
            reasons.append(StageDiscoveryReason.TRANSITION_1_TO_2.value)
        contraction = float(row.get("weekly_range_contraction") or 0.0)
        dry_up = float(row.get("volume_dry_up") or 0.0)
        pivot_distance = float(row.get("distance_to_pivot_pct") or 999.0)
        if contraction >= 0.25:
            reasons.append(StageDiscoveryReason.BASE_CONTRACTION.value)
        if dry_up >= 0.20:
            reasons.append(StageDiscoveryReason.VOLUME_DRY_UP.value)
        if pivot_distance <= config.light_pattern_pivot_distance_threshold:
            reasons.append(StageDiscoveryReason.PIVOT_APPROACH.value)
        score = min(100.0, contraction * 35.0 + dry_up * 30.0 + max(0.0, 20.0 - pivot_distance) * 1.5 + (15.0 if row["effective_stage"] == WeinsteinStage.TRANSITION_1_TO_2.value else 0.0))
        eligible = (
            float(row.get("stage_confidence_score") or 0.0) >= config.stage_discovery_confidence_threshold
            and int(row.get("base_duration_weeks") or 0) >= config.light_pattern_min_base_weeks
            and float(row.get("base_depth_pct") or 999.0) <= config.light_pattern_max_base_depth
            and len(reasons) >= 2
        )
        promoted_flag = eligible and (
            score >= config.light_pattern_score_threshold
            or pivot_distance <= config.light_pattern_pivot_distance_threshold
            or row["effective_stage"] == WeinsteinStage.TRANSITION_1_TO_2.value
        )
        output = {
            **row,
            "discovery_reasons": "|".join(reasons),
            "light_pattern_score": round(score, 2),
            "stage_discovery_eligible": eligible,
            "stage_promoted": promoted_flag,
            "light_pattern_rule_version": config.light_pattern_rule_version,
        }
        rows.append(output)
        if promoted_flag:
            promoted.append(output)
    return pd.DataFrame(rows), pd.DataFrame(promoted)


def persist_stage_history(registry: Any, stock: pd.DataFrame, sector: pd.DataFrame, *, run_id: str, attempt: int) -> None:
    with registry._writer() as conn:  # noqa: SLF001
        for row in stock.to_dict(orient="records"):
            source_hash = str(row["source_artifact_hash"])
            observation_id = hashlib.sha256(f"stock|{row['exchange']}|{row['symbol_id']}|{row['source_week_end']}|{row['stage_status']}|{row['classifier_version']}|{source_hash}".encode()).hexdigest()
            conn.execute(
                """INSERT INTO weekly_stock_stage_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                   ON CONFLICT(observation_id) DO NOTHING""",
                [observation_id, row["exchange"], row["symbol_id"], row.get("sector_id"), row.get("sector_name"), row["as_of"], row["source_week_start"], row["source_week_end"], row["stage_status"], row["effective_stage"], row["classifier_version"], source_hash, json.dumps(row, default=str, sort_keys=True), run_id, attempt],
            )
        for row in sector.to_dict(orient="records"):
            source_hash = str(row["source_artifact_hash"])
            observation_id = hashlib.sha256(f"sector|{row['sector_id']}|{row['source_week_end']}|{row['stage_status']}|{row['aggregation_rule_version']}|{source_hash}".encode()).hexdigest()
            conn.execute(
                """INSERT INTO weekly_sector_stage_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                   ON CONFLICT(observation_id) DO NOTHING""",
                [observation_id, row["sector_id"], row["sector_name"], row["as_of"], row["source_week_start"], row["source_week_end"], row["stage_status"], row["effective_stage"], row["aggregation_rule_version"], source_hash, json.dumps(row, default=str, sort_keys=True), run_id, attempt],
            )


def read_stock_stage_as_of(
    registry: Any,
    *,
    as_of: str,
    exchange: str = "NSE",
    symbols: list[str] | None = None,
) -> pd.DataFrame:
    """Reconstruct the latest universal stock-stage observation available as-of."""
    clauses = ["exchange = ?", "as_of <= CAST(? AS TIMESTAMP)"]
    params: list[Any] = [exchange, as_of]
    if symbols:
        clauses.append(f"symbol_id IN ({','.join('?' for _ in symbols)})")
        params.extend(str(symbol).upper() for symbol in symbols)
    with registry._reader() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"""SELECT observation_json FROM weekly_stock_stage_history
                WHERE {' AND '.join(clauses)}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY exchange, symbol_id
                    ORDER BY as_of DESC, source_week_end DESC, created_at DESC
                ) = 1""",
            params,
        ).fetchall()
    return pd.DataFrame([json.loads(row[0]) for row in rows])


def _base_features(weekly: pd.DataFrame) -> dict[str, Any]:
    window = weekly.tail(26)
    high = float(window["high"].max())
    low = float(window["low"].min())
    close = float(window["close"].iloc[-1])
    recent_range = ((window["high"] - window["low"]) / window["close"]).tail(4).mean()
    prior_range = ((window["high"] - window["low"]) / window["close"]).head(max(len(window) - 4, 1)).tail(8).mean()
    contraction = 0.0 if not prior_range or pd.isna(prior_range) else max(0.0, 1.0 - float(recent_range / prior_range))
    recent_volume = float(window["volume"].tail(4).mean())
    prior_volume = float(window["volume"].head(max(len(window) - 4, 1)).tail(8).mean())
    dry_up = 0.0 if not prior_volume or pd.isna(prior_volume) else max(0.0, 1.0 - recent_volume / prior_volume)
    returns = window["close"].pct_change()
    up_volume = float(window.loc[returns.gt(0), "volume"].mean() or 0.0)
    down_volume = float(window.loc[returns.le(0), "volume"].mean() or 0.0)
    return {
        "base_duration_weeks": int(min(len(window), 26)),
        "base_depth_pct": 0.0 if high <= 0 else (high - low) / high * 100.0,
        "weekly_range_contraction": contraction,
        "contraction_count": int(((window["high"] - window["low"]) / window["close"]).diff().lt(0).tail(8).sum()),
        "volatility_contraction": contraction,
        "volume_dry_up": dry_up,
        "up_week_down_week_volume_ratio": None if down_volume <= 0 else up_volume / down_volume,
        "pivot_price": high,
        "distance_to_pivot_pct": 0.0 if close <= 0 else max(0.0, (high - close) / close * 100.0),
        "failed_breakout_count": int((window["high"].gt(window["high"].shift(1).rolling(10).max()) & window["close"].lt(window["open"])).sum()),
        "relative_strength_improving": bool(_return_pct(window["close"], 13) > _return_pct(window["close"].iloc[:-1], 13)),
        "weekly_ma_slope_improving": bool(pd.to_numeric(window["ma30w_slope_4w"], errors="coerce").diff().iloc[-1] > 0),
    }


def _valid_ohlcv(frame: pd.DataFrame) -> bool:
    numeric = frame[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
    return bool(numeric.notna().all().all() and numeric["close"].gt(0).all() and numeric["volume"].ge(0).all() and numeric["high"].ge(numeric[["open", "close", "low"]].max(axis=1)).all() and numeric["low"].le(numeric[["open", "close", "high"]].min(axis=1)).all())


def _hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, default=str, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _exclusion(symbol: str, reason: str, *, scope: str = "stock") -> dict[str, str]:
    return {"symbol_id": symbol, "reason": reason, "scope": scope}


def _float(value: Any) -> float | None:
    try:
        return None if value is None or pd.isna(value) else float(value)
    except (TypeError, ValueError):
        return None


def _return_pct(series: pd.Series, periods: int) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) <= periods or float(values.iloc[-periods - 1]) == 0:
        return 0.0
    return float((values.iloc[-1] / values.iloc[-periods - 1] - 1.0) * 100.0)


def _confidence_band(score: float) -> str:
    if score >= 85:
        return "very_high"
    if score >= 70:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _transition(previous: WeinsteinStage, current: WeinsteinStage) -> str:
    return "none" if previous is current or previous is WeinsteinStage.UNKNOWN else f"{previous.value}_to_{current.value}"
