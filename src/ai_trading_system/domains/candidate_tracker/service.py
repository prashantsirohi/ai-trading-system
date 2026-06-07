"""Candidate lifecycle tracker for live monitoring.

This domain is intentionally separate from ``research.perf_tracker``.  The
perf tracker measures forward returns for historical cohorts; this module keeps
stateful live episodes for symbols that enter the candidates/fundamentals
shortlists and classifies their ongoing technical and fundamental health.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.fundamentals.enrich_rank import normalize_symbol


TRACKING_STATUSES = [
    "STRONG_IMPROVING",
    "IMPROVING",
    "STABLE",
    "WATCH_CAREFULLY",
    "DETERIORATING",
    "RESULT_FAILURE",
    "TECHNICAL_FAILURE",
    "REMOVE_FROM_TRACKING",
]


CURRENT_COLUMNS = [
    "episode_id",
    "symbol",
    "first_seen_date",
    "last_seen_date",
    "latest_candidate_group",
    "latest_watchlist_bucket",
    "status",
    "tracking_health_score",
    "technical_health_score",
    "fundamental_health_score",
    "valuation_history_score",
    "close",
    "return_since_first_seen",
    "max_return_since_first_seen",
    "drawdown_from_tracking_high",
    "composite_score",
    "relative_strength",
    "sector_strength",
    "near_52w_high_pct",
    "close_above_sma50",
    "close_above_sma200",
    "stage2_flag",
    "quarterly_result_score",
    "quarterly_result_bucket",
    "result_score_delta",
    "sales_yoy_delta",
    "operating_profit_yoy_delta",
    "profit_yoy_delta",
    "opm_yoy_change_delta_bps",
    "valuation_history_score_delta",
    "active",
]


SNAPSHOT_COLUMNS = [
    "episode_id",
    "symbol",
    "snapshot_date",
    "close",
    "return_since_first_seen",
    "max_return_since_first_seen",
    "drawdown_from_tracking_high",
    "composite_score",
    "relative_strength",
    "sector_strength",
    "near_52w_high_pct",
    "close_above_sma50",
    "close_above_sma200",
    "stage2_flag",
    "technical_health_score",
    "fundamental_health_score",
    "valuation_history_score",
    "tracking_health_score",
    "status",
    "status_reasons",
    "created_at",
]


REVIEW_COLUMNS = [
    "episode_id",
    "symbol",
    "report_date",
    "available_at",
    "review_date",
    "quarterly_result_score",
    "quarterly_result_bucket",
    "result_score_delta",
    "sales_yoy_pct",
    "sales_yoy_delta",
    "operating_profit_yoy_pct",
    "operating_profit_yoy_delta",
    "profit_yoy_pct",
    "profit_yoy_delta",
    "opm_yoy_change_bps",
    "opm_yoy_change_delta_bps",
    "valuation_history_score",
    "valuation_history_score_delta",
    "fundamental_health_score",
    "created_at",
]


ALERT_COLUMNS = [
    "alert_id",
    "episode_id",
    "symbol",
    "alert_date",
    "alert_type",
    "prior_status",
    "new_status",
    "severity",
    "message",
    "created_at",
]


@dataclass(frozen=True)
class CandidateTrackerConfig:
    db_path: Path
    ohlcv_db_path: Path | None = None
    run_date: str = ""
    run_id: str = ""
    max_age_days: int = 365
    review_window_days: int = 120
    archive_failures: bool = False


@dataclass(frozen=True)
class CandidateTrackerResult:
    current: pd.DataFrame
    snapshots: pd.DataFrame
    fundamental_reviews: pd.DataFrame
    alerts: pd.DataFrame
    summary: dict[str, Any]


def run_candidate_tracker(
    *,
    config: CandidateTrackerConfig,
    final_candidates: pd.DataFrame,
    watchlist_candidates: pd.DataFrame | None = None,
    quarterly_result_scores: pd.DataFrame | None = None,
    stock_valuation_bands_latest: pd.DataFrame | None = None,
    ranked_signals: pd.DataFrame | None = None,
    sector_dashboard: pd.DataFrame | None = None,
) -> CandidateTrackerResult:
    """Ingest candidate artifacts, update tracker DB, and return run outputs."""

    run_date = str(config.run_date or pd.Timestamp.utcnow().date())
    db_path = Path(config.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []

    candidates = _build_candidate_universe(final_candidates, watchlist_candidates)
    if candidates.empty:
        with _connect(db_path) as conn:
            ensure_schema(conn)
        empty_current = pd.DataFrame(columns=CURRENT_COLUMNS)
        return CandidateTrackerResult(
            current=empty_current,
            snapshots=pd.DataFrame(columns=SNAPSHOT_COLUMNS),
            fundamental_reviews=pd.DataFrame(columns=REVIEW_COLUMNS),
            alerts=pd.DataFrame(columns=ALERT_COLUMNS),
            summary={
                "status": "completed_empty",
                "run_date": run_date,
                "active_candidates": 0,
                "new_episodes": 0,
                "updated_episodes": 0,
                "snapshots": 0,
                "fundamental_reviews": 0,
                "alerts": 0,
                "warnings": ["no candidates found"],
            },
        )

    technical = _build_technical_frame(
        candidates=candidates,
        ranked_signals=ranked_signals,
        sector_dashboard=sector_dashboard,
        ohlcv_db_path=config.ohlcv_db_path,
        run_date=run_date,
        warnings=warnings,
    )
    valuation = _valuation_scores(stock_valuation_bands_latest)
    result_scores = _latest_results(quarterly_result_scores, run_date=run_date, window_days=config.review_window_days)

    now = _utc_now()
    new_episodes = 0
    updated_episodes = 0
    snapshots: list[dict[str, Any]] = []
    reviews: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []
    current_rows: list[dict[str, Any]] = []

    with _connect(db_path) as conn:
        ensure_schema(conn)
        prior_status_by_episode = _prior_status_by_episode(conn)

        for _, candidate in candidates.sort_values("symbol", kind="stable").iterrows():
            symbol = str(candidate["symbol"])
            episode = _active_episode(conn, symbol)
            tech = technical.get(symbol, {})
            close = _float_or_none(tech.get("close"))
            if episode is None:
                episode_id = _new_episode_id(conn, symbol, run_date)
                first_seen_close = close
                _insert_episode(conn, episode_id, symbol, candidate, run_date, first_seen_close, now)
                new_episodes += 1
                episode = _active_episode(conn, symbol)
            else:
                episode_id = str(episode["episode_id"])
                _update_episode_seen(conn, episode_id, candidate, run_date, now)
                updated_episodes += 1
                episode = _active_episode(conn, symbol)

            assert episode is not None
            episode_id = str(episode["episode_id"])
            first_seen_date = str(episode["first_seen_date"])
            first_seen_close = _float_or_none(episode.get("first_seen_close")) or close
            if first_seen_close is None and close is not None:
                first_seen_close = close
                conn.execute(
                    "UPDATE tracked_candidates SET first_seen_close = ? WHERE episode_id = ?",
                    [first_seen_close, episode_id],
                )

            valuation_score = valuation.get(symbol, 50.0)
            prior_snapshot = _prior_snapshot(conn, episode_id)
            review = _maybe_build_review(
                conn=conn,
                episode_id=episode_id,
                symbol=symbol,
                run_date=run_date,
                result_row=result_scores.get(symbol),
                valuation_history_score=valuation_score,
                now=now,
            )
            if review is not None:
                reviews.append(review)
            latest_review = review or _latest_review(conn, episode_id)

            snapshot = _build_snapshot(
                episode_id=episode_id,
                symbol=symbol,
                snapshot_date=run_date,
                close=close,
                first_seen_close=first_seen_close,
                prior_snapshot=prior_snapshot,
                tech=tech,
                latest_review=latest_review,
                valuation_history_score=valuation_score,
                now=now,
            )
            status, reasons = classify_candidate(
                snapshot=snapshot,
                latest_review=latest_review,
                prior_snapshot=prior_snapshot,
                last_seen_date=str(episode.get("last_seen_date") or run_date),
                run_date=run_date,
                review_window_days=config.review_window_days,
            )
            snapshot["status"] = status
            snapshot["status_reasons"] = "; ".join(reasons)
            snapshots.append(snapshot)
            _upsert_snapshot(conn, snapshot)

            prior_status = prior_status_by_episode.get(episode_id) or str(episode.get("latest_status") or "")
            if status != prior_status:
                alert = _build_alert(episode_id, symbol, run_date, prior_status, status, reasons, now)
                alerts.append(alert)
                _insert_alert(conn, alert)

            archive = bool(config.archive_failures and status == "REMOVE_FROM_TRACKING")
            _update_episode_status(conn, episode_id, status, run_date, now, archive=archive)
            current_rows.append(
                _current_row(
                    episode=episode,
                    candidate=candidate,
                    snapshot=snapshot,
                    latest_review=latest_review,
                    status=status,
                    active=not archive,
                )
            )

        current = pd.DataFrame(current_rows)
        if not current.empty:
            current = current.reindex(columns=CURRENT_COLUMNS)
        snapshots_df = pd.DataFrame(snapshots).reindex(columns=SNAPSHOT_COLUMNS)
        reviews_df = pd.DataFrame(reviews).reindex(columns=REVIEW_COLUMNS)
        alerts_df = pd.DataFrame(alerts).reindex(columns=ALERT_COLUMNS)

    summary = {
        "status": "completed",
        "run_date": run_date,
        "run_id": config.run_id,
        "active_candidates": int(current["active"].fillna(False).astype(bool).sum()) if not current.empty else 0,
        "new_episodes": int(new_episodes),
        "updated_episodes": int(updated_episodes),
        "snapshots": int(len(snapshots_df)),
        "fundamental_reviews": int(len(reviews_df)),
        "alerts": int(len(alerts_df)),
        "status_counts": current["status"].fillna("UNKNOWN").value_counts().to_dict() if not current.empty else {},
        "warnings": warnings,
    }
    return CandidateTrackerResult(current=current, snapshots=snapshots_df, fundamental_reviews=reviews_df, alerts=alerts_df, summary=summary)


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_candidates (
            episode_id VARCHAR,
            symbol VARCHAR NOT NULL,
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            first_seen_close DOUBLE,
            latest_candidate_group VARCHAR,
            latest_watchlist_bucket VARCHAR,
            latest_source VARCHAR,
            active BOOLEAN DEFAULT TRUE,
            latest_status VARCHAR,
            archived_at TIMESTAMP,
            archive_reason VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tracked_candidates_symbol_active ON tracked_candidates(symbol, active)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_tracking_snapshots (
            episode_id VARCHAR,
            symbol VARCHAR,
            snapshot_date DATE,
            close DOUBLE,
            return_since_first_seen DOUBLE,
            max_return_since_first_seen DOUBLE,
            drawdown_from_tracking_high DOUBLE,
            composite_score DOUBLE,
            relative_strength DOUBLE,
            sector_strength DOUBLE,
            near_52w_high_pct DOUBLE,
            close_above_sma50 BOOLEAN,
            close_above_sma200 BOOLEAN,
            stage2_flag BOOLEAN,
            technical_health_score DOUBLE,
            fundamental_health_score DOUBLE,
            valuation_history_score DOUBLE,
            tracking_health_score DOUBLE,
            status VARCHAR,
            status_reasons VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_candidate_snapshots_episode_date ON candidate_tracking_snapshots(episode_id, snapshot_date)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_fundamental_reviews (
            episode_id VARCHAR,
            symbol VARCHAR,
            report_date DATE,
            available_at DATE,
            review_date DATE,
            quarterly_result_score DOUBLE,
            quarterly_result_bucket VARCHAR,
            result_score_delta DOUBLE,
            sales_yoy_pct DOUBLE,
            sales_yoy_delta DOUBLE,
            operating_profit_yoy_pct DOUBLE,
            operating_profit_yoy_delta DOUBLE,
            profit_yoy_pct DOUBLE,
            profit_yoy_delta DOUBLE,
            opm_yoy_change_bps DOUBLE,
            opm_yoy_change_delta_bps DOUBLE,
            valuation_history_score DOUBLE,
            valuation_history_score_delta DOUBLE,
            fundamental_health_score DOUBLE,
            created_at TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_candidate_reviews_episode_available ON candidate_fundamental_reviews(episode_id, available_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_tracker_alerts (
            alert_id VARCHAR,
            episode_id VARCHAR,
            symbol VARCHAR,
            alert_date DATE,
            alert_type VARCHAR,
            prior_status VARCHAR,
            new_status VARCHAR,
            severity VARCHAR,
            message VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_candidate_alerts_episode_date ON candidate_tracker_alerts(episode_id, alert_date)")


def classify_candidate(
    *,
    snapshot: dict[str, Any],
    latest_review: dict[str, Any] | None,
    prior_snapshot: dict[str, Any] | None,
    last_seen_date: str,
    run_date: str,
    review_window_days: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    q_score = _num_from_review(latest_review, "quarterly_result_score", 50.0)
    q_bucket = str((latest_review or {}).get("quarterly_result_bucket") or "").upper()
    profit_yoy = _num_from_review(latest_review, "profit_yoy_pct", 0.0)
    opm_yoy = _num_from_review(latest_review, "opm_yoy_change_bps", 0.0)
    result_delta = _num_from_review(latest_review, "result_score_delta", 0.0)
    rs = _float(snapshot.get("relative_strength"), 50.0)
    composite = _float(snapshot.get("composite_score"), 50.0)
    tracking = _float(snapshot.get("tracking_health_score"), 50.0)
    drawdown = _float(snapshot.get("drawdown_from_tracking_high"), 0.0)
    below_sma50 = not bool(snapshot.get("close_above_sma50"))
    below_sma200 = not bool(snapshot.get("close_above_sma200"))
    rs_delta = rs - _float((prior_snapshot or {}).get("relative_strength"), rs)
    composite_delta = composite - _float((prior_snapshot or {}).get("composite_score"), composite)

    result_failure = q_bucket == "DETERIORATING" or (profit_yoy < 0 and opm_yoy < -200)
    if result_failure:
        reasons.append("result failure")
    technical_failure = below_sma200 or rs < 45 or drawdown > 25
    if technical_failure:
        reasons.append("technical failure")

    inactive_days = _date_diff(run_date, last_seen_date)
    inactive_remove = inactive_days > (int(review_window_days) * 2) and tracking < 40
    if inactive_remove:
        reasons.append("inactive and weak")
    if (result_failure and technical_failure) or inactive_remove:
        return "REMOVE_FROM_TRACKING", reasons
    if result_failure:
        return "RESULT_FAILURE", reasons
    if technical_failure:
        return "TECHNICAL_FAILURE", reasons

    deteriorating_checks = [
        (result_delta <= -10, "result score down >= 10"),
        (opm_yoy < -200, "OPM YoY change below -200 bps"),
        (profit_yoy < 0, "profit YoY negative"),
        (rs_delta <= -15, "relative strength down >= 15"),
        (composite_delta <= -15, "composite score down >= 15"),
        (below_sma50, "price below SMA50"),
        (drawdown > 15, "drawdown > 15%"),
    ]
    deterioration = [reason for ok, reason in deteriorating_checks if ok]
    if len(deterioration) >= 2:
        return "DETERIORATING", deterioration

    result_stable_or_improving = result_delta >= 0 or latest_review is None
    rs_stable_or_improving = rs_delta >= 0
    if (
        tracking >= 80
        and result_stable_or_improving
        and rs_stable_or_improving
        and bool(snapshot.get("close_above_sma50"))
        and bool(snapshot.get("close_above_sma200"))
    ):
        return "STRONG_IMPROVING", ["health >= 80", "result and RS stable or improving"]
    if tracking >= 65 and not result_failure and not technical_failure:
        return "IMPROVING", ["health >= 65"]
    if 50 <= tracking < 65:
        return "STABLE", ["health 50-65"]
    if 35 <= tracking < 50:
        return "WATCH_CAREFULLY", ["health 35-50"]
    return "WATCH_CAREFULLY", ["weak health"]


def _connect(db_path: Path):
    class _ConnectionContext:
        def __enter__(self) -> duckdb.DuckDBPyConnection:
            self.conn = duckdb.connect(str(db_path))
            return self.conn

        def __exit__(self, exc_type, exc, tb) -> None:
            self.conn.close()

    return _ConnectionContext()


def _build_candidate_universe(final_candidates: pd.DataFrame | None, watchlist_candidates: pd.DataFrame | None) -> pd.DataFrame:
    final = _symbol_frame(final_candidates)
    watch = _symbol_frame(watchlist_candidates)
    if not final.empty:
        final.loc[:, "in_final_candidates"] = True
    if not watch.empty:
        watch.loc[:, "in_watchlist_candidates"] = True
    merged = pd.concat([final, watch], ignore_index=True, sort=False)
    if merged.empty:
        return pd.DataFrame(columns=["symbol"])
    merged.loc[:, "_source_rank"] = _bool_series(merged, "in_final_candidates").map({True: 0, False: 1})
    merged = merged.sort_values(["_source_rank", "symbol"], kind="stable").drop_duplicates("symbol", keep="first")
    merged.loc[:, "in_final_candidates"] = _bool_series(merged, "in_final_candidates")
    merged.loc[:, "in_watchlist_candidates"] = _bool_series(merged, "in_watchlist_candidates")
    for column in ("candidate_group", "watchlist_bucket", "industry_group", "sector", "sector_name", "name"):
        if column not in merged.columns:
            merged.loc[:, column] = ""
    return merged.drop(columns=["_source_rank"], errors="ignore").reset_index(drop=True)


def _symbol_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol"])
    output = frame.copy()
    if "symbol" not in output.columns:
        for candidate in ("symbol_id", "Symbol", "NSE Code", "ticker"):
            if candidate in output.columns:
                output.loc[:, "symbol"] = output[candidate]
                break
    if "symbol" not in output.columns:
        return pd.DataFrame(columns=["symbol"])
    output.loc[:, "symbol"] = output["symbol"].map(normalize_symbol)
    return output.loc[output["symbol"].ne("")].copy()


def _build_technical_frame(
    *,
    candidates: pd.DataFrame,
    ranked_signals: pd.DataFrame | None,
    sector_dashboard: pd.DataFrame | None,
    ohlcv_db_path: Path | None,
    run_date: str,
    warnings: list[str],
) -> dict[str, dict[str, Any]]:
    frames = [_symbol_frame(candidates), _symbol_frame(ranked_signals)]
    combined = pd.concat([frame for frame in frames if frame is not None and not frame.empty], ignore_index=True, sort=False)
    if combined.empty:
        combined = candidates[["symbol"]].copy()
    combined = combined.drop_duplicates("symbol", keep="first").set_index("symbol", drop=False)
    ohlcv = _latest_ohlcv_features(sorted(candidates["symbol"].astype(str).unique()), ohlcv_db_path, run_date, warnings)
    sector_lookup = _sector_strength_lookup(sector_dashboard)
    output: dict[str, dict[str, Any]] = {}
    for symbol in sorted(candidates["symbol"].astype(str).unique()):
        row = combined.loc[symbol].to_dict() if symbol in combined.index else {"symbol": symbol}
        fallback = ohlcv.get(symbol, {})
        sector_key = _norm_text(_first_value(row, ["sector", "sector_name", "industry_group"]))
        close = _first_number(row, ["close", "Close", "last_price"], fallback.get("close"))
        sma50 = _first_number(row, ["sma_50", "sma50", "SMA50"], fallback.get("sma50"))
        sma200 = _first_number(row, ["sma_200", "sma200", "SMA200"], fallback.get("sma200"))
        near_high = _first_number(row, ["near_52w_high_pct", "distance_from_52w_high_pct"], fallback.get("near_52w_high_pct"))
        stage2_value = _first_value(row, ["is_stage2_uptrend", "is_stage2_structural", "stage2_flag", "stage2_label"])
        output[symbol] = {
            "close": close,
            "sma50": sma50,
            "sma200": sma200,
            "composite_score": _first_number(row, ["composite_score", "final_candidate_score"], 50.0),
            "relative_strength": _first_number(row, ["rel_strength_score", "relative_strength", "rel_strength", "rs_score", "RS"], 50.0),
            "sector_strength": sector_lookup.get(sector_key, _first_number(row, ["sector_strength", "sector_rs", "sector_rs_score"], 50.0)),
            "near_52w_high_pct": near_high,
            "close_above_sma50": bool(close is not None and sma50 is not None and close > sma50),
            "close_above_sma200": bool(close is not None and sma200 is not None and close > sma200),
            "stage2_flag": _stage2_bool(stage2_value),
        }
    return output


def _latest_ohlcv_features(symbols: list[str], db_path: Path | None, run_date: str, warnings: list[str]) -> dict[str, dict[str, Any]]:
    if not symbols or db_path is None or not Path(db_path).exists():
        return {}
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        exists = bool(conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_catalog'").fetchone()[0])
        if not exists:
            return {}
        rows = conn.execute(
            """
            WITH scoped AS (
                SELECT symbol_id, CAST(timestamp AS DATE) AS trade_date, close
                FROM _catalog
                WHERE UPPER(symbol_id) IN (SELECT UNNEST(?))
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
            ),
            features AS (
                SELECT
                    UPPER(symbol_id) AS symbol,
                    trade_date,
                    close,
                    AVG(close) OVER (PARTITION BY symbol_id ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma50,
                    AVG(close) OVER (PARTITION BY symbol_id ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200,
                    MAX(close) OVER (PARTITION BY symbol_id ORDER BY trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high52
                FROM scoped
            )
            SELECT symbol, close, sma50, sma200,
                   CASE WHEN high52 > 0 THEN ((high52 - close) / high52) * 100 ELSE NULL END AS near_52w_high_pct
            FROM features
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) = 1
            """,
            [symbols, run_date],
        ).fetchdf()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"OHLCV fallback unavailable: {exc}")
        return {}
    finally:
        conn.close()
    return {str(row["symbol"]): row.to_dict() for _, row in rows.iterrows()}


def _sector_strength_lookup(frame: pd.DataFrame | None) -> dict[str, float]:
    if frame is None or frame.empty:
        return {}
    out: dict[str, float] = {}
    for _, row in frame.iterrows():
        data = row.to_dict()
        key = _norm_text(_first_value(data, ["sector", "Sector", "sector_name", "industry_group", "industry"]))
        if not key:
            continue
        score = _first_number(data, ["sector_strength", "rs_score", "RS", "rs", "relative_strength", "sector_rs"], None)
        if score is None:
            rank = _first_number(data, ["RS_rank", "rs_rank", "rank", "Rank"], None)
            score = max(0.0, min(100.0, 100.0 - ((rank or 50.0) - 1.0) * 5.0))
        out[key] = _clip(score, 0, 100)
    return out


def _valuation_scores(frame: pd.DataFrame | None) -> dict[str, float]:
    data = _symbol_frame(frame)
    if data.empty:
        return {}
    out: dict[str, float] = {}
    for _, row in data.iterrows():
        payload = row.to_dict()
        score = _first_number(
            payload,
            ["valuation_history_score", "valuation_score", "own_history_score", "valuation_band_score", "score"],
            None,
        )
        if score is None:
            bucket = str(_first_value(payload, ["valuation_history_bucket", "valuation_bucket", "bucket"], "")).upper()
            score = _bucket_score(bucket)
        out[str(row["symbol"])] = _clip(score, 0, 100)
    return out


def _latest_results(frame: pd.DataFrame | None, *, run_date: str, window_days: int) -> dict[str, dict[str, Any]]:
    data = _symbol_frame(frame)
    if data.empty:
        return {}
    if "available_at" not in data.columns:
        data.loc[:, "available_at"] = data.get("report_date", run_date)
    data.loc[:, "_available_at"] = pd.to_datetime(data["available_at"], errors="coerce")
    run_ts = pd.Timestamp(run_date)
    min_ts = run_ts - pd.Timedelta(days=int(window_days))
    data = data.loc[data["_available_at"].notna() & data["_available_at"].le(run_ts) & data["_available_at"].ge(min_ts)].copy()
    if data.empty:
        return {}
    data = data.sort_values(["symbol", "_available_at"], ascending=[True, False], kind="stable").drop_duplicates("symbol", keep="first")
    return {str(row["symbol"]): row.drop(labels=["_available_at"], errors="ignore").to_dict() for _, row in data.iterrows()}


def _active_episode(conn: duckdb.DuckDBPyConnection, symbol: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM tracked_candidates
        WHERE symbol = ? AND active = TRUE
        ORDER BY first_seen_date DESC, created_at DESC
        LIMIT 1
        """,
        [symbol],
    ).fetchdf()
    if row.empty:
        return None
    return row.iloc[0].to_dict()


def _new_episode_id(conn: duckdb.DuckDBPyConnection, symbol: str, run_date: str) -> str:
    count = conn.execute("SELECT COUNT(*) FROM tracked_candidates WHERE symbol = ?", [symbol]).fetchone()[0]
    return f"{symbol}-{run_date}-{int(count or 0) + 1}"


def _insert_episode(
    conn: duckdb.DuckDBPyConnection,
    episode_id: str,
    symbol: str,
    candidate: pd.Series,
    run_date: str,
    first_seen_close: float | None,
    now: str,
) -> None:
    conn.execute(
        """
        INSERT INTO tracked_candidates (
            episode_id, symbol, first_seen_date, last_seen_date, first_seen_close,
            latest_candidate_group, latest_watchlist_bucket, latest_source,
            active, latest_status, created_at, updated_at
        )
        VALUES (?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, TRUE, 'WATCH_CAREFULLY', CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
        """,
        [
            episode_id,
            symbol,
            run_date,
            run_date,
            first_seen_close,
            str(candidate.get("candidate_group") or ""),
            str(candidate.get("watchlist_bucket") or ""),
            _source_label(candidate),
            now,
            now,
        ],
    )


def _update_episode_seen(conn: duckdb.DuckDBPyConnection, episode_id: str, candidate: pd.Series, run_date: str, now: str) -> None:
    conn.execute(
        """
        UPDATE tracked_candidates
        SET last_seen_date = CAST(? AS DATE),
            latest_candidate_group = ?,
            latest_watchlist_bucket = ?,
            latest_source = ?,
            updated_at = CAST(? AS TIMESTAMP)
        WHERE episode_id = ?
        """,
        [
            run_date,
            str(candidate.get("candidate_group") or ""),
            str(candidate.get("watchlist_bucket") or ""),
            _source_label(candidate),
            now,
            episode_id,
        ],
    )


def _prior_status_by_episode(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = conn.execute("SELECT episode_id, latest_status FROM tracked_candidates").fetchdf()
    if rows.empty:
        return {}
    return {str(row["episode_id"]): str(row["latest_status"] or "") for _, row in rows.iterrows()}


def _prior_snapshot(conn: duckdb.DuckDBPyConnection, episode_id: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT *
        FROM candidate_tracking_snapshots
        WHERE episode_id = ?
        ORDER BY snapshot_date DESC, created_at DESC
        LIMIT 1
        """,
        [episode_id],
    ).fetchdf()
    if rows.empty:
        return None
    return rows.iloc[0].to_dict()


def _latest_review(conn: duckdb.DuckDBPyConnection, episode_id: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT *
        FROM candidate_fundamental_reviews
        WHERE episode_id = ?
        ORDER BY available_at DESC, report_date DESC
        LIMIT 1
        """,
        [episode_id],
    ).fetchdf()
    if rows.empty:
        return None
    return rows.iloc[0].to_dict()


def _maybe_build_review(
    *,
    conn: duckdb.DuckDBPyConnection,
    episode_id: str,
    symbol: str,
    run_date: str,
    result_row: dict[str, Any] | None,
    valuation_history_score: float,
    now: str,
) -> dict[str, Any] | None:
    if not result_row:
        return None
    available_at = str(result_row.get("available_at") or result_row.get("report_date") or run_date)[:10]
    existing = conn.execute(
        """
        SELECT COUNT(*)
        FROM candidate_fundamental_reviews
        WHERE episode_id = ? AND CAST(available_at AS DATE) = CAST(? AS DATE)
        """,
        [episode_id, available_at],
    ).fetchone()[0]
    if int(existing or 0) > 0:
        return None
    prior = _latest_review(conn, episode_id)
    q_score = _float(result_row.get("quarterly_result_score"), 50.0)
    sales = _float(result_row.get("sales_yoy_pct"), 0.0)
    op = _float(result_row.get("operating_profit_yoy_pct"), 0.0)
    profit = _float(result_row.get("profit_yoy_pct"), 0.0)
    opm = _float(result_row.get("opm_yoy_change_bps"), 0.0)
    result_delta = q_score - _float((prior or {}).get("quarterly_result_score"), q_score)
    sales_delta = sales - _float((prior or {}).get("sales_yoy_pct"), sales)
    op_delta = op - _float((prior or {}).get("operating_profit_yoy_pct"), op)
    profit_delta = profit - _float((prior or {}).get("profit_yoy_pct"), profit)
    opm_delta = opm - _float((prior or {}).get("opm_yoy_change_bps"), opm)
    valuation_delta = valuation_history_score - _float((prior or {}).get("valuation_history_score"), valuation_history_score)
    fundamental = _fundamental_health_score(
        quarterly_result_score=q_score,
        result_score_delta=result_delta,
        opm_yoy_change_bps=opm,
        profit_yoy_pct=profit,
        valuation_history_score=valuation_history_score,
    )
    review = {
        "episode_id": episode_id,
        "symbol": symbol,
        "report_date": str(result_row.get("report_date") or available_at)[:10],
        "available_at": available_at,
        "review_date": run_date,
        "quarterly_result_score": q_score,
        "quarterly_result_bucket": str(result_row.get("quarterly_result_bucket") or ""),
        "result_score_delta": round(result_delta, 4),
        "sales_yoy_pct": sales,
        "sales_yoy_delta": round(sales_delta, 4),
        "operating_profit_yoy_pct": op,
        "operating_profit_yoy_delta": round(op_delta, 4),
        "profit_yoy_pct": profit,
        "profit_yoy_delta": round(profit_delta, 4),
        "opm_yoy_change_bps": opm,
        "opm_yoy_change_delta_bps": round(opm_delta, 4),
        "valuation_history_score": valuation_history_score,
        "valuation_history_score_delta": round(valuation_delta, 4),
        "fundamental_health_score": fundamental,
        "created_at": now,
    }
    _insert_frame(conn, "candidate_fundamental_reviews", pd.DataFrame([review])[REVIEW_COLUMNS])
    return review


def _build_snapshot(
    *,
    episode_id: str,
    symbol: str,
    snapshot_date: str,
    close: float | None,
    first_seen_close: float | None,
    prior_snapshot: dict[str, Any] | None,
    tech: dict[str, Any],
    latest_review: dict[str, Any] | None,
    valuation_history_score: float,
    now: str,
) -> dict[str, Any]:
    prior_max_return = _float((prior_snapshot or {}).get("max_return_since_first_seen"), 0.0)
    return_since = 0.0
    if close is not None and first_seen_close not in (None, 0):
        return_since = ((close / float(first_seen_close)) - 1.0) * 100.0
    max_return = max(return_since, prior_max_return)
    prior_high_close = _tracking_high_from_snapshot(prior_snapshot, first_seen_close)
    tracking_high = max([value for value in (prior_high_close, close, first_seen_close) if value is not None] or [0.0])
    drawdown = ((tracking_high - close) / tracking_high * 100.0) if close is not None and tracking_high > 0 else 0.0
    technical = _technical_health_score(tech, drawdown)
    fundamental = _float((latest_review or {}).get("fundamental_health_score"), 50.0)
    tracking = (0.45 * fundamental) + (0.40 * technical) + (0.15 * valuation_history_score)
    return {
        "episode_id": episode_id,
        "symbol": symbol,
        "snapshot_date": snapshot_date,
        "close": close,
        "return_since_first_seen": round(return_since, 4),
        "max_return_since_first_seen": round(max_return, 4),
        "drawdown_from_tracking_high": round(drawdown, 4),
        "composite_score": _float(tech.get("composite_score"), 50.0),
        "relative_strength": _float(tech.get("relative_strength"), 50.0),
        "sector_strength": _float(tech.get("sector_strength"), 50.0),
        "near_52w_high_pct": _float_or_none(tech.get("near_52w_high_pct")),
        "close_above_sma50": bool(tech.get("close_above_sma50")),
        "close_above_sma200": bool(tech.get("close_above_sma200")),
        "stage2_flag": bool(tech.get("stage2_flag")),
        "technical_health_score": round(technical, 4),
        "fundamental_health_score": round(fundamental, 4),
        "valuation_history_score": round(valuation_history_score, 4),
        "tracking_health_score": round(tracking, 4),
        "status": "",
        "status_reasons": "",
        "created_at": now,
    }


def _tracking_high_from_snapshot(prior_snapshot: dict[str, Any] | None, first_seen_close: float | None) -> float | None:
    if not prior_snapshot:
        return first_seen_close
    close = _float_or_none(prior_snapshot.get("close"))
    max_return = _float(prior_snapshot.get("max_return_since_first_seen"), 0.0)
    if first_seen_close not in (None, 0):
        return float(first_seen_close) * (1.0 + max_return / 100.0)
    return close


def _technical_health_score(tech: dict[str, Any], drawdown: float) -> float:
    near_high = _float_or_none(tech.get("near_52w_high_pct"))
    near_high_score = 50.0 if near_high is None else _clip(100.0 - near_high * 4.0, 0, 100)
    trend_score = _trend_structure_score(tech)
    drawdown_score = _clip(100.0 - drawdown * 3.33, 0, 100)
    score = (
        0.30 * _float(tech.get("composite_score"), 50.0)
        + 0.25 * _float(tech.get("relative_strength"), 50.0)
        + 0.15 * _float(tech.get("sector_strength"), 50.0)
        + 0.10 * near_high_score
        + 0.10 * trend_score
        + 0.10 * drawdown_score
    )
    return round(_clip(score, 0, 100), 4)


def _fundamental_health_score(
    *,
    quarterly_result_score: float,
    result_score_delta: float,
    opm_yoy_change_bps: float,
    profit_yoy_pct: float,
    valuation_history_score: float,
) -> float:
    delta_score = _clip(50.0 + result_score_delta * 2.0, 0, 100)
    opm_score = _clip((opm_yoy_change_bps + 200.0) / 400.0 * 100.0, 0, 100)
    profit_score = _clip((profit_yoy_pct + 20.0) / 60.0 * 100.0, 0, 100)
    score = (
        0.40 * quarterly_result_score
        + 0.20 * delta_score
        + 0.15 * opm_score
        + 0.15 * profit_score
        + 0.10 * valuation_history_score
    )
    return round(_clip(score, 0, 100), 4)


def _trend_structure_score(tech: dict[str, Any]) -> float:
    above50 = bool(tech.get("close_above_sma50"))
    above200 = bool(tech.get("close_above_sma200"))
    stage2 = bool(tech.get("stage2_flag"))
    if above50 and above200 and stage2:
        return 100.0
    if above50 and above200:
        return 80.0
    if above50:
        return 55.0
    if above200:
        return 35.0
    return 20.0


def _upsert_snapshot(conn: duckdb.DuckDBPyConnection, snapshot: dict[str, Any]) -> None:
    conn.execute(
        "DELETE FROM candidate_tracking_snapshots WHERE episode_id = ? AND CAST(snapshot_date AS DATE) = CAST(? AS DATE)",
        [snapshot["episode_id"], snapshot["snapshot_date"]],
    )
    _insert_frame(conn, "candidate_tracking_snapshots", pd.DataFrame([snapshot])[SNAPSHOT_COLUMNS])


def _insert_alert(conn: duckdb.DuckDBPyConnection, alert: dict[str, Any]) -> None:
    _insert_frame(conn, "candidate_tracker_alerts", pd.DataFrame([alert])[ALERT_COLUMNS])


def _insert_frame(conn: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    name = f"_{table}_frame"
    conn.register(name, frame)
    try:
        conn.execute(f"INSERT INTO {table} SELECT * FROM {name}")
    finally:
        conn.unregister(name)


def _update_episode_status(conn: duckdb.DuckDBPyConnection, episode_id: str, status: str, run_date: str, now: str, *, archive: bool) -> None:
    if archive:
        conn.execute(
            """
            UPDATE tracked_candidates
            SET latest_status = ?, active = FALSE, archived_at = CAST(? AS TIMESTAMP),
                archive_reason = ?, updated_at = CAST(? AS TIMESTAMP)
            WHERE episode_id = ?
            """,
            [status, now, f"{status} on {run_date}", now, episode_id],
        )
    else:
        conn.execute(
            "UPDATE tracked_candidates SET latest_status = ?, updated_at = CAST(? AS TIMESTAMP) WHERE episode_id = ?",
            [status, now, episode_id],
        )


def _build_alert(
    episode_id: str,
    symbol: str,
    run_date: str,
    prior_status: str,
    status: str,
    reasons: list[str],
    now: str,
) -> dict[str, Any]:
    alert_type = "STATUS_CHANGE"
    if status in {"RESULT_FAILURE", "TECHNICAL_FAILURE", "REMOVE_FROM_TRACKING"}:
        alert_type = status
    elif status in {"STRONG_IMPROVING", "IMPROVING"}:
        alert_type = "IMPROVING"
    severity = "info"
    if status == "REMOVE_FROM_TRACKING":
        severity = "critical"
    elif status in {"RESULT_FAILURE", "TECHNICAL_FAILURE", "DETERIORATING"}:
        severity = "high"
    elif status == "WATCH_CAREFULLY":
        severity = "medium"
    return {
        "alert_id": f"{episode_id}-{run_date}-{status}",
        "episode_id": episode_id,
        "symbol": symbol,
        "alert_date": run_date,
        "alert_type": alert_type,
        "prior_status": prior_status,
        "new_status": status,
        "severity": severity,
        "message": f"{symbol}: {prior_status or 'NEW'} -> {status}" + (f" ({'; '.join(reasons)})" if reasons else ""),
        "created_at": now,
    }


def _current_row(
    *,
    episode: dict[str, Any],
    candidate: pd.Series,
    snapshot: dict[str, Any],
    latest_review: dict[str, Any] | None,
    status: str,
    active: bool,
) -> dict[str, Any]:
    review = latest_review or {}
    return {
        "episode_id": snapshot["episode_id"],
        "symbol": snapshot["symbol"],
        "first_seen_date": str(episode.get("first_seen_date"))[:10],
        "last_seen_date": str(episode.get("last_seen_date"))[:10],
        "latest_candidate_group": str(candidate.get("candidate_group") or episode.get("latest_candidate_group") or ""),
        "latest_watchlist_bucket": str(candidate.get("watchlist_bucket") or episode.get("latest_watchlist_bucket") or ""),
        "status": status,
        "tracking_health_score": snapshot.get("tracking_health_score"),
        "technical_health_score": snapshot.get("technical_health_score"),
        "fundamental_health_score": snapshot.get("fundamental_health_score"),
        "valuation_history_score": snapshot.get("valuation_history_score"),
        "close": snapshot.get("close"),
        "return_since_first_seen": snapshot.get("return_since_first_seen"),
        "max_return_since_first_seen": snapshot.get("max_return_since_first_seen"),
        "drawdown_from_tracking_high": snapshot.get("drawdown_from_tracking_high"),
        "composite_score": snapshot.get("composite_score"),
        "relative_strength": snapshot.get("relative_strength"),
        "sector_strength": snapshot.get("sector_strength"),
        "near_52w_high_pct": snapshot.get("near_52w_high_pct"),
        "close_above_sma50": snapshot.get("close_above_sma50"),
        "close_above_sma200": snapshot.get("close_above_sma200"),
        "stage2_flag": snapshot.get("stage2_flag"),
        "quarterly_result_score": review.get("quarterly_result_score"),
        "quarterly_result_bucket": review.get("quarterly_result_bucket"),
        "result_score_delta": review.get("result_score_delta"),
        "sales_yoy_delta": review.get("sales_yoy_delta"),
        "operating_profit_yoy_delta": review.get("operating_profit_yoy_delta"),
        "profit_yoy_delta": review.get("profit_yoy_delta"),
        "opm_yoy_change_delta_bps": review.get("opm_yoy_change_delta_bps"),
        "valuation_history_score_delta": review.get("valuation_history_score_delta"),
        "active": active,
    }


def read_csv_optional(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists() or file_path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except EmptyDataError:
        return pd.DataFrame()


def _source_label(candidate: pd.Series) -> str:
    sources = []
    if bool(candidate.get("in_final_candidates", False)):
        sources.append("final_candidates")
    if bool(candidate.get("in_watchlist_candidates", False)):
        sources.append("watchlist_candidates")
    return ",".join(sources) if sources else "candidate"


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    values = frame[column]
    if values.dtype == bool:
        return values.fillna(False).astype(bool)
    object_values = values.astype("object")
    text = object_values.where(object_values.notna(), "").astype(str).str.strip().str.lower()
    numeric = pd.to_numeric(object_values, errors="coerce")
    return text.isin({"1", "true", "t", "yes", "y"}) | numeric.gt(0).fillna(False)


def _first_value(row: dict[str, Any], columns: list[str], default: Any = "") -> Any:
    for column in columns:
        value = row.get(column)
        if value is not None and not pd.isna(value) and str(value).strip() != "":
            return value
    return default


def _first_number(row: dict[str, Any], columns: list[str], default: float | None) -> float | None:
    for column in columns:
        value = _float_or_none(row.get(column))
        if value is not None:
            return value
    return default


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _float(value: Any, default: float) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _num_from_review(review: dict[str, Any] | None, key: str, default: float) -> float:
    return _float((review or {}).get(key), default)


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _stage2_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    return "stage2" in text or "stage 2" in text or text in {"s2", "strong_stage2"}


def _norm_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _bucket_score(bucket: str) -> float:
    text = str(bucket or "").upper()
    if any(token in text for token in ("CHEAP", "ATTRACTIVE", "UNDERVALUED", "LOW")):
        return 80.0
    if any(token in text for token in ("EXPENSIVE", "OVERVALUED", "HIGH")):
        return 35.0
    if any(token in text for token in ("FAIR", "NEUTRAL", "MID")):
        return 60.0
    return 50.0


def _date_diff(later: str, earlier: str) -> int:
    try:
        return int((pd.Timestamp(later).date() - pd.Timestamp(earlier).date()).days)
    except Exception:
        return 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
