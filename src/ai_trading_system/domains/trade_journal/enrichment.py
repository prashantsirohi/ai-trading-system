"""Look-ahead-safe, read-only market and governance enrichment."""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path
from statistics import fmean, pstdev
from typing import Any

import duckdb


TRUSTED_VALIDATION_PREFIXES = ("trusted_", "research_backfill")


def _mean(values: list[float], window: int) -> float | None:
    return fmean(values[-window:]) if len(values) >= window else None


def _return(values: list[float], sessions: int) -> float | None:
    if len(values) <= sessions or values[-sessions - 1] == 0:
        return None
    return values[-1] / values[-sessions - 1] - 1.0


def _atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(closes) <= period:
        return None
    true_ranges = [
        max(highs[index] - lows[index], abs(highs[index] - closes[index - 1]), abs(lows[index] - closes[index - 1]))
        for index in range(1, len(closes))
    ]
    return _mean(true_ranges, period)


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) <= period:
        return None
    changes = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
    gains = [max(change, 0.0) for change in changes[-period:]]
    losses = [max(-change, 0.0) for change in changes[-period:]]
    average_gain, average_loss = fmean(gains), fmean(losses)
    if average_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + average_gain / average_loss))


def _adx(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period * 2 + 1:
        return None
    true_ranges: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for index in range(1, len(closes)):
        up = highs[index] - highs[index - 1]
        down = lows[index - 1] - lows[index]
        true_ranges.append(max(highs[index] - lows[index], abs(highs[index] - closes[index - 1]), abs(lows[index] - closes[index - 1])))
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    dx_values: list[float] = []
    for end in range(period, len(true_ranges) + 1):
        tr = fmean(true_ranges[end - period:end])
        if tr <= 0:
            continue
        plus_di = 100.0 * fmean(plus_dm[end - period:end]) / tr
        minus_di = 100.0 * fmean(minus_dm[end - period:end]) / tr
        denominator = plus_di + minus_di
        if denominator > 0:
            dx_values.append(100.0 * abs(plus_di - minus_di) / denominator)
    return _mean(dx_values, period)


def _volume_metrics(volumes: list[float]) -> tuple[float | None, float | None, float | None]:
    if not volumes:
        return None, None, None
    current = volumes[-1]
    prior_20 = volumes[-21:-1]
    prior_50 = volumes[-51:-1]
    ratio = current / fmean(prior_20) if len(prior_20) == 20 and fmean(prior_20) else None

    def zscore(prior: list[float]) -> float | None:
        deviation = pstdev(prior) if len(prior) >= 2 else 0.0
        return (current - fmean(prior)) / deviation if deviation else None

    return ratio, zscore(prior_20) if len(prior_20) == 20 else None, zscore(prior_50) if len(prior_50) == 50 else None


class JournalMarketDataReader:
    def __init__(
        self,
        ohlcv_db_path: Path | str,
        *,
        control_plane_db_path: Path | str | None = None,
        master_db_path: Path | str | None = None,
    ):
        self.db_path = Path(ohlcv_db_path)
        self.control_plane_db_path = Path(control_plane_db_path) if control_plane_db_path else None
        self.master_db_path = Path(master_db_path) if master_db_path else None

    def prior_session_context(self, *, symbol: str, exchange: str, decision_date: date) -> dict[str, Any]:
        return self.prior_session_contexts({(symbol, exchange, decision_date)})[(symbol, exchange, decision_date)]

    def corporate_action_proposals(
        self, *, isins: set[str], from_date: date, to_date: date
    ) -> list[dict[str, Any]]:
        """Read active NSE split/bonus evidence without applying it to the ledger."""
        if not self.db_path.is_file() or not isins:
            return []
        placeholders = ",".join("?" for _ in isins)
        market = duckdb.connect(str(self.db_path), read_only=True)
        try:
            tables = {row[0] for row in market.execute("SHOW TABLES").fetchall()}
            if "_corporate_actions" not in tables:
                return []
            rows = market.execute(
                f"""SELECT isin,symbol,ex_date,action_type,share_factor,price_factor,
                           source,action_key,raw_payload_hash,parsed_ratio
                    FROM _corporate_actions WHERE isin IN ({placeholders})
                    AND action_type IN ('split','bonus') AND status='active'
                    AND ex_date BETWEEN ? AND ? AND share_factor>0
                    ORDER BY ex_date,action_key""",
                [*sorted(isins), from_date, to_date],
            ).fetchall()
        finally:
            market.close()
        return [{
            "isin": row[0], "symbol": row[1], "effective_date": row[2],
            "action_type": row[3], "quantity_factor": row[4],
            "cost_factor": row[5], "source": row[6], "source_ref": row[7],
            "content_hash": row[8], "parsed_ratio": row[9],
        } for row in rows]

    def prior_session_contexts(
        self, requests: set[tuple[str, str, date]]
    ) -> dict[tuple[str, str, date], dict[str, Any]]:
        if not self.db_path.is_file():
            return {request: self._unavailable("OHLCV_STORE_MISSING") for request in requests}
        market = duckdb.connect(str(self.db_path), read_only=True)
        control = (
            duckdb.connect(str(self.control_plane_db_path), read_only=True)
            if self.control_plane_db_path and self.control_plane_db_path.is_file() else None
        )
        master = (
            sqlite3.connect(f"file:{self.master_db_path}?mode=ro", uri=True)
            if self.master_db_path and self.master_db_path.is_file() else None
        )
        try:
            catalog_columns = {row[1] for row in market.execute("PRAGMA table_info('_catalog')").fetchall()}
            market_tables = {row[0] for row in market.execute("SHOW TABLES").fetchall()}
            control_tables = {row[0] for row in control.execute("SHOW TABLES").fetchall()} if control else set()
            benchmark_cache: dict[date, float | None] = {}
            output: dict[tuple[str, str, date], dict[str, Any]] = {}
            for request in sorted(requests):
                symbol, exchange, cutoff = request
                technical = self._technical_context(
                    market, catalog_columns, market_tables, symbol=symbol,
                    exchange=exchange, decision_date=cutoff,
                )
                cutoff_session = technical.get("cutoff_session")
                if cutoff_session:
                    cutoff_date = date.fromisoformat(str(cutoff_session))
                    if cutoff_date not in benchmark_cache:
                        benchmark_cache[cutoff_date] = self._benchmark_return_63(
                            market, catalog_columns, exchange, cutoff_date + timedelta(days=1)
                        )
                    stock_return = technical["metrics"].get("return_63")
                    benchmark_return = benchmark_cache[cutoff_date]
                    technical["metrics"]["stock_rs_63"] = (
                        stock_return - benchmark_return
                        if stock_return is not None and benchmark_return is not None else None
                    )
                    governance = self._governance_context(
                        control, control_tables, master, symbol=symbol,
                        exchange=exchange, cutoff=cutoff_date,
                    )
                    technical["metrics"].update(governance["metrics"])
                    technical["source_snapshot"]["governance"] = governance["source_snapshot"]
                    if governance["trust_status"] == "UNTRUSTED":
                        technical["trust_status"] = "PARTIAL"
                technical["source_snapshot"]["content_hash"] = hashlib.sha256(
                    repr((symbol, exchange, cutoff, technical["metrics"], technical["source_snapshot"])).encode()
                ).hexdigest()
                output[request] = technical
            return output
        finally:
            market.close()
            if control:
                control.close()
            if master:
                master.close()

    @staticmethod
    def _unavailable(reason: str) -> dict[str, Any]:
        return {
            "trust_status": "UNAVAILABLE", "cutoff_session": None,
            "metrics": {}, "source_snapshot": {"reason": reason},
        }

    def _technical_context(
        self,
        conn: duckdb.DuckDBPyConnection,
        columns: set[str],
        tables: set[str],
        *,
        symbol: str,
        exchange: str,
        decision_date: date,
    ) -> dict[str, Any]:
        adjusted = all(name in columns for name in ("adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close"))
        price_cols = (
            "COALESCE(adjusted_open,open),COALESCE(adjusted_high,high),COALESCE(adjusted_low,low),COALESCE(adjusted_close,close)"
            if adjusted else "open,high,low,close"
        )
        provider_cols = (
            "provider,validation_status,ingestion_version"
            if {"provider", "validation_status", "ingestion_version"} <= columns else "NULL,NULL,NULL"
        )
        rows = conn.execute(
            f"""SELECT CAST(timestamp AS DATE),{price_cols},volume,{provider_cols}
                FROM _catalog WHERE symbol_id=? AND exchange=? AND CAST(timestamp AS DATE)<?
                ORDER BY timestamp DESC LIMIT 320""",
            [symbol, exchange, decision_date],
        ).fetchall()
        if not rows:
            return self._unavailable("NO_PRIOR_SESSION")
        rows.reverse()
        dates = [row[0] for row in rows]
        opens = [float(row[1]) for row in rows]
        highs = [float(row[2]) for row in rows]
        lows = [float(row[3]) for row in rows]
        closes = [float(row[4]) for row in rows]
        volumes = [float(row[5] or 0) for row in rows]
        latest = rows[-1]
        volume_ratio, volume_z_20, volume_z_50 = _volume_metrics(volumes)
        atr_14 = _atr(highs, lows, closes)
        sma_20, sma_50, sma_200 = _mean(closes, 20), _mean(closes, 50), _mean(closes, 200)
        high_252 = max(highs[-252:]) if len(highs) >= 20 else None
        metrics: dict[str, Any] = {
            "open": opens[-1], "high": highs[-1], "low": lows[-1], "close": closes[-1],
            "volume": volumes[-1], "sma_20": sma_20, "sma_50": sma_50, "sma_200": sma_200,
            "sma_50_slope_20": (sma_50 / fmean(closes[-70:-20]) - 1.0) if sma_50 and len(closes) >= 70 else None,
            "sma_200_slope_20": (sma_200 / fmean(closes[-220:-20]) - 1.0) if sma_200 and len(closes) >= 220 else None,
            "atr_14": atr_14, "atr_pct": atr_14 / closes[-1] if atr_14 and closes[-1] else None,
            "rsi_14": _rsi(closes), "adx_14": _adx(highs, lows, closes),
            "volume_ratio_20": volume_ratio, "volume_zscore_20": volume_z_20,
            "volume_zscore_50": volume_z_50,
            "return_20": _return(closes, 20), "return_63": _return(closes, 63),
            "return_126": _return(closes, 126), "return_252": _return(closes, 252),
            "near_52w_high_pct": (high_252 - closes[-1]) / high_252 if high_252 else None,
            "history_sessions": len(rows),
        }
        if "_delivery" in tables:
            delivery = conn.execute(
                """SELECT delivery_pct FROM _delivery WHERE symbol_id=? AND exchange=?
                   AND CAST(timestamp AS DATE)<=? ORDER BY timestamp DESC LIMIT 21""",
                [symbol, exchange, dates[-1]],
            ).fetchall()
            delivery_values = [float(row[0]) for row in reversed(delivery) if row[0] is not None]
            delivery_baseline = _mean(delivery_values[:-1], 20)
            metrics.update({
                "delivery_pct": delivery_values[-1] if delivery_values else None,
                "delivery_pct_5d_avg": _mean(delivery_values, 5),
                "delivery_pct_20d_avg": _mean(delivery_values, 20),
                "delivery_intensity": (
                    delivery_values[-1] / delivery_baseline
                    if len(delivery_values) > 20 and delivery_baseline else None
                ),
            })
        if "feat_phase1_market_breadth" in tables:
            breadth = conn.execute(
                """SELECT breadth_score,breadth_velocity_score,breadth_velocity_bucket,
                          pct_above_200dma,pct_at_52w_high,advance_decline_ratio
                   FROM feat_phase1_market_breadth WHERE exchange=? AND date<=?
                   ORDER BY date DESC LIMIT 1""",
                [exchange, dates[-1]],
            ).fetchone()
            if breadth:
                metrics.update({
                    "breadth_score": breadth[0], "breadth_velocity_score": breadth[1],
                    "breadth_velocity_bucket": breadth[2], "pct_above_200dma": breadth[3],
                    "pct_at_52w_high": breadth[4], "advance_decline_ratio": breadth[5],
                    "regime": self._regime(breadth[0], breadth[1], breadth[2]),
                })
        active_quarantine = 0
        if "_catalog_quarantine" in tables:
            quarantine_row = conn.execute(
                """SELECT count(*) FROM _catalog_quarantine WHERE symbol_id=? AND exchange=?
                   AND trade_date<=? AND status='active'""",
                [symbol, exchange, dates[-1]],
            ).fetchone()
            active_quarantine = int(quarantine_row[0]) if quarantine_row else 0
        validation = str(latest[7] or "")
        trusted = validation.startswith(TRUSTED_VALIDATION_PREFIXES) and active_quarantine == 0
        same_day = conn.execute(
            f"""SELECT {price_cols},volume FROM _catalog WHERE symbol_id=? AND exchange=?
                AND CAST(timestamp AS DATE)=? ORDER BY timestamp DESC LIMIT 1""",
            [symbol, exchange, decision_date],
        ).fetchone()
        if same_day:
            metrics["same_day_descriptive"] = {
                "open": same_day[0], "high": same_day[1], "low": same_day[2],
                "close": same_day[3], "volume": same_day[4], "decision_input": False,
            }
        return {
            "trust_status": "TRUSTED" if trusted else "UNTRUSTED",
            "cutoff_session": dates[-1].isoformat(),
            "metrics": metrics,
            "source_snapshot": {
                "table": "_catalog", "cutoff_session": dates[-1].isoformat(),
                "provider": latest[6], "validation_status": validation,
                "ingestion_version": latest[8], "adjusted_prices": adjusted,
                "active_quarantine_rows": active_quarantine,
            },
        }

    def _governance_context(
        self,
        control: duckdb.DuckDBPyConnection | None,
        tables: set[str],
        master: sqlite3.Connection | None,
        *,
        symbol: str,
        exchange: str,
        cutoff: date,
    ) -> dict[str, Any]:
        metrics: dict[str, Any] = {
            "stage_label": None, "stage_maturity": None, "pattern_state": None,
            "pattern_family": None, "pattern_score": None, "pivot_price": None,
            "breakout_state": None, "sector": None, "sector_stage": None,
        }
        sources: dict[str, Any] = {}
        trusted = True
        if control and "stage_history" in tables:
            row = control.execute(
                """SELECT stage_label,stage_confidence,stage_input_completeness_pct,
                          stage_model_version,stage_config_hash,pipeline_run_id,trade_date
                   FROM stage_history WHERE symbol_id=? AND exchange=? AND trade_date<=?
                   ORDER BY trade_date DESC,updated_at DESC LIMIT 1""",
                [symbol, exchange, cutoff],
            ).fetchone()
            if row:
                metrics.update({"stage_label": row[0], "stage_confidence": row[1], "stage_completeness_pct": row[2]})
                sources["stage"] = {"table": "stage_history", "model_version": row[3], "config_hash": row[4], "run_id": row[5], "as_of": str(row[6])}
        if control and "stage1_history" in tables:
            row = control.execute(
                """SELECT stage1_maturity_score,stage1_substate,stage1_data_completeness_pct,
                          stage1_model_version,stage1_config_hash,pipeline_run_id,trade_date
                   FROM stage1_history WHERE symbol_id=? AND exchange=? AND trade_date<=?
                   ORDER BY trade_date DESC,updated_at DESC LIMIT 1""",
                [symbol, exchange, cutoff],
            ).fetchone()
            if row:
                metrics.update({"stage_maturity": row[0], "stage1_substate": row[1], "stage1_completeness_pct": row[2]})
                sources["stage1"] = {"table": "stage1_history", "model_version": row[3], "config_hash": row[4], "run_id": row[5], "as_of": str(row[6])}
        if control and "pattern_history" in tables:
            row = control.execute(
                """SELECT pattern_family,pattern_state,pattern_score,setup_quality,pivot_price,
                          distance_to_pivot_pct,breakout_status,pattern_model_version,
                          pattern_config_hash,pipeline_run_id,trade_date
                   FROM pattern_history WHERE symbol_id=? AND exchange=? AND trade_date<=?
                   ORDER BY trade_date DESC,updated_at DESC,pattern_score DESC NULLS LAST LIMIT 1""",
                [symbol, exchange, cutoff],
            ).fetchone()
            if row:
                metrics.update({
                    "pattern_family": row[0], "pattern_state": row[1], "pattern_score": row[2],
                    "setup_quality": row[3], "pivot_price": row[4], "distance_to_pivot_pct": row[5],
                    "breakout_state": row[6],
                })
                sources["pattern"] = {"table": "pattern_history", "model_version": row[7], "config_hash": row[8], "run_id": row[9], "as_of": str(row[10])}
        membership = None
        if control and "sector_membership_history" in tables:
            membership = control.execute(
                """SELECT sector_id,sector_name,membership_trust,point_in_time_valid,
                          source_type,source_hash,valid_from,valid_to,recorded_at
                   FROM sector_membership_history WHERE symbol_id=? AND exchange=?
                   AND valid_from<=? AND (valid_to IS NULL OR valid_to>=?)
                   AND CAST(recorded_at AS DATE)<=?
                   ORDER BY point_in_time_valid DESC,recorded_at DESC LIMIT 1""",
                [symbol, exchange, cutoff, cutoff, cutoff],
            ).fetchone()
            if membership:
                metrics["sector"] = membership[1]
                sources["sector_membership"] = {
                    "table": "sector_membership_history", "trust": membership[2],
                    "point_in_time_valid": membership[3], "source_type": membership[4],
                    "source_hash": membership[5], "valid_from": str(membership[6]),
                    "valid_to": str(membership[7]) if membership[7] else None,
                }
                trusted = bool(membership[3])
        if membership is None and master:
            columns = {row[1] for row in master.execute("PRAGMA table_info('symbols')")}
            symbol_column = "nse_symbol" if exchange == "NSE" and "nse_symbol" in columns else "symbol_id"
            row = master.execute(
                f"SELECT sector FROM symbols WHERE {symbol_column}=? ORDER BY last_updated DESC LIMIT 1",
                [symbol],
            ).fetchone()
            if row and row[0]:
                metrics["sector"] = row[0]
                sources["sector_membership"] = {"table": "masterdata.symbols", "trust": "LATEST_ONLY"}
                trusted = False
        if control and metrics["sector"] and "weekly_sector_stage_history" in tables:
            row = control.execute(
                """SELECT effective_stage,stage_status,source_artifact_hash,run_id,as_of
                   FROM weekly_sector_stage_history WHERE sector_name=? AND as_of<=?
                   ORDER BY as_of DESC,created_at DESC LIMIT 1""",
                [metrics["sector"], cutoff],
            ).fetchone()
            if row:
                metrics["sector_stage"] = row[0]
                sources["sector_stage"] = {"table": "weekly_sector_stage_history", "status": row[1], "source_hash": row[2], "run_id": row[3], "as_of": str(row[4])}
        return {"metrics": metrics, "source_snapshot": sources, "trust_status": "TRUSTED" if trusted else "UNTRUSTED"}

    @staticmethod
    def _regime(score: Any, velocity: Any, bucket: Any) -> str | None:
        if score is None and velocity is None and bucket is None:
            return None
        score_value = float(score or 0)
        velocity_value = float(velocity or 0)
        if score_value >= 60 and velocity_value >= 0:
            return "RISK_ON"
        if score_value <= 40 and velocity_value <= 0:
            return "RISK_OFF"
        return "TRANSITION"

    def _benchmark_return_63(
        self, conn: duckdb.DuckDBPyConnection, columns: set[str], exchange: str, decision_date: date
    ) -> float | None:
        if "is_benchmark" not in columns:
            return None
        close_column = "COALESCE(adjusted_close,close)" if "adjusted_close" in columns else "close"
        rows = conn.execute(
            f"""SELECT {close_column} FROM _catalog WHERE exchange=? AND is_benchmark
                AND CAST(timestamp AS DATE)<? ORDER BY timestamp DESC LIMIT 64""",
            [exchange, decision_date],
        ).fetchall()
        closes = [float(row[0]) for row in reversed(rows) if row[0] is not None]
        return _return(closes, 63)

    def forward_outcome(
        self, *, symbol: str, exchange: str, anchor_date: date,
        anchor_price: float, end_date: date | None = None, max_sessions: int = 60,
    ) -> dict[str, Any]:
        if not self.db_path.is_file() or anchor_price <= 0:
            return {"trust_status": "UNAVAILABLE", "sessions": 0}
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            return self._forward_outcome_query(
                conn, symbol=symbol, exchange=exchange, anchor_date=anchor_date,
                anchor_price=anchor_price, end_date=end_date, max_sessions=max_sessions,
            )
        finally:
            conn.close()

    def forward_outcomes(
        self,
        requests: dict[str, tuple[str, str, date, float, date | None]],
        *,
        max_sessions: int = 60,
    ) -> dict[str, dict[str, Any]]:
        if not self.db_path.is_file():
            return {key: {"trust_status": "UNAVAILABLE", "sessions": 0} for key in requests}
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            return {
                key: self._forward_outcome_query(
                    conn, symbol=value[0], exchange=value[1], anchor_date=value[2],
                    anchor_price=value[3], end_date=value[4], max_sessions=max_sessions,
                )
                for key, value in requests.items()
            }
        finally:
            conn.close()

    @staticmethod
    def _forward_outcome_query(
        conn: duckdb.DuckDBPyConnection, *, symbol: str, exchange: str,
        anchor_date: date, anchor_price: float, end_date: date | None,
        max_sessions: int,
    ) -> dict[str, Any]:
        columns = {row[1] for row in conn.execute("PRAGMA table_info('_catalog')").fetchall()}
        adjusted = all(name in columns for name in ("adjusted_high", "adjusted_low", "adjusted_close"))
        prices = (
            "COALESCE(adjusted_high,high),COALESCE(adjusted_low,low),COALESCE(adjusted_close,close)"
            if adjusted else "high,low,close"
        )
        clauses = ["symbol_id=?", "exchange=?", "CAST(timestamp AS DATE)>?"]
        params: list[Any] = [symbol, exchange, anchor_date]
        if end_date:
            clauses.append("CAST(timestamp AS DATE)<=?")
            params.append(end_date)
        params.append(max_sessions)
        rows = conn.execute(
            f"""SELECT CAST(timestamp AS DATE),{prices},validation_status,provider,ingestion_version
                FROM _catalog WHERE {' AND '.join(clauses)} ORDER BY timestamp LIMIT ?""",
            params,
        ).fetchall()
        if not rows:
            return {"trust_status": "UNAVAILABLE", "sessions": 0}
        trusted = all(str(row[4] or "").startswith(TRUSTED_VALIDATION_PREFIXES) for row in rows)
        returns = {
            f"forward_return_{horizon}": (float(rows[horizon - 1][3]) / anchor_price - 1.0) if len(rows) >= horizon else None
            for horizon in (1, 5, 20, 60)
        }
        highs = [float(row[1]) for row in rows]
        lows = [float(row[2]) for row in rows]
        return {
            **returns,
            "mfe": max(highs) / anchor_price - 1.0,
            "mae": min(lows) / anchor_price - 1.0,
            "sessions": len(rows), "first_session": rows[0][0].isoformat(),
            "last_session": rows[-1][0].isoformat(),
            "trust_status": "TRUSTED" if trusted else "UNTRUSTED",
            "source_snapshot": {
                "table": "_catalog", "provider": rows[-1][5],
                "ingestion_version": rows[-1][6], "adjusted_prices": adjusted,
                "starts_next_session": True,
            },
        }

    def candles(
        self, *, symbols: Iterable[str], exchange: str,
        from_date: date, to_date: date, limit: int = 750,
    ) -> dict[str, Any]:
        if not self.db_path.is_file():
            return {"items": [], "trust_status": "UNAVAILABLE"}
        normalized = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
        if not normalized:
            return {"items": [], "trust_status": "UNAVAILABLE"}
        placeholders = ",".join("?" for _ in normalized)
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            columns = {row[1] for row in conn.execute("PRAGMA table_info('_catalog')").fetchall()}
            adjusted = all(name in columns for name in ("adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close"))
            prices = (
                "COALESCE(adjusted_open,open),COALESCE(adjusted_high,high),COALESCE(adjusted_low,low),COALESCE(adjusted_close,close)"
                if adjusted else "open,high,low,close"
            )
            rows = conn.execute(
                f"""SELECT CAST(timestamp AS DATE),{prices},volume,validation_status,symbol_id
                    FROM _catalog WHERE symbol_id IN ({placeholders}) AND exchange=?
                    AND CAST(timestamp AS DATE) BETWEEN ? AND ?
                    ORDER BY timestamp,symbol_id LIMIT ?""",
                [*normalized, exchange, from_date, to_date, min(2000, max(1, limit))],
            ).fetchall()
        finally:
            conn.close()
        items = [{
            "time": row[0].isoformat(), "open": row[1], "high": row[2], "low": row[3],
            "close": row[4], "volume": row[5], "validation_status": row[6], "symbol": row[7],
        } for row in rows]
        trusted = bool(items) and all(
            str(row["validation_status"] or "").startswith(TRUSTED_VALIDATION_PREFIXES) for row in items
        )
        return {
            "items": items, "trust_status": "TRUSTED" if trusted else ("PARTIAL" if items else "UNAVAILABLE"),
            "source_snapshot": {"table": "_catalog", "adjusted_prices": adjusted},
        }

    def load_bars(
        self, *, symbols: Iterable[str], exchange: str,
        from_date: date, to_date: date,
    ) -> list[dict[str, Any]]:
        """Load a bounded trusted-price frame for portfolio reconstruction."""
        if not self.db_path.is_file():
            return []
        normalized = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
        if not normalized:
            return []
        placeholders = ",".join("?" for _ in normalized)
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            columns = {row[1] for row in conn.execute("PRAGMA table_info('_catalog')").fetchall()}
            adjusted = "adjusted_close" in columns
            close_column = "COALESCE(adjusted_close,close)" if adjusted else "close"
            rows = conn.execute(
                f"""SELECT symbol_id,exchange,CAST(timestamp AS DATE),{close_column},
                           validation_status,provider,ingestion_version
                    FROM _catalog WHERE symbol_id IN ({placeholders}) AND exchange=?
                    AND CAST(timestamp AS DATE) BETWEEN ? AND ? ORDER BY timestamp,symbol_id""",
                [*normalized, exchange, from_date, to_date],
            ).fetchall()
        finally:
            conn.close()
        return [{
            "symbol": row[0], "exchange": row[1], "date": row[2], "close": row[3],
            "validation_status": row[4], "provider": row[5],
            "ingestion_version": row[6], "adjusted_prices": adjusted,
            "trust_status": (
                "TRUSTED" if str(row[4] or "").startswith(TRUSTED_VALIDATION_PREFIXES)
                else "UNTRUSTED"
            ),
        } for row in rows]
