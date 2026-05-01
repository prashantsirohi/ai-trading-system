"""Facade for the production stock ranking workflow."""

from __future__ import annotations

import os
from typing import Dict, List, Optional

import pandas as pd

from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.domains.ranking.composite import (
    apply_rank_stability,
    compute_factor_scores,
    compute_rank_confidence,
    filter_ranked_scores,
    load_factor_weights,
    select_rank_output_columns,
)
from ai_trading_system.domains.ranking.contracts import (
    RANK_MODES,
    STAGE2_FRESH_BARS_MAX,
    STAGE2_FRESHNESS_BONUS,
    STAGE2_MID_BARS_MAX,
    STAGE2_MID_FRESHNESS_BONUS,
    STAGE2_TRANSITION_BONUS,
    STAGE2_TRANSITION_BONUS_BARS_MAX,
)
from ai_trading_system.domains.ranking.eligibility import apply_rank_eligibility
from ai_trading_system.domains.ranking.factors import (
    add_signal_freshness,
    apply_delivery,
    apply_momentum_acceleration,
    apply_proximity_highs,
    apply_relative_strength,
    apply_sector_strength,
    apply_trend_persistence,
    apply_volume_intensity,
    compute_penalty_score,
)
from ai_trading_system.domains.ranking.input_loader import RankerInputLoader
from ai_trading_system.domains.ranking.stage_store import read_latest_snapshot


# Weight given to NIFTY-relative RS in the blended ``rel_strength`` factor.
# 0.0 = pure absolute multi-period RS (legacy); 1.0 = pure benchmark-relative.
# 0.4 mixes the two: existing percentile-ranked RS still dominates, but
# stocks lagging NIFTY are penalised in proportion. No-op when benchmark
# history is unavailable in the loader / DuckDB store.
NIFTY_RS_BLEND: float = 0.4


class StockRanker:
    """Facade that preserves the legacy ranking API over modular rank services."""

    WEIGHTS = load_factor_weights()

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        data_domain: str = "operational",
    ):
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
        )
        paths = ensure_domain_layout(
            project_root=project_root,
            data_domain=data_domain,
        )
        if ohlcv_db_path is None:
            ohlcv_db_path = str(paths.ohlcv_db_path)
        if feature_store_dir is None:
            feature_store_dir = str(paths.feature_store_dir)
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.data_domain = data_domain
        self.master_db_path = str(paths.master_db_path)
        self.input_loader = RankerInputLoader(
            ohlcv_db_path=self.ohlcv_db_path,
            feature_store_dir=self.feature_store_dir,
            master_db_path=self.master_db_path,
        )
        os.makedirs(self.feature_store_dir, exist_ok=True)

    def _get_conn(self):
        return self.input_loader.get_conn()

    def _normalize_symbol_exchange_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.input_loader.normalize_symbol_exchange_columns(data)

    def _load_sector_inputs(self) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
        return self.input_loader.load_sector_inputs()

    def rank_all(
        self,
        date: str = None,
        exchanges: List[str] = None,
        min_score: float = 50.0,
        top_n: int = None,
        benchmark_symbol: str = "NIFTY50",
        weights: Dict[str, float] = None,
        rank_mode: str = "default",
        previous_ranked: pd.DataFrame | None = None,
        apply_penalty_adjustment: bool = False,
        weekly_stage_gate: bool = False,
    ) -> pd.DataFrame:
        """
        Rank all symbols for a given date while preserving the current artifact contract.
        """
        weights = dict(weights or self.WEIGHTS)
        exchanges = exchanges or ["NSE"]

        if date is None:
            date = self.input_loader.latest_available_date(exchange="NSE")
        if date is None:
            logger.warning("No data available for ranking")
            return pd.DataFrame()

        logger.info("Ranking stocks for date=%s, exchanges=%s", date, exchanges)
        if rank_mode not in RANK_MODES:
            logger.warning("Unknown rank_mode=%s; falling back to default", rank_mode)
            rank_mode = "default"

        scores = self.input_loader.load_latest_market_data(exchanges=exchanges)
        if scores.empty:
            logger.warning("No data available for ranking")
            return pd.DataFrame()

        scores = self._compute_relative_strength(scores, date, benchmark_symbol)
        scores = apply_momentum_acceleration(scores)
        scores = self._compute_volume_intensity(scores)
        scores = self._compute_trend_persistence(scores, date)
        scores = self._compute_proximity_highs(scores, date)
        scores = self._compute_delivery(scores, date)
        scores = self._compute_sector_strength(scores, date)
        scores = compute_factor_scores(scores, weights=weights)
        scores = self._compute_stage2(scores, date, exchanges)
        scores = self._attach_weekly_stage_context(scores, date)
        if weekly_stage_gate:
            scores = self._apply_weekly_stage_gate(scores, date)
        scores.loc[:, "rank_mode"] = rank_mode

        # ── Stage 2 enrichment (additive, non-breaking) ──────────────────
        # stage2_score remains a soft ranking bonus. When operating in
        # stage2_breakout mode, hard gating comes from structural Stage 2.
        if "stage2_score" in scores.columns:
            s2_score = pd.to_numeric(scores["stage2_score"], errors="coerce").fillna(0.0)
            scores.loc[:, "stage2_score_bonus"] = (s2_score / 100.0) * 5.0
        else:
            scores.loc[:, "stage2_score_bonus"] = 0.0
        scores = self._apply_stage2_age_bonuses(scores)

        stage2_gate_column = None
        if "is_stage2_structural" in scores.columns:
            stage2_gate_column = "is_stage2_structural"
        elif "is_stage2_uptrend" in scores.columns:
            stage2_gate_column = "is_stage2_uptrend"

        if rank_mode == "stage2_breakout" and stage2_gate_column is not None:
            pre_filter_count = len(scores)
            scores = scores[scores[stage2_gate_column].fillna(False)].copy()
            logger.info(
                "stage2_breakout mode: %d → %d symbols after Stage 2 filter",
                pre_filter_count,
                len(scores),
            )

        scores = apply_rank_eligibility(
            scores,
            stage2_gate_enabled=(rank_mode == "stage2_breakout"),
            weekly_stage_gate_enabled=weekly_stage_gate,
        )
        scores = compute_penalty_score(scores)
        scores.loc[:, "composite_score_adjusted"] = (
            scores["composite_score"]
            + scores["stage2_score_bonus"]
            + scores["stage2_freshness_bonus"]
            + scores["stage2_transition_bonus"]
            - scores["penalty_score"].fillna(0.0)
        ).clip(0.0, 100.0)
        scores = compute_rank_confidence(scores)
        scores = add_signal_freshness(scores)
        scores = apply_rank_stability(scores, previous_frame=previous_ranked)
        if apply_penalty_adjustment:
            scores.loc[:, "composite_score"] = scores["composite_score_adjusted"]
        # scores = self._apply_1yr_penalty(scores, weights)
        scores = filter_ranked_scores(scores, min_score=min_score, top_n=top_n)
        return select_rank_output_columns(scores)

    def _apply_1yr_penalty(
        self,
        scores: pd.DataFrame,
        weights: Dict[str, float] = None,
    ) -> pd.DataFrame:
        """
        Apply a penalty to stocks down >30% over 1 year.
        Preserved as a facade seam for future re-enablement.
        """
        penalty_weights = dict(weights or self.WEIGHTS)
        penalty = penalty_weights["proximity_highs"] * 30
        try:
            conn = self._get_conn()
            try:
                yr_return = conn.execute(
                    """
                    SELECT
                        symbol_id, exchange, close,
                        LAG(close, 252) OVER (
                            PARTITION BY symbol_id ORDER BY timestamp
                        ) AS close_1yr_ago
                    FROM _catalog
                    WHERE exchange = 'NSE'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY symbol_id ORDER BY timestamp DESC
                    ) = 1
                    """
                ).fetchdf()
            finally:
                conn.close()

            if "close_1yr_ago" not in yr_return.columns:
                return scores

            yr_return["ret_1yr"] = (
                (yr_return["close"] - yr_return["close_1yr_ago"])
                / yr_return["close_1yr_ago"].replace(0, float("nan"))
            ).fillna(0) * 100

            scores = scores.merge(
                yr_return[["symbol_id", "exchange", "ret_1yr"]],
                on=["symbol_id", "exchange"],
                how="left",
            )
            penalty_mask = scores["ret_1yr"].fillna(0) < -30
            scores.loc[penalty_mask, "composite_score"] -= penalty
            scores.loc[penalty_mask, "prox_high_score"] = (
                scores.loc[penalty_mask, "prox_high_score"] * 0.5
            )
            scores.drop(columns=["ret_1yr"], inplace=True, errors="ignore")
            logger.info(
                "1-year penalty applied to %s stocks (down >30%% over 1 year)",
                int(penalty_mask.sum()),
            )
        except Exception as exc:
            logger.warning("Could not apply 1-year penalty: %s", exc)

        return scores

    def _compute_relative_strength(
        self,
        data: pd.DataFrame,
        date: str,
        benchmark_symbol: str,
        periods: List[int] = None,
    ) -> pd.DataFrame:
        period_list = periods or [5, 10, 20, 60, 120]
        try:
            return_frame = self.input_loader.load_return_frame_multi(periods=period_list)
        except Exception as exc:
            logger.warning("Could not compute relative strength: %s", exc)
            cols = ["symbol_id", "exchange"] + [f"return_{p}" for p in period_list]
            return_frame = pd.DataFrame(columns=cols)
        scored = apply_relative_strength(data, return_frame=return_frame)

        # Stock-vs-NIFTY relative strength: subtract benchmark per-period
        # returns from each symbol's, percentile-rank the blend, mix into
        # ``rel_strength`` at ``NIFTY_RS_BLEND``. Defensive: no-op when the
        # benchmark history isn't available (test fixtures, missing DB).
        try:
            scored = self._blend_nifty_relative_rs(
                scored,
                date=date,
                benchmark_symbol=benchmark_symbol,
                periods=period_list,
            )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("NIFTY-relative RS blend skipped: %s", exc)
        return scored

    def _blend_nifty_relative_rs(
        self,
        data: pd.DataFrame,
        *,
        date: str,
        benchmark_symbol: str,
        periods: List[int],
    ) -> pd.DataFrame:
        if data.empty:
            return data
        benchmark_returns = self._load_benchmark_returns(
            loader=self.input_loader,
            date=date,
            benchmark_symbol=benchmark_symbol,
            periods=periods,
        )
        if not benchmark_returns:
            return data

        scored = data.copy()
        deltas: list[pd.Series] = []
        for period in periods:
            col = f"return_{period}"
            if col not in scored.columns:
                continue
            symbol_ret = pd.to_numeric(scored[col], errors="coerce").fillna(0.0)
            bench_ret = float(benchmark_returns.get(period, 0.0))
            scored.loc[:, f"rs_vs_nifty_{period}"] = symbol_ret - bench_ret
            deltas.append(scored[f"rs_vs_nifty_{period}"])

        if deltas:
            blended = pd.concat(deltas, axis=1).mean(axis=1)
            nifty_rs_score = blended.rank(pct=True) * 100.0
            scored.loc[:, "rs_vs_nifty_score"] = nifty_rs_score
            if "rel_strength" in scored.columns:
                scored.loc[:, "rel_strength"] = (
                    pd.to_numeric(scored["rel_strength"], errors="coerce").fillna(0.0)
                    * (1.0 - NIFTY_RS_BLEND)
                    + nifty_rs_score * NIFTY_RS_BLEND
                )
        return scored

    def _load_benchmark_returns(
        self,
        *,
        loader: "RankerInputLoader",
        date: str,
        benchmark_symbol: str,
        periods: List[int],
    ) -> Dict[int, float]:
        """Best-effort fetch of NIFTY's multi-period returns.

        Tries (in order):
          1. ``loader.load_benchmark_returns(symbol, date, periods)`` if defined.
          2. Direct DuckDB query against ``self.ohlcv_db_path``.
          3. Empty dict (no-op blend).
        """
        method = getattr(loader, "load_benchmark_returns", None)
        if callable(method):
            try:
                returns = method(symbol=benchmark_symbol, date=date, periods=periods)
                if isinstance(returns, dict):
                    return {int(k): float(v) for k, v in returns.items()}
            except Exception as exc:  # pragma: no cover
                logger.debug("benchmark loader method failed: %s", exc)

        try:
            import duckdb
        except ImportError:  # pragma: no cover
            return {}
        if not self.ohlcv_db_path or not os.path.exists(self.ohlcv_db_path):
            return {}

        try:
            conn = duckdb.connect(self.ohlcv_db_path, read_only=True)
        except Exception:  # pragma: no cover
            return {}
        try:
            history = conn.execute(
                """
                SELECT timestamp, close FROM ohlcv
                WHERE symbol_id = ? AND timestamp <= CAST(? AS TIMESTAMP)
                ORDER BY timestamp DESC LIMIT 250
                """,
                [benchmark_symbol, date or "9999-12-31"],
            ).fetchdf()
        except Exception:
            return {}
        finally:
            conn.close()

        if history.empty or "close" not in history.columns:
            return {}
        history = history.sort_values("timestamp").reset_index(drop=True)
        latest_close = float(history["close"].iloc[-1])
        if latest_close <= 0:
            return {}

        out: Dict[int, float] = {}
        for period in periods:
            if len(history) <= period:
                continue
            past_close = float(history["close"].iloc[-period - 1])
            if past_close <= 0:
                continue
            out[int(period)] = (latest_close - past_close) / past_close * 100.0
        return out

    def _compute_volume_intensity(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            volume_frame = self.input_loader.load_volume_frame()
        except Exception as exc:
            logger.warning("Could not compute volume intensity: %s", exc)
            volume_frame = pd.DataFrame(
                columns=["symbol_id", "exchange", "vol_20_avg", "vol_20_max", "volume_zscore_20"]
            )
        return apply_volume_intensity(data, volume_frame=volume_frame)

    def _compute_trend_persistence(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        try:
            adx_frame = self.input_loader.load_latest_adx(date=date)
        except Exception as exc:
            logger.warning("ADX load failed: %s", exc)
            adx_frame = pd.DataFrame(columns=["symbol_id", "exchange", "adx_14"])

        try:
            sma_frame = self.input_loader.load_latest_sma(date=date)
        except Exception as exc:
            logger.warning("Could not compute SMA: %s", exc)
            sma_frame = pd.DataFrame(columns=["symbol_id", "exchange", "sma_20", "sma_50"])

        return apply_trend_persistence(data, adx_frame=adx_frame, sma_frame=sma_frame)

    def _compute_proximity_highs(
        self,
        data: pd.DataFrame,
        date: str,
        window: int = 252,
    ) -> pd.DataFrame:
        try:
            highs_frame = self.input_loader.load_latest_highs(date=date, window=window)
        except Exception as exc:
            logger.warning("Could not compute proximity highs: %s", exc)
            highs_frame = pd.DataFrame(columns=["symbol_id", "exchange", "high_52w"])
        return apply_proximity_highs(data, highs_frame=highs_frame)

    def _compute_delivery(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        try:
            delivery_frame = self.input_loader.load_latest_delivery(date=date)
        except Exception as exc:
            logger.warning("Could not compute delivery factor: %s", exc)
            delivery_frame = pd.DataFrame(columns=["symbol_id", "exchange", "delivery_pct"])
        return apply_delivery(data, delivery_frame=delivery_frame)

    def _compute_sector_strength(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        sector_rs, stock_vs_sector, sector_map = self._load_sector_inputs()
        return apply_sector_strength(
            data,
            sector_rs=sector_rs,
            stock_vs_sector=stock_vs_sector,
            sector_map=sector_map,
            date=date,
        )

    def _attach_weekly_stage_context(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """Join latest weekly stage snapshot onto rank candidates."""
        if data.empty:
            return data
        try:
            snap = read_latest_snapshot(self.ohlcv_db_path, asof=date)
        except Exception as exc:
            logger.warning("Could not load weekly stage snapshot: %s", exc)
            return data

        if snap.empty:
            return data

        snap_cols = [
            column
            for column in [
                "symbol",
                "stage_label",
                "stage_confidence",
                "stage_transition",
                "bars_in_stage",
                "stage_entry_date",
            ]
            if column in snap.columns
        ]
        snap = snap[snap_cols].rename(
            columns={
                "symbol": "symbol_id",
                "stage_label": "weekly_stage_label",
                "stage_confidence": "weekly_stage_confidence",
                "stage_transition": "weekly_stage_transition",
            }
        )
        output = data.drop(
            columns=[
                "weekly_stage_label",
                "weekly_stage_confidence",
                "weekly_stage_transition",
                "bars_in_stage",
                "stage_entry_date",
            ],
            errors="ignore",
        )
        return output.merge(snap, on="symbol_id", how="left")

    def _apply_weekly_stage_gate(
        self,
        data: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """Join latest weekly stage snapshot and leave gate columns for eligibility."""
        merged = data if "weekly_stage_label" in data.columns else self._attach_weekly_stage_context(data, date)
        if "weekly_stage_label" not in merged.columns:
            logger.info("weekly_stage_gate: no snapshot rows for asof=%s — gate skipped", date)
            return merged

        s2_count = (merged["weekly_stage_label"] == "S2").sum()
        no_snap = merged["weekly_stage_label"].isna().sum()
        logger.info(
            "weekly_stage_gate: %d S2, %d no-snapshot (pass-through), %d other",
            s2_count, no_snap, len(merged) - s2_count - no_snap,
        )
        return merged

    def _apply_stage2_age_bonuses(self, data: pd.DataFrame) -> pd.DataFrame:
        output = data.copy()
        output.loc[:, "stage2_freshness_bonus"] = 0.0
        output.loc[:, "stage2_transition_bonus"] = 0.0
        output.loc[:, "stage2_age_warning"] = ""
        if output.empty or "weekly_stage_label" not in output.columns:
            return output

        bars = pd.to_numeric(output.get("bars_in_stage", pd.Series(pd.NA, index=output.index)), errors="coerce")
        weekly_s2 = output["weekly_stage_label"].astype(str).eq("S2")
        fresh = weekly_s2 & bars.notna() & (bars <= STAGE2_FRESH_BARS_MAX)
        mid = weekly_s2 & bars.notna() & (bars > STAGE2_FRESH_BARS_MAX) & (bars <= STAGE2_MID_BARS_MAX)
        mature = weekly_s2 & bars.notna() & (bars >= STAGE2_MID_BARS_MAX + 1)
        output.loc[fresh, "stage2_freshness_bonus"] = STAGE2_FRESHNESS_BONUS
        output.loc[mid, "stage2_freshness_bonus"] = STAGE2_MID_FRESHNESS_BONUS
        output.loc[mature, "stage2_age_warning"] = "mature_stage2"

        transition = output.get("weekly_stage_transition", pd.Series("", index=output.index)).astype(str)
        recent_transition = transition.eq("S1_TO_S2") & bars.notna() & (bars <= STAGE2_TRANSITION_BONUS_BARS_MAX)
        output.loc[recent_transition, "stage2_transition_bonus"] = STAGE2_TRANSITION_BONUS
        return output

    def _compute_stage2(
        self,
        data: pd.DataFrame,
        date: str,
        exchanges: List[str],
    ) -> pd.DataFrame:
        try:
            stage2_frame = self.input_loader.load_latest_stage2(
                date=date,
                exchanges=exchanges,
                rel_strength_frame=data,
            )
        except Exception as exc:
            logger.warning("Could not compute Stage 2 enrichment: %s", exc)
            return data

        if stage2_frame.empty:
            return data

        merged = data.merge(
            stage2_frame,
            on=["symbol_id", "exchange"],
            how="left",
            suffixes=("", "_stage2"),
        )

        for column in [
            "timestamp",
            "close",
            "sma_200",
            "sma_150",
            "sma200_slope_20d_pct",
            "stage2_score",
            "is_stage2_structural",
            "is_stage2_candidate",
            "is_stage2_uptrend",
            "stage2_label",
            "stage2_hard_fail_reason",
            "stage2_fail_reason",
        ]:
            stage2_column = f"{column}_stage2"
            if stage2_column not in merged.columns:
                continue
            if column in merged.columns:
                merged.loc[:, column] = merged[column].where(merged[column].notna(), merged[stage2_column])
            else:
                merged.loc[:, column] = merged[stage2_column]

        drop_cols = [col for col in merged.columns if col.endswith("_stage2")]
        if drop_cols:
            merged = merged.drop(columns=drop_cols, errors="ignore")
        return merged

    def rank_with_fundamentals(
        self,
        date: str = None,
        exchanges: List[str] = None,
        industry_filter: str = None,
        mcap_filter: str = None,
        min_score: float = 60.0,
        top_n: int = 50,
    ) -> pd.DataFrame:
        """Rank with fundamental filters applied after technical scoring."""
        scores = self.rank_all(date, exchanges, min_score=0, top_n=None)

        fund_path = os.path.join(self.feature_store_dir, "fundamental", "NSE")
        if os.path.exists(fund_path):
            import glob

            fund_files = glob.glob(os.path.join(fund_path, "*.parquet"))
            if fund_files:
                fund_df = pd.concat(pd.read_parquet(file_path) for file_path in fund_files)
                scores = scores.merge(
                    fund_df[["symbol_id", "industry_group", "industry", "mcap_category"]],
                    on="symbol_id",
                    how="left",
                )

                if industry_filter:
                    scores = scores[scores["industry"].str.contains(industry_filter, na=False)]
                if mcap_filter:
                    scores = scores[scores["mcap_category"] == mcap_filter]

        scores = scores.sort_values("composite_score", ascending=False)
        if top_n:
            scores = scores.head(top_n)
        return scores.reset_index(drop=True)
