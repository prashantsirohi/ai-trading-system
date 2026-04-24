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
from ai_trading_system.domains.ranking.contracts import RANK_MODES
from ai_trading_system.domains.ranking.eligibility import apply_rank_eligibility
from ai_trading_system.domains.ranking.factors import (
    add_signal_freshness,
    apply_delivery,
    apply_proximity_highs,
    apply_relative_strength,
    apply_sector_strength,
    apply_trend_persistence,
    apply_volume_intensity,
    compute_penalty_score,
)
from ai_trading_system.domains.ranking.input_loader import RankerInputLoader


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
        scores = self._compute_volume_intensity(scores)
        scores = self._compute_trend_persistence(scores, date)
        scores = self._compute_proximity_highs(scores, date)
        scores = self._compute_delivery(scores, date)
        scores = self._compute_sector_strength(scores, date)
        scores = compute_factor_scores(scores, weights=weights)
        scores = self._compute_stage2(scores, date, exchanges)
        scores.loc[:, "rank_mode"] = rank_mode

        # ── Stage 2 enrichment (additive, non-breaking) ──────────────────
        # stage2_score remains a soft ranking bonus. When operating in
        # stage2_breakout mode, hard gating comes from structural Stage 2.
        if "stage2_score" in scores.columns:
            s2_score = pd.to_numeric(scores["stage2_score"], errors="coerce").fillna(0.0)
            scores.loc[:, "stage2_score_bonus"] = (s2_score / 100.0) * 5.0
        else:
            scores.loc[:, "stage2_score_bonus"] = 0.0

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
        )
        scores = compute_penalty_score(scores)
        scores.loc[:, "composite_score_adjusted"] = (
            scores["composite_score"]
            + scores["stage2_score_bonus"]
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
        _ = (date, benchmark_symbol)
        period_list = periods or [20, 60, 120]
        try:
            return_frame = self.input_loader.load_return_frame_multi(periods=period_list)
        except Exception as exc:
            logger.warning("Could not compute relative strength: %s", exc)
            cols = ["symbol_id", "exchange"] + [f"return_{p}" for p in period_list]
            return_frame = pd.DataFrame(columns=cols)
        return apply_relative_strength(data, return_frame=return_frame)

    def _compute_volume_intensity(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            volume_frame = self.input_loader.load_volume_frame()
        except Exception as exc:
            logger.warning("Could not compute volume intensity: %s", exc)
            volume_frame = pd.DataFrame(columns=["symbol_id", "exchange", "vol_20_avg", "vol_20_max"])
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
