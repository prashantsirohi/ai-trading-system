import os
import time
import logging
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AIQScreener:
    """
    AI Trading System Screener — End-to-End Pipeline.

    Combines all layers:
      1. Regime Detection: Is market trending or ranging?
      2. Multi-Factor Ranking: Score all stocks on 4 factors
      3. ML Alpha Engine: XGBoost signals on ranked stocks
      4. Risk Manager: ATR-based position sizing
      5. Backtester: Event-driven backtest on signals
      6. Visualizer: Reports and charts

    Daily workflow:
      a) Detect market regime
      b) Rank all stocks (multi-factor)
      c) Apply ML alpha filter
      d) Risk-size positions
      e) Run backtest simulation
      f) Generate report
    """

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        model_dir: str = None,
        output_dir: str = None,
    ):
        from analytics.regime_detector import RegimeDetector
        from analytics.ranker import StockRanker
        from analytics.ml_engine import AlphaEngine
        from analytics.risk_manager import RiskManager
        from analytics.backtester import EventBacktester
        from analytics.visualizations import Visualizer

        if ohlcv_db_path is None:
            ohlcv_db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "ohlcv.duckdb",
            )
        if feature_store_dir is None:
            feature_store_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "feature_store",
            )
        if model_dir is None:
            model_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "models",
            )
        if output_dir is None:
            output_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "reports",
            )

        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.model_dir = model_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        self.regime_detector = RegimeDetector(
            ohlcv_db_path=ohlcv_db_path,
            feature_store_dir=feature_store_dir,
        )
        self.ranker = StockRanker(
            ohlcv_db_path=ohlcv_db_path,
            feature_store_dir=feature_store_dir,
        )
        self.ml_engine = AlphaEngine(
            ohlcv_db_path=ohlcv_db_path,
            feature_store_dir=feature_store_dir,
            model_dir=model_dir,
        )
        self.risk_manager = RiskManager(
            ohlcv_db_path=ohlcv_db_path,
            feature_store_dir=feature_store_dir,
        )
        self.backtester = EventBacktester(
            ohlcv_db_path=ohlcv_db_path,
            feature_store_dir=feature_store_dir,
        )
        self.visualizer = Visualizer(
            ohlcv_db_path=ohlcv_db_path,
            feature_store_dir=feature_store_dir,
            output_dir=output_dir,
        )

        self._current_regime = "TREND"
        self._current_signals = pd.DataFrame()
        self._portfolio = pd.DataFrame()

    def run_daily_screening(
        self,
        date: str = None,
        top_n: int = 50,
        min_score: float = 55.0,
        exchanges: List[str] = None,
        run_ml: bool = True,
        run_backtest: bool = True,
        generate_report: bool = True,
        capital: float = 10_000_000,
    ) -> Dict:
        """
        Run the complete daily screening pipeline.

        Returns:
            Dict with: regime, ranked_signals, ml_signals, portfolio,
                      backtest_results, report_paths
        """
        t0 = time.time()
        if exchanges is None:
            exchanges = ["NSE"]

        if date is None:
            conn = duckdb.connect(self.ohlcv_db_path)
            try:
                latest = conn.execute(
                    "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
                ).fetchone()[0]
                date = (
                    str(latest.date()) if hasattr(latest, "date") else str(latest)[:10]
                )
            finally:
                conn.close()

        logger.info("=" * 60)
        logger.info(f"AIQ Screener — Daily Run — Date: {date}")
        logger.info("=" * 60)

        regime_info = self.regime_detector.get_market_regime(date=date)
        self._current_regime = regime_info["market_regime"]
        logger.info(f"[1/6] Market Regime: {self._current_regime}")

        ranked = self.ranker.rank_all(
            date=date,
            exchanges=exchanges,
            min_score=min_score,
            top_n=top_n * 2,
        )
        logger.info(f"[2/6] Ranked {len(ranked)} candidates (score >= {min_score})")

        if ranked.empty:
            logger.warning("No candidates above minimum score threshold")
            return {
                "regime": regime_info,
                "ranked": ranked,
                "portfolio": pd.DataFrame(),
            }

        top_symbols = ranked.head(top_n)["symbol_id"].tolist()

        ml_signals = pd.DataFrame()
        if run_ml:
            ml_signals = self.ml_engine.predict(
                symbols=top_symbols,
                horizon=5,
            )
            logger.info(
                f"[3/6] ML predictions: {len(ml_signals)} symbols, "
                f"{ml_signals[ml_signals['prediction'] == 1]['symbol_id'].tolist()[:10] if not ml_signals.empty else 'none'}"
            )
        else:
            ml_signals = ranked.head(top_n).copy()
            ml_signals["probability"] = ml_signals["composite_score"] / 100
            ml_signals["prediction"] = 1
            ml_signals["direction"] = "LONG"

        regime_mult = 1.0 if self._current_regime == "STRONG_TREND" else 0.7
        portfolio = self.risk_manager.build_portfolio(
            signals=ml_signals,
            capital=capital,
            regime=self._current_regime,
            regime_multiplier=regime_mult,
        )
        self._current_portfolio = portfolio
        logger.info(
            f"[4/6] Portfolio: {len(portfolio)} positions, "
            f"total_exposure={portfolio['weight'].sum() * 100:.1f}%"
            if not portfolio.empty
            else "[4/6] No positions sized"
        )

        bt_results = {}
        if run_backtest and not ml_signals.empty:
            top_syms = ml_signals.head(10)["symbol_id"].tolist()
            event_type = (
                "TREND_FOLLOW"
                if self._current_regime in ("STRONG_TREND", "MIXED")
                else "MEAN_REV"
            )
            bt = self.backtester.run_event_backtest(
                symbols=top_syms,
                event_type=event_type,
                from_date=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                to_date=date,
            )
            bt_results = bt
            logger.info(
                f"[5/6] Backtest ({event_type}): {bt.get('n_trades', 0)} trades, "
                f"Sharpe={bt.get('metrics', {}).get('sharpe', 0):.2f}"
            )

        report_paths = {}
        if generate_report:
            equity = bt_results.get("equity_curve", pd.DataFrame())
            report_paths = self.visualizer.generate_daily_report(
                ranked_signals=ranked.head(top_n),
                equity_curve=equity,
                regime=self._current_regime,
            )
            logger.info(f"[6/6] Report generated: {report_paths.get('summary', 'N/A')}")

        elapsed = time.time() - t0
        logger.info("=" * 60)
        logger.info(
            f"Daily screening complete in {elapsed:.1f}s — "
            f"Regime={self._current_regime}, "
            f"Candidates={len(ranked)}, "
            f"Positions={len(portfolio)}"
        )
        logger.info("=" * 60)

        return {
            "date": date,
            "regime": regime_info,
            "ranked_signals": ranked.head(top_n),
            "ml_signals": ml_signals,
            "portfolio": portfolio,
            "backtest_results": bt_results,
            "report_paths": report_paths,
            "elapsed_sec": round(elapsed, 1),
        }

    def train_models(
        self,
        symbols: List[str] = None,
        train_days: int = 504,
        test_days: int = 63,
        horizon: int = 5,
    ) -> Dict:
        """
        Run walk-forward validation and train final models.
        """
        logger.info("Starting model training pipeline...")
        result = self.ml_engine.walk_forward_validate(
            symbols=symbols,
            train_days=train_days,
            test_days=test_days,
            horizon=horizon,
        )
        return result

    def compare_regimes(self) -> pd.DataFrame:
        """
        Compare backtest performance across all event types.
        """
        result = self.backtester.compare_events()
        return result

    def get_top_signals(
        self,
        date: str = None,
        min_score: float = 60.0,
        n: int = 20,
    ) -> pd.DataFrame:
        """
        Quick top-N signals without full pipeline.
        """
        return self.ranker.rank_all(date=date, min_score=min_score, top_n=n)

    def regime_distribution(
        self,
        symbols: List[str] = None,
        date: str = None,
    ) -> pd.DataFrame:
        """
        Get regime breakdown across all symbols.
        """
        if symbols is None:
            conn = duckdb.connect(self.ohlcv_db_path)
            try:
                syms = conn.execute("""
                    SELECT DISTINCT symbol_id FROM _catalog
                    WHERE exchange = 'NSE' ORDER BY symbol_id
                """).fetchdf()
                symbols = syms["symbol_id"].tolist()
            finally:
                conn.close()

        regimes = self.regime_detector.detect_bulk_regimes(
            symbols=symbols,
            date=date,
        )
        return regimes

    def summary(self) -> Dict:
        """
        Return current system status summary.
        """
        conn = duckdb.connect(self.ohlcv_db_path)
        try:
            total_rows = conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0]
            total_syms = conn.execute(
                "SELECT COUNT(DISTINCT symbol_id) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            latest = conn.execute(
                "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
        finally:
            conn.close()

        feat_path = self.feature_store_dir
        import glob

        n_feature_files = len(
            glob.glob(os.path.join(feat_path, "*", "NSE", "*.parquet"))
        )

        return {
            "ohlcv_rows": total_rows,
            "symbols": total_syms,
            "latest_date": str(latest)[:10] if latest else None,
            "feature_files": n_feature_files,
            "current_regime": self._current_regime,
            "portfolio_positions": len(self._current_portfolio),
            "output_dir": self.output_dir,
        }
