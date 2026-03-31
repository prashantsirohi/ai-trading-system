import os
import sys
import logging
from datetime import datetime

from core.bootstrap import ensure_project_root_on_path
ensure_project_root_on_path(__file__)

from collectors import DhanCollector, NSECollector
from features import FeatureEngine
from signals import SignalDetector
from backtesting import BacktestEngine
from ai import SignalRanker
from risk import RiskManager
from execution import DhanExecutor
from config import CONFIG
from utils.env import load_project_env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_project_env(__file__)


class AITradingSystem:
    """
    Main AI Trading System orchestrator.
    
    Flow:
    1. Collect market data
    2. Calculate features/indicators
    3. Generate trading signals
    4. Run backtests
    5. Rank signals with AI
    6. Apply risk management
    7. Execute trades
    """

    def __init__(self, config=None):
        self.config = config or CONFIG
        self.dhan_collector = None
        self.nse_collector = None
        self.feature_engine = None
        self.signal_detector = None
        self.backtest_engine = None
        self.ai_ranker = None
        self.risk_manager = None
        self.executor = None

    def initialize(self):
        """Initialize all components"""
        logger.info("Initializing AI Trading System...")

        self.dhan_collector = DhanCollector(
            api_key=self.config.dhan.api_key,
            client_id=self.config.dhan.client_id,
            data_dir=self.config.data.raw_dir
        )

        self.nse_collector = NSECollector(
            data_dir=self.config.data.raw_dir
        )

        self.feature_engine = FeatureEngine(
            data_dir=self.config.data.data_dir
        )

        self.signal_detector = SignalDetector(
            data_dir=self.config.data.data_dir
        )

        self.backtest_engine = BacktestEngine(
            data_dir=self.config.data.data_dir
        )

        self.ai_ranker = SignalRanker(
            data_dir=self.config.data.data_dir,
            model_path=self.config.ai.model_path
        )

        self.risk_manager = RiskManager(
            max_risk_per_trade=self.config.risk.max_risk_per_trade,
            max_portfolio_exposure=self.config.risk.max_portfolio_exposure,
            max_drawdown=self.config.risk.max_drawdown,
            max_positions=self.config.risk.max_positions
        )

        if self.config.dhan.access_token:
            self.executor = DhanExecutor(
                api_key=self.config.dhan.api_key,
                client_id=self.config.dhan.client_id,
                access_token=self.config.dhan.access_token,
                risk_manager=self.risk_manager
            )

        logger.info("AI Trading System initialized successfully")

    def collect_data(self, symbols: list):
        """Collect market data for symbols"""
        logger.info(f"Collecting data for {len(symbols)} symbols...")

        for symbol in symbols:
            df = self.nse_collector.get_ohlc_data(symbol)
            if not df.empty:
                self.nse_collector.save_to_parquet(df, symbol)

        logger.info("Data collection complete")

    def generate_signals(self, symbols: list):
        """Generate trading signals"""
        logger.info(f"Generating signals for {len(symbols)} symbols...")

        all_signals = []
        for symbol in symbols:
            features = self.feature_engine.get_latest_features(symbol)
            if features:
                self.feature_engine.save_features(
                    self.feature_engine.load_raw_data(symbol), symbol
                )

            signals = self.signal_detector.generate_signals(symbol)
            if not signals.empty:
                all_signals.append(signals)

        if all_signals:
            combined = pd.concat(all_signals, ignore_index=True)
            return combined.sort_values("signal_strength", ascending=False)

        return pd.DataFrame()

    def rank_signals(self, signals_df: pd.DataFrame):
        """Rank signals using AI"""
        logger.info("Ranking signals with AI...")

        if not self.ai_ranker.is_trained:
            logger.warning("AI model not trained, using signal strength")
            return signals_df

        return self.ai_ranker.get_top_signals(
            signals_df,
            top_n=self.config.ai.top_n_signals,
            min_score=self.config.ai.min_prediction_score
        )

    def execute_trades(self, signals_df: pd.DataFrame):
        """Execute trades for top signals"""
        if not self.executor:
            logger.warning("Executor not initialized")
            return

        logger.info(f"Executing trades for {len(signals_df)} signals...")

        for _, signal in signals_df.iterrows():
            result = self.executor.execute_signal(
                signal=signal,
                price=signal.get("close", 0),
                risk_manager=self.risk_manager
            )
            logger.info(f"Trade result: {result}")

    def run_daily_pipeline(self, symbols: list):
        """Run complete daily trading pipeline"""
        logger.info("Starting daily trading pipeline...")

        self.collect_data(symbols)

        signals = self.generate_signals(symbols)
        if signals.empty:
            logger.info("No signals generated")
            return

        ranked_signals = self.rank_signals(signals)

        self.execute_trades(ranked_signals)

        logger.info("Daily trading pipeline complete")

    def train_ai(self, symbols: list):
        """Train AI ranking model"""
        logger.info("Training AI model...")

        result = self.ai_ranker.train(
            symbols=symbols,
            model_type=self.config.ai.model_type
        )

        logger.info(f"AI training complete: {result}")
        return result


def main():
    """Main entry point"""
    config = CONFIG.from_env()

    system = AITradingSystem(config)
    system.initialize()

    symbols = ["RELIANCE", "INFY", "TCS", "HDFCBANK", "SBIN", "HINDUNILVR"]

    system.train_ai(symbols)

    system.run_daily_pipeline(symbols)


if __name__ == "__main__":
    main()
